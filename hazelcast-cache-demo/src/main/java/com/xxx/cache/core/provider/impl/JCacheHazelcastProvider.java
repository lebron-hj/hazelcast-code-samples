package com.xxx.cache.core.provider.impl;

import com.xxx.cache.core.provider.CacheProvider;
import com.xxx.cache.entity.Product;

import javax.cache.Cache;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

/**
 * 基于 Hazelcast JCache（JSR-107）的 {@link CacheProvider} 实现，是整个缓存强一致性方案的核心适配层。
 *
 * <h3>架构定位</h3>
 * <pre>
 *   StrictWriteThroughStrategy / ReadThroughStrategy
 *              ↓ 通过 CacheProvider 接口调用
 *   JCacheHazelcastProvider（本类）
 *              ↓ 委托给
 *   javax.cache.Cache（Hazelcast JCache 实现）
 *              ↓ 框架自动调用
 *   ProductCacheWriter   ← 写操作前先写 DB
 *   ProductCacheLoader   ← 读未命中时从 DB 加载
 * </pre>
 *
 * <h3>强一致性机制</h3>
 * <ul>
 *   <li><b>写操作（put/delete）</b>：JCache 框架保证先调用 {@code ProductCacheWriter}
 *       持久化到 DB，成功后才更新缓存；DB 失败则缓存保持不变，无需手动回滚。</li>
 *   <li><b>读操作（get）</b>：JCache 框架在缓存未命中时自动调用 {@code ProductCacheLoader}
 *       从 DB 加载并回填缓存，调用者无感知。</li>
 * </ul>
 *
 * <h3>与 HazelcastCacheProvider 的区别</h3>
 * {@code HazelcastCacheProvider} 直接操作 {@code IMap}，不走 JCache 回调，
 * 需要策略层手动维护 DB 与缓存的写顺序；本类通过 JCache 框架托管写顺序，
 * 策略层代码大幅简化，一致性保证更可靠。
 *
 * @see com.xxx.cache.core.jcache.ProductCacheWriter
 * @see com.xxx.cache.core.jcache.ProductCacheLoader
 */
public class JCacheHazelcastProvider implements CacheProvider<String, Product> {

    /**
     * 底层 JCache 实例，由 {@link com.xxx.cache.config.JCacheConfig} 创建并注入，
     * 已配置 read-through + write-through。
     */
    private final Cache<String, Product> cache;

    /**
     * @param cache 已配置 read-through + write-through 的 JCache 实例。
     */
    public JCacheHazelcastProvider(Cache<String, Product> cache) {
        this.cache = cache;
    }

    /**
     * 写入缓存，触发 write-through：
     * <ol>
     *   <li>JCache 框架调用 {@code ProductCacheWriter.write()}，先将数据写入 DB。</li>
     *   <li>DB 写成功后，JCache 更新缓存条目。</li>
     *   <li>DB 写失败时，{@code CacheWriterException} 被抛出，缓存保持旧值。</li>
     * </ol>
     */
    @Override
    public void put(String key, Product value) {
        cache.put(key, value);
    }

    /**
     * 读取缓存，触发 read-through（当 key 不在缓存时）：
     * <ol>
     *   <li>key 在缓存中 → 直接返回，不访问 DB（缓存命中）。</li>
     *   <li>key 不在缓存 → JCache 框架调用 {@code ProductCacheLoader.load()}，
     *       从 DB 加载并回填缓存后返回（缓存未命中）。</li>
     *   <li>DB 也不存在 → 返回 {@link Optional#empty()}，缓存不存入空条目。</li>
     * </ol>
     */
    @Override
    public Optional<Product> get(String key) {
        return Optional.ofNullable(cache.get(key));
    }

    /**
     * 判断 key 是否在缓存中（不触发 read-through），仅用于命中/未命中指标采集。
     * <p>注意：{@code containsKey} 与 {@code get} 之间无原子性保证，不应用于业务判断。</p>
     */
    @Override
    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    /**
     * 批量读取，逐 key 通过 JCache 获取，触发各自的 read-through（如有必要）。
     */
    @Override
    public Map<String, Product> getAll(Collection<String> keys) {
        Map<String, Product> result = new HashMap<>();
        // JCache.getAll 同样触发 read-through，此处用原始 Map 接口适配 CacheProvider 签名
        cache.getAll(new HashSet<>(keys)).forEach(result::put);
        return result;
    }

    /**
     * 删除缓存条目，触发 write-through：
     * <ol>
     *   <li>JCache 框架调用 {@code ProductCacheWriter.delete()}，先从 DB 删除记录。</li>
     *   <li>DB 删除成功后，JCache 移除缓存条目。</li>
     *   <li>DB 删除失败时，{@code CacheWriterException} 被抛出，缓存条目保留。</li>
     * </ol>
     */
    @Override
    public void delete(String key) {
        cache.remove(key);
    }

    /**
     * 批量删除，同 {@link #delete}，触发 write-through 的 {@code deleteAll} 回调。
     */
    @Override
    public void deleteAll(Collection<String> keys) {
        cache.removeAll(new HashSet<>(keys));
    }

    /**
     * 清空缓存，<b>不触发</b> write-through（不会删除 DB 数据），仅用于测试/运维场景。
     */
    @Override
    public void clear() {
        cache.clear();
    }
}
