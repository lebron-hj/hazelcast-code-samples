package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;

/**
 * JCache 读穿透（read-through）回调实现。
 *
 * <h3>调用时机</h3>
 * 当调用者执行 {@code cache.get(key)}，且 key 在 Hazelcast IMap 中不存在时，
 * JCache 框架自动调用 {@link #load(String)} 从数据库加载数据，并将结果回填到缓存中，
 * 整个过程对调用者透明。
 *
 * <h3>调用链路</h3>
 * <pre>
 *   ReadThroughStrategy.getById(key)
 *       → JCacheHazelcastProvider.get(key)
 *           → javax.cache.Cache.get(key)                 // 缓存未命中
 *               → ProductCacheLoader.load(key)           // JCache 框架自动调用（本类）
 *                   → DbRepository.selectById(key)       // 从数据库加载
 *               → cache.put(key, product)                // JCache 框架自动回填缓存
 *       → 返回 product
 * </pre>
 *
 * <h3>强一致性保证</h3>
 * load 的结果是数据库的"快照"，回填后缓存与 DB 保持一致。
 * 若 DB 中不存在该 key，返回 {@code null}，JCache 不会存入空条目。
 *
 * @see ProductCacheLoaderFactory
 */
public class ProductCacheLoader implements CacheLoader<String, Product> {

    /** 底层数据库访问接口，具体实现可为 MongoDB / JDBC 等。 */
    private final DbRepository<String, Product> dbRepository;

    /**
     * @param dbRepository 由 {@link ProductCacheLoaderFactory} 注入，共享同一实例。
     */
    public ProductCacheLoader(DbRepository<String, Product> dbRepository) {
        this.dbRepository = dbRepository;
    }

    /**
     * 按 key 从数据库加载单条商品记录。
     *
     * <p>返回值约定：
     * <ul>
     *   <li>找到记录 → 返回 {@link Product} 对象，JCache 将其存入缓存。</li>
     *   <li>记录不存在 → 返回 {@code null}，JCache 不写入空条目（避免空值污染缓存）。</li>
     *   <li>DB 异常 → 包装为 {@link CacheLoaderException} 抛出，JCache 将不更新缓存。</li>
     * </ul>
     * </p>
     *
     * @param key 商品 ID
     * @return 商品对象，或 {@code null}（DB 中不存在）
     * @throws CacheLoaderException 数据库访问失败时抛出
     */
    @Override
    public Product load(String key) throws CacheLoaderException {
        try {
            return dbRepository.selectById(key).orElse(null);
        } catch (Exception ex) {
            throw new CacheLoaderException("从数据库加载 key 失败: " + key, ex);
        }
    }

    /**
     * 批量加载多个 key 对应的商品记录。
     *
     * <p>逐 key 调用 {@link #load}，加载结果中不包含 {@code null}（即 DB 不存在的 key
     * 不会出现在返回 Map 中）。</p>
     *
     * @param keys 商品 ID 集合
     * @return key → Product 映射（只含 DB 中存在的条目）
     * @throws CacheLoaderException 任意 key 加载失败时抛出
     */
    @Override
    public Map<String, Product> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
        Map<String, Product> result = new HashMap<>();
        for (String key : keys) {
            Product product = load(key);
            if (product != null) {
                // 过滤 null，只将实际存在的记录放入结果
                result.put(key, product);
            }
        }
        return result;
    }
}
