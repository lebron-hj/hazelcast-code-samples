package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.Iterator;

/**
 * JCache 写穿透（write-through）回调实现，是强一致性的核心保障。
 *
 * <h3>调用时机</h3>
 * JCache 框架在执行 {@code cache.put()} 或 {@code cache.remove()} 之前，
 * 自动调用本类的 {@link #write} 或 {@link #delete} 方法，先将数据持久化到 DB，
 * 只有持久化成功后才更新缓存条目。
 *
 * <h3>强一致性保证（JSR-107 规范约定）</h3>
 * <ul>
 *   <li>若 {@link #write} 抛出 {@link CacheWriterException}，缓存条目<b>不会</b>被更新，
 *       保证缓存与 DB 不会出现"DB 失败但缓存已更新"的不一致状态。</li>
 *   <li>若 {@link #delete} 抛出 {@link CacheWriterException}，缓存条目<b>不会</b>被移除，
 *       保证缓存与 DB 不会出现"DB 失败但缓存已删除"的不一致状态。</li>
 *   <li>无需手动补偿（回滚）：失败即停，缓存自然保持旧值。</li>
 * </ul>
 *
 * <h3>写操作调用链路</h3>
 * <pre>
 *   StrictWriteThroughStrategy.insert/update(key, product)
 *       → JCacheHazelcastProvider.put(key, product)
 *           → javax.cache.Cache.put(key, product)
 *               → ProductCacheWriter.write(entry)          // JCache 先调用本方法（本类）
 *                   → DbRepository.insert 或 update       // 持久化到 DB
 *               → 缓存条目更新                              // DB 成功后才执行
 *
 *   StrictWriteThroughStrategy.delete(key)
 *       → JCacheHazelcastProvider.delete(key)
 *           → javax.cache.Cache.remove(key)
 *               → ProductCacheWriter.delete(key)           // JCache 先调用本方法（本类）
 *                   → DbRepository.delete(key)             // 从 DB 删除
 *               → 缓存条目移除                              // DB 成功后才执行
 * </pre>
 *
 * <h3>insert vs update 判断逻辑</h3>
 * {@link DbRepository} 将 insert 和 update 拆为两个独立方法。
 * 本类通过 {@code selectById} 探测 key 是否已存在，决定执行哪个操作（upsert 语义）。
 *
 * @see ProductCacheWriterFactory
 */
public class ProductCacheWriter implements CacheWriter<String, Product> {

    /** 底层数据库访问接口。 */
    private final DbRepository<String, Product> dbRepository;

    /**
     * @param dbRepository 由 {@link ProductCacheWriterFactory} 注入。
     */
    public ProductCacheWriter(DbRepository<String, Product> dbRepository) {
        this.dbRepository = dbRepository;
    }

    /**
     * 将缓存条目写入数据库（insert 或 update），由 JCache 在修改缓存前调用。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>探测 DB 中是否已有该 key（{@code selectById}）。</li>
     *   <li>不存在 → 执行 {@code insert}；已存在 → 执行 {@code update}。</li>
     *   <li>DB 操作成功后返回，JCache 框架继续更新缓存条目。</li>
     *   <li>DB 操作失败 → 抛出 {@link CacheWriterException}，JCache 取消缓存更新。</li>
     * </ol>
     * </p>
     *
     * @param entry 待写入的缓存条目（含 key 和 value）
     * @throws CacheWriterException DB 写入失败时抛出，缓存条目将保持不变
     */
    @Override
    public void write(Cache.Entry<? extends String, ? extends Product> entry) throws CacheWriterException {
        try {
            String key = entry.getKey();
            Product value = entry.getValue();
            // 根据 DB 中是否存在该 key 决定执行 insert 还是 update
            if (dbRepository.selectById(key).isPresent()) {
                dbRepository.update(key, value);
            } else {
                dbRepository.insert(key, value);
            }
        } catch (CacheWriterException cwe) {
            // 已经是规范异常，直接透传
            throw cwe;
        } catch (Exception ex) {
            // 将业务异常包装为 JCache 规范异常，通知框架取消缓存更新
            throw new CacheWriterException("DB 写入失败，key=" + entry.getKey(), ex);
        }
    }

    /**
     * 批量写入缓存条目到数据库，遵循 JCache 的 {@code iterator.remove()} 协议。
     *
     * <p>处理规则：
     * <ul>
     *   <li>每成功写入一条，立即调用 {@code it.remove()} 将其从集合中移除。</li>
     *   <li>JCache 框架通过检查集合剩余元素来判断哪些条目写入失败，
     *       只对成功写入的条目更新缓存。</li>
     *   <li>若某条写入失败抛出异常，集合中仍有未处理的条目，框架将跳过对应缓存更新。</li>
     * </ul>
     * </p>
     *
     * @param entries 待批量写入的缓存条目集合（处理完成后应为空）
     */
    @Override
    public void writeAll(Collection<Cache.Entry<? extends String, ? extends Product>> entries)
            throws CacheWriterException {
        Iterator<Cache.Entry<? extends String, ? extends Product>> it = entries.iterator();
        while (it.hasNext()) {
            write(it.next());
            // 写入成功后移除，通知 JCache 该条目已处理完毕
            it.remove();
        }
    }

    /**
     * 从数据库删除指定 key 的记录，由 JCache 在移除缓存条目前调用。
     *
     * <p>若 DB 删除失败，抛出 {@link CacheWriterException}，
     * JCache 框架将保留缓存条目（即缓存与 DB 保持一致，均有该记录）。</p>
     *
     * @param key 待删除的商品 ID
     * @throws CacheWriterException DB 删除失败时抛出
     */
    @Override
    public void delete(Object key) throws CacheWriterException {
        try {
            dbRepository.delete((String) key);
        } catch (Exception ex) {
            throw new CacheWriterException("DB 删除失败，key=" + key, ex);
        }
    }

    /**
     * 批量删除，遵循 {@code iterator.remove()} 协议，同 {@link #writeAll}。
     *
     * @param keys 待删除的 key 集合（处理完成后应为空）
     */
    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {
        Iterator<?> it = keys.iterator();
        while (it.hasNext()) {
            delete(it.next());
            // 删除成功后移除，通知 JCache 该 key 已处理完毕
            it.remove();
        }
    }
}
