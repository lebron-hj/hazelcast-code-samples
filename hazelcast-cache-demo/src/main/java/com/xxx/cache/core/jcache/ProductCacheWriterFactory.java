package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;

import javax.cache.configuration.Factory;

/**
 * {@link ProductCacheWriter} 的工厂类，由 JCache 框架通过 {@link Factory#create()} 获取实例。
 *
 * <h3>为什么需要 Factory</h3>
 * JCache 规范要求 {@code CacheWriter} 通过工厂创建，不能直接传入实例。
 * 在分布式部署时，工厂需要序列化到各节点；本项目为嵌入式单节点，
 * 字段标注 {@code transient} 以避免序列化持有 DB 连接的 {@link DbRepository}。
 *
 * <h3>生命周期</h3>
 * Spring 容器将工厂实例注入 {@link com.xxx.cache.config.JCacheConfig}，
 * JCache 框架启动时调用一次 {@link #create()} 获取 Writer 实例并复用。
 *
 * @see ProductCacheWriter
 */
public class ProductCacheWriterFactory implements Factory<ProductCacheWriter> {

    /**
     * transient：DbRepository 在嵌入式单节点场景下无需序列化。
     * 如迁移至多节点集群，需改为可序列化的代理或通过 Hazelcast UserContext 传递。
     */
    private final transient DbRepository<String, Product> dbRepository;

    /**
     * @param dbRepository 数据库访问实现，由 Spring 容器注入。
     */
    public ProductCacheWriterFactory(DbRepository<String, Product> dbRepository) {
        this.dbRepository = dbRepository;
    }

    /**
     * 创建并返回一个新的 {@link ProductCacheWriter} 实例。
     * <p>JCache 框架在初始化 Cache 时调用一次，后续复用同一实例。</p>
     */
    @Override
    public ProductCacheWriter create() {
        return new ProductCacheWriter(dbRepository);
    }
}
