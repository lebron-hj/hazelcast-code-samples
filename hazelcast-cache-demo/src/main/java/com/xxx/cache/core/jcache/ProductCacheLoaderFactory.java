package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;

import javax.cache.configuration.Factory;

/**
 * {@link ProductCacheLoader} 的工厂类，由 JCache 框架通过 {@link Factory#create()} 获取实例。
 *
 * <h3>为什么需要 Factory</h3>
 * JCache 规范要求 {@code CacheLoader} 通过工厂创建，而不是直接传入实例。
 * 在分布式部署时，工厂需要序列化到各节点；本项目为嵌入式单节点，
 * 字段标注 {@code transient} 避免序列化 {@link DbRepository}（它持有 DB 连接等不可序列化资源）。
 *
 * <h3>生命周期</h3>
 * Spring 容器将工厂实例注入 {@link com.xxx.cache.config.JCacheConfig}，
 * JCache 框架启动时调用一次 {@link #create()} 获取 Loader 实例并复用。
 *
 * @see ProductCacheLoader
 */
public class ProductCacheLoaderFactory implements Factory<ProductCacheLoader> {

    /**
     * transient：单节点场景下 DbRepository 无需跨 JVM 序列化；
     * 若迁移到多节点集群，需改为可序列化的配置对象或通过 JNDI 等方式注入。
     */
    private final transient DbRepository<String, Product> dbRepository;

    /**
     * @param dbRepository 数据库访问实现，由 Spring 容器注入。
     */
    public ProductCacheLoaderFactory(DbRepository<String, Product> dbRepository) {
        this.dbRepository = dbRepository;
    }

    /**
     * 创建并返回一个新的 {@link ProductCacheLoader} 实例。
     * <p>JCache 框架在初始化 Cache 时调用一次，后续复用同一实例。</p>
     */
    @Override
    public ProductCacheLoader create() {
        return new ProductCacheLoader(dbRepository);
    }
}
