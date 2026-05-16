package com.xxx.cache.config;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.xxx.cache.core.jcache.ProductCacheLoaderFactory;
import com.xxx.cache.core.jcache.ProductCacheWriterFactory;
import com.xxx.cache.core.provider.impl.JCacheHazelcastProvider;
import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * JCache（JSR-107）核心配置，负责将 Hazelcast JCache 实现与读/写穿透回调连接起来。
 *
 * <h3>装配关系</h3>
 * <pre>
 *   HazelcastInstance（HazelcastConfig 创建）
 *       ↓ propertiesByInstanceItself（绑定到已有节点）
 *   HazelcastCachingProvider → CacheManager
 *       ↓ createCache（配置 read-through + write-through）
 *   javax.cache.Cache&lt;String, Product&gt;
 *       ↓ 注入
 *   JCacheHazelcastProvider → CacheProvider（PluginConfig 注册为 Spring Bean）
 * </pre>
 *
 * <h3>关键配置说明</h3>
 * <ul>
 *   <li>{@code setReadThrough(true)}：缓存未命中时自动调用 {@link ProductCacheLoaderFactory}
 *       创建的 {@code ProductCacheLoader}，从 DB 加载并回填缓存。</li>
 *   <li>{@code setWriteThrough(true)}：{@code cache.put()} / {@code cache.remove()} 之前
 *       自动调用 {@link ProductCacheWriterFactory} 创建的 {@code ProductCacheWriter}，
 *       先写 DB，成功后才更新缓存。</li>
 *   <li>{@code CreatedExpiryPolicy}：条目创建时开始计时，TTL 到期后自动移除。</li>
 *   <li>{@code propertiesByInstanceItself}：将 JCache CacheManager 绑定到已有的
 *       {@link HazelcastInstance}，避免启动第二个 Hazelcast 节点。</li>
 * </ul>
 *
 * <h3>依赖顺序</h3>
 * 本配置类依赖 {@link RepositoryConfig} 提供的 {@code DbRepository} Bean（无循环依赖）：
 * <pre>
 *   RepositoryConfig → DbRepository
 *       ↑
 *   JCacheConfig（依赖 DbRepository 构造 Factory）
 *       ↑
 *   PluginConfig（依赖 JCacheHazelcastProvider 构造 CacheProvider）
 * </pre>
 */
@Configuration
public class JCacheConfig {

    /**
     * 创建并配置 Hazelcast JCache 实例，绑定到已有 Hazelcast 节点，
     * 启用 read-through + write-through，设置 TTL 过期策略。
     *
     * <p>若同名 Cache 已存在（应用重启或热加载场景），直接返回已有实例，不重复创建。</p>
     *
     * @param hazelcastInstance 已启动的 Hazelcast 节点
     * @param properties        缓存框架配置（Map 名称、TTL 等）
     * @param dbRepository      DB 访问接口，用于构造 Loader/Writer 工厂
     * @return 配置完毕的 {@code javax.cache.Cache} 实例
     */
    @Bean
    public Cache<String, Product> productJCache(HazelcastInstance hazelcastInstance,
                                                CacheFrameworkProperties properties,
                                                DbRepository<String, Product> dbRepository) {
        // 步骤①：获取 Hazelcast 的 JCache 实现（HazelcastCachingProvider）
        CachingProvider cachingProvider = Caching.getCachingProvider(
                HazelcastCachingProvider.class.getName());

        // 步骤②：创建 CacheManager，通过 propertiesByInstanceItself 绑定到已有节点，
        //         避免 Hazelcast 再启动一个新的 embedded 节点
        Properties hz = HazelcastCachingProvider.propertiesByInstanceItself(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager(null, null, hz);

        // 步骤③：防止重复创建——若同名 Cache 已存在则直接返回
        String cacheName = properties.getCache().getName();
        Cache<String, Product> existing = cacheManager.getCache(cacheName, String.class, Product.class);
        if (existing != null) {
            return existing;
        }

        // 步骤④：构造 Cache 配置
        int ttlSeconds = properties.getCache().getTtlSeconds();
        MutableConfiguration<String, Product> config = new MutableConfiguration<String, Product>()
                .setTypes(String.class, Product.class)
                // 启用读穿透：缓存未命中时 JCache 自动调用 ProductCacheLoader.load()
                .setReadThrough(true)
                // 启用写穿透：cache.put/remove 前 JCache 自动调用 ProductCacheWriter.write/delete()
                .setWriteThrough(true)
                // 注入 Loader 工厂：JCache 在初始化时调用 create() 获取 Loader 实例
                .setCacheLoaderFactory(new ProductCacheLoaderFactory(dbRepository))
                // 注入 Writer 工厂：JCache 在初始化时调用 create() 获取 Writer 实例
                .setCacheWriterFactory(new ProductCacheWriterFactory(dbRepository))
                // 设置 TTL：条目创建后经过 ttlSeconds 秒自动过期（CreatedExpiryPolicy 以创建时间起算）
                .setExpiryPolicyFactory(
                        CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, ttlSeconds)));

        // 步骤⑤：创建并返回配置好的 Cache
        return cacheManager.createCache(cacheName, config);
    }

    /**
     * 创建 {@link JCacheHazelcastProvider} Bean，作为 {@code CacheProvider} 的 JCache 实现注入。
     * <p>由 {@link PluginConfig} 根据配置将其注册为最终使用的 {@code CacheProvider} Bean。</p>
     *
     * @param productJCache 上方 Bean 创建的 JCache 实例
     * @return JCache 实现的 CacheProvider
     */
    @Bean
    public JCacheHazelcastProvider jCacheHazelcastProvider(Cache<String, Product> productJCache) {
        return new JCacheHazelcastProvider(productJCache);
    }
}
