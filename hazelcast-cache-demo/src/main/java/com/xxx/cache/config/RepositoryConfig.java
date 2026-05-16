package com.xxx.cache.config;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.core.repository.extensions.FlussRepository;
import com.xxx.cache.core.repository.extensions.JdbcRepository;
import com.xxx.cache.core.repository.impl.MongoDbRepository;
import com.xxx.cache.entity.Product;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RepositoryConfig {

    @Bean
    public DbRepository<String, Product> dbRepository(CacheFrameworkProperties properties,
                                                      MongoDbRepository mongoDbRepository) {
        String repository = properties.getDatabase().getRepository();
        if ("mongo".equalsIgnoreCase(repository)) {
            return mongoDbRepository;
        }
        if ("jdbc".equalsIgnoreCase(repository)) {
            return new JdbcRepository();
        }
        if ("fluss".equalsIgnoreCase(repository)) {
            return new FlussRepository();
        }
        throw new IllegalArgumentException("Unsupported DB repository: " + repository);
    }
}
