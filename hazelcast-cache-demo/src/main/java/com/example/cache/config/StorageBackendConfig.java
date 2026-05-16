/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.cache.config;

import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import com.example.cache.storage.fluss.FlussStorageBackend;
import com.example.cache.storage.jdbc.JdbcStorageBackend;
import com.example.cache.storage.mongodb.CachedEntityRepository;
import com.example.cache.storage.mongodb.MongoStorageBackend;
import com.example.cache.storage.rocksdb.RocksDbStorageBackend;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the active {@link StorageBackend} bean based on
 * the {@code cache.storage.backend} configuration property.
 *
 * <p>Switching the backend requires only a configuration change; no code modification needed.
 */
@Configuration
public class StorageBackendConfig {

    /**
     * Creates the MongoDB storage backend when {@code cache.storage.backend=mongodb}
     * (or when the property is absent — the default).
     *
     * @param repository Spring Data MongoDB repository
     * @return MongoDB-backed storage backend
     */
    @Bean
    @ConditionalOnProperty(name = "cache.storage.backend", havingValue = "mongodb", matchIfMissing = true)
    public StorageBackend<String, CachedEntity> mongoStorageBackend(CachedEntityRepository repository) {
        return new MongoStorageBackend(repository);
    }

    /**
     * Creates the RocksDB storage backend stub when {@code cache.storage.backend=rocksdb}.
     *
     * @return RocksDB-backed storage backend (stub — throws on every operation)
     */
    @Bean
    @ConditionalOnProperty(name = "cache.storage.backend", havingValue = "rocksdb")
    public StorageBackend<String, CachedEntity> rocksDbStorageBackend() {
        return new RocksDbStorageBackend();
    }

    /**
     * Creates the Fluss storage backend stub when {@code cache.storage.backend=fluss}.
     *
     * @return Fluss-backed storage backend (stub — throws on every operation)
     */
    @Bean
    @ConditionalOnProperty(name = "cache.storage.backend", havingValue = "fluss")
    public StorageBackend<String, CachedEntity> flussStorageBackend() {
        return new FlussStorageBackend();
    }

    /**
     * Creates the JDBC storage backend stub when {@code cache.storage.backend=jdbc}.
     *
     * @return JDBC-backed storage backend (stub — throws on every operation)
     */
    @Bean
    @ConditionalOnProperty(name = "cache.storage.backend", havingValue = "jdbc")
    public StorageBackend<String, CachedEntity> jdbcStorageBackend() {
        return new JdbcStorageBackend();
    }
}
