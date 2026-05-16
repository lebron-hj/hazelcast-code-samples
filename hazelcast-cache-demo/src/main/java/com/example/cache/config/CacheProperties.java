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

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Externalized configuration for the cache layer, bound from the {@code cache.*} namespace.
 */
@ConfigurationProperties(prefix = "cache")
@Validated
public class CacheProperties {

    @Valid
    @NotNull
    private HazelcastProperties hazelcast = new HazelcastProperties();

    @Valid
    @NotNull
    private StorageProperties storage = new StorageProperties();

    /**
     * Returns Hazelcast-specific configuration properties.
     *
     * @return hazelcast properties
     */
    public HazelcastProperties getHazelcast() {
        return hazelcast;
    }

    /**
     * Sets Hazelcast-specific configuration properties.
     *
     * @param hazelcast hazelcast properties
     */
    public void setHazelcast(HazelcastProperties hazelcast) {
        this.hazelcast = hazelcast;
    }

    /**
     * Returns storage backend configuration properties.
     *
     * @return storage properties
     */
    public StorageProperties getStorage() {
        return storage;
    }

    /**
     * Sets storage backend configuration properties.
     *
     * @param storage storage properties
     */
    public void setStorage(StorageProperties storage) {
        this.storage = storage;
    }

    /**
     * Hazelcast IMap configuration.
     */
    public static class HazelcastProperties {

        @NotBlank
        private String mapName = "entity-cache";

        @Min(1)
        private long ttlSeconds = 300;

        @Min(1)
        private int maxSize = 10000;

        /**
         * Returns the IMap name used for caching entities.
         *
         * @return map name
         */
        public String getMapName() {
            return mapName;
        }

        /**
         * Sets the IMap name.
         *
         * @param mapName map name
         */
        public void setMapName(String mapName) {
            this.mapName = mapName;
        }

        /**
         * Returns the time-to-live in seconds for cached entries.
         *
         * @return TTL in seconds
         */
        public long getTtlSeconds() {
            return ttlSeconds;
        }

        /**
         * Sets the time-to-live in seconds for cached entries.
         *
         * @param ttlSeconds TTL seconds
         */
        public void setTtlSeconds(long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
        }

        /**
         * Returns the maximum number of entries per node (PER_NODE policy).
         *
         * @return max size
         */
        public int getMaxSize() {
            return maxSize;
        }

        /**
         * Sets the maximum number of entries per node.
         *
         * @param maxSize max size
         */
        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }
    }

    /**
     * Storage backend selection configuration.
     */
    public static class StorageProperties {

        @NotBlank
        private String backend = "mongodb";

        /**
         * Returns the active storage backend identifier.
         *
         * @return backend name (e.g. {@code mongodb}, {@code rocksdb}, {@code fluss}, {@code jdbc})
         */
        public String getBackend() {
            return backend;
        }

        /**
         * Sets the active storage backend identifier.
         *
         * @param backend backend name
         */
        public void setBackend(String backend) {
            this.backend = backend;
        }
    }
}
