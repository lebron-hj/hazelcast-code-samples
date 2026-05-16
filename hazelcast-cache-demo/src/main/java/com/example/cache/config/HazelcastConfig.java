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

import com.hazelcast.config.ClasspathYamlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bootstraps an embedded {@link HazelcastInstance} from {@code hazelcast.yaml} and applies
 * configurable map settings (map name, TTL, max-size) from {@link CacheProperties}.
 *
 * <p>Near-cache is explicitly disabled to prevent stale reads across update/delete operations.
 */
@Configuration
public class HazelcastConfig {

    private final CacheProperties properties;

    /**
     * Constructs a new {@code HazelcastConfig} with the given cache properties.
     *
     * @param properties externalized cache configuration
     */
    public HazelcastConfig(CacheProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates and returns an embedded {@link HazelcastInstance}.
     *
     * <p>Base configuration is loaded from {@code hazelcast.yaml} on the classpath.
     * Dynamic settings (map name, TTL, max-size) are applied from {@link CacheProperties}.
     *
     * @return a fully configured Hazelcast instance
     */
    @Bean
    public HazelcastInstance hazelcastInstance() {
        Config config = new ClasspathYamlConfig("hazelcast.yaml");

        CacheProperties.HazelcastProperties hz = properties.getHazelcast();

        EvictionConfig eviction = new EvictionConfig()
            .setEvictionPolicy(EvictionPolicy.LRU)
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(hz.getMaxSize());

        MapConfig mapConfig = new MapConfig(hz.getMapName())
            .setTimeToLiveSeconds((int) hz.getTtlSeconds())
            .setEvictionConfig(eviction);

        config.addMapConfig(mapConfig);
        return Hazelcast.newHazelcastInstance(config);
    }
}
