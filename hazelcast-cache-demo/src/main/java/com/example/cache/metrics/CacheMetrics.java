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

package com.example.cache.metrics;

import com.example.cache.config.CacheProperties;
import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Micrometer-based metrics for the cache layer.
 *
 * <p>Exposes the following meters:
 * <ul>
 *   <li>{@code cache.hit.count} (tag: map) — incremented on each cache hit</li>
 *   <li>{@code cache.miss.count} (tag: map) — incremented on each cache miss</li>
 *   <li>{@code cache.write.duration} (tag: operation) — full write-through latency</li>
 *   <li>{@code storage.read.duration} (tag: backend) — storage read latency on miss</li>
 *   <li>{@code storage.write.duration} (tag: backend, operation) — storage write latency</li>
 * </ul>
 */
@Component
public class CacheMetrics {

    private final MeterRegistry registry;
    private final String mapName;
    private final String backendName;
    private final Counter hitCounter;
    private final Counter missCounter;

    /**
     * Constructs {@code CacheMetrics} and pre-registers counters.
     *
     * @param registry    Micrometer meter registry
     * @param properties  cache configuration (supplies map name)
     * @param backend     active storage backend (supplies backend name for metric tags)
     */
    public CacheMetrics(MeterRegistry registry, CacheProperties properties,
                        StorageBackend<String, CachedEntity> backend) {
        this.registry = registry;
        this.mapName = properties.getHazelcast().getMapName();
        this.backendName = backend.backendName();
        this.hitCounter = Counter.builder("cache.hit.count")
            .tag("map", mapName)
            .description("Number of cache hits")
            .register(registry);
        this.missCounter = Counter.builder("cache.miss.count")
            .tag("map", mapName)
            .description("Number of cache misses")
            .register(registry);
    }

    /**
     * Increments the cache-hit counter.
     */
    public void recordHit() {
        hitCounter.increment();
    }

    /**
     * Increments the cache-miss counter.
     */
    public void recordMiss() {
        missCounter.increment();
    }

    /**
     * Times the full write-through operation (cache + storage) for the given {@code operation}.
     *
     * @param operation one of {@code insert}, {@code update}, {@code delete}
     * @param fn        the write operation to execute and time
     * @param <T>       return type of the operation
     * @return the value produced by {@code fn}
     */
    public <T> T timeCacheWrite(String operation, Supplier<T> fn) {
        return Timer.builder("cache.write.duration")
            .tag("operation", operation)
            .description("End-to-end latency of write-through cache operations")
            .register(registry)
            .record(fn);
    }

    /**
     * Times a storage-layer read (triggered on cache miss).
     *
     * @param fn  the storage read to execute and time
     * @param <T> return type of the read
     * @return the value produced by {@code fn}
     */
    public <T> T timeStorageRead(Supplier<T> fn) {
        return Timer.builder("storage.read.duration")
            .tag("backend", backendName)
            .description("Latency of storage backend reads on cache miss")
            .register(registry)
            .record(fn);
    }

    /**
     * Times a storage-layer write for the given {@code operation}.
     *
     * @param operation one of {@code insert}, {@code update}, {@code delete}
     * @param fn        the storage write to execute and time
     * @param <T>       return type of the operation
     * @return the value produced by {@code fn}
     */
    public <T> T timeStorageWrite(String operation, Supplier<T> fn) {
        return Timer.builder("storage.write.duration")
            .tag("backend", backendName)
            .tag("operation", operation)
            .description("Latency of storage backend write operations")
            .register(registry)
            .record(fn);
    }

    /**
     * Times a storage-layer void write for the given {@code operation}.
     *
     * @param operation one of {@code insert}, {@code update}, {@code delete}
     * @param action    the storage write to execute and time
     */
    public void timeStorageWriteVoid(String operation, Runnable action) {
        Timer.builder("storage.write.duration")
            .tag("backend", backendName)
            .tag("operation", operation)
            .description("Latency of storage backend write operations")
            .register(registry)
            .record(action);
    }

    /**
     * Times a storage-layer read returning an {@link Optional}.
     *
     * @param fn  the storage read to execute and time
     * @param <T> element type
     * @return the {@link Optional} produced by {@code fn}
     */
    public <T> Optional<T> timeStorageReadOptional(Supplier<Optional<T>> fn) {
        return Timer.builder("storage.read.duration")
            .tag("backend", backendName)
            .description("Latency of storage backend reads on cache miss")
            .register(registry)
            .record(fn);
    }
}
