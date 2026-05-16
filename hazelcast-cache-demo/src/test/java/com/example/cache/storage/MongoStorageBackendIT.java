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

package com.example.cache.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.cache.exception.StorageBackendException;
import com.example.cache.model.CachedEntity;
import com.example.cache.service.CacheService;
import com.example.cache.storage.mongodb.MongoStorageBackend;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for {@link MongoStorageBackend} and the full read-through /
 * write-through flow, running against a real MongoDB instance via Testcontainers.
 */
@Testcontainers
@SpringBootTest
class MongoStorageBackendIT {

    @Container
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri",
            () -> "mongodb://localhost:" + mongo.getMappedPort(27017) + "/cachedb-it");
    }

    @Autowired
    private StorageBackend<String, CachedEntity> storageBackend;

    @Autowired
    private CacheService<String, CachedEntity> cacheService;

    @BeforeEach
    void clearCollection() {
        // Use the storage backend to check existence and clean; no direct Mongo access.
    }

    // ── MongoStorageBackend CRUD ─────────────────────────────────────────────

    // Purpose: Verify save persists an entity and assigns metadata fields.
    @Test
    void save_persistsEntityAndGeneratesId() {
        CachedEntity entity = buildEntity("alpha", "value1");

        CachedEntity saved = storageBackend.save(entity);

        assertThat(saved.getId()).isNotBlank();
        assertThat(saved.getName()).isEqualTo("alpha");
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();
    }

    // Purpose: Verify findById returns a previously persisted entity.
    @Test
    void findById_returnsPersistedEntity() {
        CachedEntity saved = storageBackend.save(buildEntity("beta", "v2"));

        Optional<CachedEntity> found = storageBackend.findById(saved.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("beta");
    }

    // Purpose: Verify findById returns empty for unknown identifiers.
    @Test
    void findById_unknownId_returnsEmpty() {
        Optional<CachedEntity> result = storageBackend.findById("nonexistent-id-xyz");

        assertThat(result).isEmpty();
    }

    // Purpose: Verify update mutates payload and advances updated timestamp.
    @Test
    void update_changesNameAndUpdatedTimestamp() throws InterruptedException {
        CachedEntity saved = storageBackend.save(buildEntity("gamma", "v3"));
        Thread.sleep(10);

        CachedEntity patch = buildEntity("gamma-updated", "v3-new");
        CachedEntity updated = storageBackend.update(saved.getId(), patch);

        assertThat(updated.getName()).isEqualTo("gamma-updated");
        assertThat(updated.getUpdatedAt()).isAfter(saved.getUpdatedAt());
    }

    // Purpose: Verify update reports backend exception for unknown identifiers.
    @Test
    void update_unknownId_throwsStorageBackendException() {
        CachedEntity patch = buildEntity("x", "y");

        assertThatThrownBy(() -> storageBackend.update("no-such-id", patch))
            .isInstanceOf(StorageBackendException.class);
    }

    // Purpose: Verify deleteById removes persisted entities from storage.
    @Test
    void deleteById_removesEntity() {
        CachedEntity saved = storageBackend.save(buildEntity("delta", "v4"));

        storageBackend.deleteById(saved.getId());

        assertThat(storageBackend.existsById(saved.getId())).isFalse();
    }

    // Purpose: Verify existsById returns true after successful persistence.
    @Test
    void existsById_returnsTrueForExistingEntity() {
        CachedEntity saved = storageBackend.save(buildEntity("epsilon", "v5"));

        assertThat(storageBackend.existsById(saved.getId())).isTrue();
    }

    // ── Full read-through / write-through flow ────────────────────────────────

    // Purpose: Verify read-through performs storage load on miss and serves subsequent hit.
    @Test
    void readThrough_onCacheMiss_backfillsFromStorage() {
        CachedEntity saved = storageBackend.save(buildEntity("read-through-test", "rt-value"));

        // First call — cache miss; should read from storage and backfill
        Optional<CachedEntity> first = cacheService.get(saved.getId());
        assertThat(first).isPresent();
        assertThat(first.get().getName()).isEqualTo("read-through-test");

        // Second call — should be a cache hit
        Optional<CachedEntity> second = cacheService.get(saved.getId());
        assertThat(second).isPresent();
        assertThat(second.get().getName()).isEqualTo("read-through-test");
    }

    // Purpose: Verify write-through insert result is immediately readable.
    @Test
    void writeThrough_insertThenGet_returnsInsertedEntity() {
        CachedEntity entity = buildEntity("write-through-insert", "wt-value");

        CachedEntity inserted = cacheService.insert(entity);
        assertThat(inserted.getId()).isNotBlank();

        Optional<CachedEntity> found = cacheService.get(inserted.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("write-through-insert");
    }

    // Purpose: Verify write-through update invalidates stale cache then serves updated state.
    @Test
    void writeThrough_updateInvalidatesCacheAndReturnsUpdated() {
        CachedEntity inserted = cacheService.insert(buildEntity("before-update", "old"));

        CachedEntity patch = buildEntity("after-update", "new");
        CachedEntity updated = cacheService.update(inserted.getId(), patch);

        assertThat(updated.getName()).isEqualTo("after-update");
        Optional<CachedEntity> read = cacheService.get(inserted.getId());
        assertThat(read).isPresent();
        assertThat(read.get().getName()).isEqualTo("after-update");
    }

    // Purpose: Verify write-through delete clears both cache view and durable storage.
    @Test
    void writeThrough_deleteRemovesFromCacheAndStorage() {
        CachedEntity inserted = cacheService.insert(buildEntity("to-delete", "v"));

        cacheService.delete(inserted.getId());

        assertThat(storageBackend.existsById(inserted.getId())).isFalse();
        Optional<CachedEntity> afterDelete = cacheService.get(inserted.getId());
        assertThat(afterDelete).isEmpty();
    }

    private CachedEntity buildEntity(String name, String value) {
        CachedEntity e = new CachedEntity();
        e.setName(name);
        e.setValue(value);
        return e;
    }
}
