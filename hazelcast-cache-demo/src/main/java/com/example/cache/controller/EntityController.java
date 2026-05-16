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

package com.example.cache.controller;

import com.example.cache.model.CachedEntity;
import com.example.cache.service.CacheService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller exposing cache operations for {@link CachedEntity}.
 *
 * <p>No business logic lives here — all operations are delegated to {@link CacheService}.
 * All reads use read-through semantics; all writes use write-through semantics.
 */
@RestController
@RequestMapping("/api/entities")
public class EntityController {

    private final CacheService<String, CachedEntity> cacheService;

    /**
     * Constructs the controller with the required cache service.
     *
     * @param cacheService read-through / write-through cache service
     */
    public EntityController(CacheService<String, CachedEntity> cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * Inserts a new entity (write-through: storage first, then cache).
     *
     * @param entity entity payload from the request body
     * @return {@code 201 Created} with the persisted entity (including generated ID)
     */
    @PostMapping
    public ResponseEntity<CachedEntity> insert(@RequestBody CachedEntity entity) {
        return ResponseEntity.status(HttpStatus.CREATED).body(cacheService.insert(entity));
    }

    /**
     * Retrieves an entity by ID (read-through on cache miss).
     *
     * @param id entity identifier
     * @return {@code 200 OK} with the entity, or {@code 404 Not Found} if absent
     */
    @GetMapping("/{id}")
    public ResponseEntity<CachedEntity> get(@PathVariable String id) {
        return cacheService.get(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Updates an existing entity (write-through: cache invalidated first, then storage updated).
     *
     * @param id     entity identifier
     * @param entity new entity state from the request body
     * @return {@code 200 OK} with the updated entity
     */
    @PutMapping("/{id}")
    public ResponseEntity<CachedEntity> update(@PathVariable String id, @RequestBody CachedEntity entity) {
        return ResponseEntity.ok(cacheService.update(id, entity));
    }

    /**
     * Deletes an entity (write-through: cache invalidated first, then deleted from storage).
     *
     * @param id entity identifier
     * @return {@code 204 No Content}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        cacheService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
