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

package com.example.cache.storage.jdbc;

import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import java.util.Optional;

/**
 * Placeholder {@link StorageBackend} for a future JDBC-backed implementation.
 *
 * <p>Activate by setting {@code cache.storage.backend=jdbc} in {@code application.yaml}.
 * Replace each TODO method body with the actual JDBC / Spring JDBC template integration.
 */
public class JdbcStorageBackend implements StorageBackend<String, CachedEntity> {

    /**
     * Constructs a new JDBC storage backend stub.
     */
    public JdbcStorageBackend() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<CachedEntity> findById(String id) {
        // TODO: implement SQL SELECT by primary key
        throw new UnsupportedOperationException("JDBC backend not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CachedEntity save(CachedEntity entity) {
        // TODO: implement SQL INSERT with generated key retrieval
        throw new UnsupportedOperationException("JDBC backend not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CachedEntity update(String id, CachedEntity entity) {
        // TODO: implement SQL UPDATE by primary key
        throw new UnsupportedOperationException("JDBC backend not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteById(String id) {
        // TODO: implement SQL DELETE by primary key
        throw new UnsupportedOperationException("JDBC backend not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean existsById(String id) {
        // TODO: implement SQL EXISTS check by primary key
        throw new UnsupportedOperationException("JDBC backend not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String backendName() {
        return "jdbc";
    }
}
