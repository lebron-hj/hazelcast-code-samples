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

package com.example.cache.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Demo domain object stored both in MongoDB and in the Hazelcast IMap.
 * Implements {@link Serializable} so Hazelcast can serialize it when needed.
 */
@Document(collection = "entities")
public class CachedEntity implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    private String id;
    private String name;
    private String value;
    private Instant createdAt;
    private Instant updatedAt;

    /** Required by Spring Data and Hazelcast deserialization. */
    public CachedEntity() {
    }

    /**
     * Returns the unique entity identifier (MongoDB ObjectId string).
     *
     * @return entity id, or {@code null} if not yet persisted
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the entity identifier.
     *
     * @param id entity id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Returns the entity name.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the entity name.
     *
     * @param name name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the entity value payload.
     *
     * @return value
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the entity value payload.
     *
     * @param value value
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Returns the timestamp when this entity was first persisted.
     *
     * @return creation timestamp
     */
    public Instant getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the creation timestamp.
     *
     * @param createdAt creation timestamp
     */
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Returns the timestamp of the last update.
     *
     * @return last-updated timestamp
     */
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /**
     * Sets the last-updated timestamp.
     *
     * @param updatedAt last-updated timestamp
     */
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
