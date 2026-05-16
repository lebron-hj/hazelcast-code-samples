package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProductCacheWriter}.
 *
 * <p>These tests verify the strong-consistency guarantee: the DB write happens inside
 * {@code write()}/{@code delete()} which the JCache framework calls BEFORE mutating the cache.
 * If the writer throws {@link CacheWriterException}, the JCache spec mandates the cache
 * entry is left unchanged.</p>
 */
class ProductCacheWriterTest {

    private DbRepository<String, Product> dbRepository;
    private ProductCacheWriter writer;
    private Product product;

    @BeforeEach
    void setUp() {
        dbRepository = mock(DbRepository.class);
        writer = new ProductCacheWriter(dbRepository);
        product = new Product("p1", "widget", BigDecimal.TEN, Instant.now());
    }

    // Purpose: write() calls insert when the key is absent from DB (new entry).
    @Test
    void write_shouldCallDbInsertWhenKeyIsNew() {
        when(dbRepository.selectById("p1")).thenReturn(Optional.empty());

        writer.write(entry("p1", product));

        verify(dbRepository).insert("p1", product);
        verify(dbRepository, never()).update("p1", product);
    }

    // Purpose: write() calls update when the key already exists in DB (existing entry).
    @Test
    void write_shouldCallDbUpdateWhenKeyExists() {
        when(dbRepository.selectById("p1")).thenReturn(Optional.of(product));

        writer.write(entry("p1", product));

        verify(dbRepository).update("p1", product);
        verify(dbRepository, never()).insert("p1", product);
    }

    // Purpose: DB failure during write() must be surfaced as CacheWriterException so the
    // JCache framework knows the cache entry must NOT be updated.
    @Test
    void write_shouldWrapDbExceptionAsCacheWriterException() {
        when(dbRepository.selectById("p1")).thenReturn(Optional.empty());
        doThrow(new RuntimeException("DB down")).when(dbRepository).insert("p1", product);

        assertThrows(CacheWriterException.class, () -> writer.write(entry("p1", product)));
    }

    // Purpose: delete() removes the record from DB BEFORE the JCache entry is evicted.
    @Test
    void delete_shouldCallDbDelete() {
        writer.delete("p1");

        verify(dbRepository).delete("p1");
    }

    // Purpose: DB failure during delete() must be surfaced as CacheWriterException.
    @Test
    void delete_shouldWrapDbExceptionAsCacheWriterException() {
        doThrow(new RuntimeException("constraint violation")).when(dbRepository).delete("p1");

        assertThrows(CacheWriterException.class, () -> writer.delete("p1"));
    }

    // Purpose: writeAll() removes entries from the collection after successful write
    // (iterator.remove() protocol signals to JCache which entries succeeded).
    @Test
    void writeAll_shouldClearCollectionAfterSuccessfulWrites() {
        when(dbRepository.selectById("p1")).thenReturn(Optional.empty());
        when(dbRepository.selectById("p2")).thenReturn(Optional.empty());
        Product p2 = new Product("p2", "gadget", BigDecimal.ONE, Instant.now());

        Collection<Cache.Entry<? extends String, ? extends Product>> entries = new ArrayList<>();
        entries.add(entry("p1", product));
        entries.add(entry("p2", p2));

        writer.writeAll(entries);

        assertTrue(entries.isEmpty());
        verify(dbRepository).insert("p1", product);
        verify(dbRepository).insert("p2", p2);
    }

    // Purpose: deleteAll() clears the key collection after successful deletes.
    @Test
    void deleteAll_shouldClearCollectionAfterSuccessfulDeletes() {
        Collection<Object> keys = new ArrayList<>(List.of("p1", "p2"));

        writer.deleteAll(keys);

        assertTrue(keys.isEmpty());
        verify(dbRepository).delete("p1");
        verify(dbRepository).delete("p2");
    }

    private Cache.Entry<String, Product> entry(String key, Product value) {
        return new Cache.Entry<>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Product getValue() {
                return value;
            }

            @Override
            public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
