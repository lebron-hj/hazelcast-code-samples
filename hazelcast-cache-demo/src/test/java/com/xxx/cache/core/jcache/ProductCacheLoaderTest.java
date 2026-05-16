package com.xxx.cache.core.jcache;

import com.xxx.cache.core.repository.DbRepository;
import com.xxx.cache.entity.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.cache.integration.CacheLoaderException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProductCacheLoaderTest {

    private DbRepository<String, Product> dbRepository;
    private ProductCacheLoader loader;
    private Product product;

    @BeforeEach
    void setUp() {
        dbRepository = mock(DbRepository.class);
        loader = new ProductCacheLoader(dbRepository);
        product = new Product("p1", "widget", BigDecimal.TEN, Instant.now());
    }

    // Purpose: load() fetches the entity from DB and returns it for cache backfill.
    @Test
    void load_shouldReturnProductFromDb() {
        when(dbRepository.selectById("p1")).thenReturn(Optional.of(product));

        Product result = loader.load("p1");

        assertEquals(product, result);
        verify(dbRepository).selectById("p1");
    }

    // Purpose: load() returns null when key not found — JCache will not store a null entry.
    @Test
    void load_shouldReturnNullWhenKeyNotFoundInDb() {
        when(dbRepository.selectById("missing")).thenReturn(Optional.empty());

        Product result = loader.load("missing");

        assertNull(result);
    }

    // Purpose: DB exceptions must be wrapped as CacheLoaderException per JCache spec.
    @Test
    void load_shouldWrapDbExceptionAsCacheLoaderException() {
        when(dbRepository.selectById("p1")).thenThrow(new RuntimeException("DB connection refused"));

        assertThrows(CacheLoaderException.class, () -> loader.load("p1"));
    }

    // Purpose: loadAll() fetches multiple keys and omits nulls (keys with no DB record).
    @Test
    void loadAll_shouldReturnOnlyFoundEntries() {
        Product p2 = new Product("p2", "gadget", BigDecimal.ONE, Instant.now());
        when(dbRepository.selectById("p1")).thenReturn(Optional.of(product));
        when(dbRepository.selectById("p2")).thenReturn(Optional.of(p2));
        when(dbRepository.selectById("missing")).thenReturn(Optional.empty());

        Map<String, Product> result = loader.loadAll(List.of("p1", "p2", "missing"));

        assertEquals(2, result.size());
        assertEquals(product, result.get("p1"));
        assertEquals(p2, result.get("p2"));
    }
}
