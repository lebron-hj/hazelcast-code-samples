package com.hazelcast.samples.duckdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EcommerceMockGeneratorTest {

    @Test
    void generateOrderBatch_keepsBuyerOrderAndItemKeysAligned() {
        EcommerceOrderBatch batch = EcommerceMockGenerator.generateOrderBatch(1L);
        assertNotNull(batch.buyer());
        assertNotNull(batch.order());
        assertFalse(batch.items().isEmpty());
        assertEquals(batch.buyer().buyerId(), batch.order().buyerId());
        for (EcommerceOrderItem item : batch.items()) {
            assertEquals(batch.order().orderId(), item.orderId());
        }
    }

    @Test
    void generateBatches_returnsExpectedCount() {
        assertEquals(3, EcommerceMockGenerator.generateBatches(3).size());
    }
}

