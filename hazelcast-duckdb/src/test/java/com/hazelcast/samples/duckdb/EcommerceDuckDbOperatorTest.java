package com.hazelcast.samples.duckdb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EcommerceDuckDbOperatorTest {

    @BeforeAll
    static void setup() {
        // 测试时禁用攒批模式，确保结果立即返回
        System.setProperty("duckdb.batch-writing.enabled", "false");
    }

    @Test
    void processBatch_writesArrowDataAndReturnsJoinedWideRows() throws Exception {
        EcommerceOrderBatch batch = EcommerceMockGenerator.generateOrderBatch(1L);
        EcommerceDuckDbOperator operator = new EcommerceDuckDbOperator();
        try {
            List<Map<String, Object>> rows = operator.processBatch(batch);
            assertEquals(batch.items().size(), rows.size());
            Map<String, Object> row = rows.get(0);
            assertNotNull(row.get("buyer_id"));
            assertNotNull(row.get("order_id"));
            assertNotNull(row.get("item_id"));
            assertEquals(batch.buyer().buyerId(), ((Number) row.get("buyer_id")).longValue());
            assertEquals(batch.order().orderId(), ((Number) row.get("order_id")).longValue());
        } finally {
            operator.close();
        }
    }

    @Test
    void processBatch_deleteOnlyRemovesOrderFromWideTable() throws Exception {
        EcommerceOrderBatch batch = EcommerceMockGenerator.generateOrderBatch(1L);
        EcommerceDuckDbOperator operator = new EcommerceDuckDbOperator();
        try {
            operator.processBatch(batch);
            operator.processBatch(EcommerceOrderBatch.deleteOnly(2L, batch.order().orderId()));
            assertEquals(0, operator.queryWideRows(batch.order().orderId()).size());
        } finally {
            operator.close();
        }
    }
}
