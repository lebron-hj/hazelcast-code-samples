package com.hazelcast.samples.duckdb;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HazelcastDuckDbJobTest {

    @BeforeAll
    static void setup() {
        // 测试时禁用攒批模式，确保结果立即返回
        System.setProperty("duckdb.batch-writing.enabled", "false");
    }

    @Test
    void run_pipelineProducesFlattenedWideRows() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(2);
            int expectedRows = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            List<java.util.Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertEquals(expectedRows, rows.size());
            assertFalse(rows.isEmpty());
        } finally {
            instance.shutdown();
        }
    }
}
