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
        // 强制使用 Arrow 模式，验证我们的修复
        System.setProperty("duckdb.write.mode", "arrow");
        // 强制不使用共享服务模式（让每个处理器都有独立的连接）
        System.setProperty("duckdb.service.shared", "false");
    }

    @Test
    void run_largeScaleTest_NoIndexOutOfBounds() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            // 测试较大数据集（100批次，超过初始容量32768）
            System.out.println("\n📊 测试大数据集（100批次）...");
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(100);
            int expectedRows = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            System.out.println("  预计写入 " + expectedRows + " 行数据");
            
            List<java.util.Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertEquals(expectedRows, rows.size());
            assertFalse(rows.isEmpty());
            
            System.out.println("\n✅ 大型测试完成，IndexOutOfBoundsException 没有出现！");
        } finally {
            instance.shutdown();
        }
    }
}
