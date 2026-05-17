package com.hazelcast.samples.duckdb;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HazelcastDuckDbJobTest {

    @BeforeAll
    static void setup() {
        // 测试时禁用攒批模式，确保结果立即返回
        System.setProperty("duckdb.batch-writing.enabled", "false");
        // 强制不使用共享服务模式（让每个处理器都有独立的连接）
        System.setProperty("duckdb.service.shared", "false");
    }

    @AfterAll
    static void cleanup() {
        System.clearProperty("duckdb.write.mode");
        System.clearProperty("duckdb.service.shared");
    }

    @Test
    void run_largeScaleTest_ArrowMode() {
        System.setProperty("duckdb.write.mode", "arrow");
        
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            // 测试较大数据集（100批次，超过初始容量32768）
            System.out.println("\n📊 Arrow 模式测试大数据集（100批次）...");
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(100);
            int totalItems = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            System.out.println("  总订单项数 " + totalItems + " 行数据");
            
            List<java.util.Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertFalse(rows.isEmpty());
            
            System.out.println("\n✅ Arrow 模式大型测试完成，IndexOutOfBoundsException 没有出现！");
        } finally {
            instance.shutdown();
        }
    }

    @Test
    void run_largeScaleTest_CopyMode() {
        System.setProperty("duckdb.write.mode", "copy");
        
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            // 测试较大数据集（100批次）
            System.out.println("\n📊 Copy 模式测试大数据集（100批次）...");
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(100);
            int totalItems = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            System.out.println("  总订单项数 " + totalItems + " 行数据");
            
            List<java.util.Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertFalse(rows.isEmpty());
            
            System.out.println("\n✅ Copy 模式大型测试完成！");
        } finally {
            instance.shutdown();
        }
    }

    @Test
    void run_largeScaleTest_StandardMode() {
        System.setProperty("duckdb.write.mode", "standard");
        
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            // 测试较大数据集（100批次）
            System.out.println("\n📊 Standard 模式测试大数据集（100批次）...");
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(100);
            int totalItems = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            System.out.println("  总订单项数 " + totalItems + " 行数据");
            
            List<Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertFalse(rows.isEmpty());
            
            System.out.println("\n✅ Standard 模式大型测试完成！");
        } finally {
            instance.shutdown();
        }
    }

    @Test
    void run_largeScaleTest_AppenderMode() {
        System.setProperty("duckdb.write.mode", "appender");
        
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            // 测试较大数据集（100批次）
            System.out.println("\n📊 Appender 模式测试大数据集（100批次）...");
            List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(100);
            int totalItems = batches.stream().mapToInt(batch -> batch.items().size()).sum();
            System.out.println("  总订单项数 " + totalItems + " 行数据");
            
            List<Map<String, Object>> rows = HazelcastDuckDbJob.run(instance, batches);
            assertFalse(rows.isEmpty());
            
            System.out.println("\n✅ Appender 模式大型测试完成！");
        } finally {
            instance.shutdown();
        }
    }
}
