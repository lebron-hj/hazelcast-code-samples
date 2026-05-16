/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.samples.duckdb;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

public final class HazelcastDuckDbApplication {

    private HazelcastDuckDbApplication() {
    }

    public static void main(String[] args) {
        // 打印启动信息
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Hazelcast + DuckDB 高性能写入测试启动");
        System.out.println("=".repeat(60));
        
        // 创建 Hazelcast 配置
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        
        // 设置 Jet 并行度
        config.getJetConfig().setInstanceConfig(config.getJetConfig().getInstanceConfig().setCooperativeThreadCount(PerfConfig.JET_PARALLELISM));
        
        // 创建 Hazelcast 实例
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        
        try {
            // 解析命令行参数
            int repeatCount = resolveRepeatCount(args);
            boolean useRealtimeSource = isRealtimeMode(args);
            int targetQps = resolveTargetQps(args);
            
            if (useRealtimeSource) {
                // 使用高性能实时数据源
                System.out.println("\n【使用高性能实时数据源模式】");
                System.out.printf("目标QPS: %d (0表示无限制)%n", targetQps);
                System.out.println("注意：此模式会持续生成数据，按 Ctrl+C 停止");
                
                // 运行作业（实时模式不会返回，会一直运行）
                HazelcastDuckDbJob.runWithRealtimeSource(hazelcastInstance, targetQps);
            } else {
                // 生成测试数据批次（原有模式）
                System.out.println("\n正在生成测试数据...");
                List<EcommerceOrderBatch> batches = EcommerceMockGenerator.generateBatches(PerfConfig.DEFAULT_BATCH_COUNT);
                System.out.printf("已生成 %d 个批次数据%n", batches.size());
                
                // 运行作业
                System.out.println("\n开始运行 DuckDB 写入作业...");
                List<java.util.Map<String, Object>> wideRows = HazelcastDuckDbJob.run(hazelcastInstance, batches, repeatCount);
                
                // 打印结果摘要
                System.out.println("\n【作业执行结果】");
                System.out.printf("生成的批次数: %,d%n", batches.size());
                System.out.printf("回放轮数 (repeatCount): %d%n", repeatCount);
                System.out.printf("宽表总行数: %,d%n", wideRows.size());
                
                // 打印第一条记录（如果有）
                if (!wideRows.isEmpty()) {
                    System.out.println("\n【示例宽表记录】");
                    System.out.println(wideRows.get(0));
                }
                
                System.out.println("\n" + "=".repeat(60));
                System.out.println("    作业执行完成");
                System.out.println("=".repeat(60));
            }
            
        } finally {
            // 停止统计收集器
            StatsCollector.getInstance().stop();
            // 关闭 Hazelcast 实例
            hazelcastInstance.shutdown();
        }
    }

    /**
     * 解析 repeatCount 参数
     * 优先级：命令行参数 > 系统属性 > 默认值
     */
    private static int resolveRepeatCount(String[] args) {
        // 优先从命令行参数获取（跳过特殊参数）
        if (args != null) {
            for (String arg : args) {
                // 跳过特殊参数
                if (arg.startsWith("--") || arg.startsWith("-")) {
                    continue;
                }
                try {
                    return Integer.parseInt(arg);
                } catch (NumberFormatException e) {
                    // 不是数字，继续
                }
            }
        }
        
        // 其次从系统属性获取
        String configured = System.getProperty("duckdb.stream.repeat-count");
        if (configured != null && !configured.isBlank()) {
            return Integer.parseInt(configured);
        }
        
        // 返回默认值
        return PerfConfig.DEFAULT_STREAM_REPEAT_COUNT;
    }
    
    /**
     * 检查是否使用实时数据源模式
     * 通过命令行参数 "--realtime" 或系统属性 "duckdb.source.realtime=true" 启用
     */
    private static boolean isRealtimeMode(String[] args) {
        // 检查命令行参数
        if (args != null) {
            for (String arg : args) {
                if ("--realtime".equalsIgnoreCase(arg) || "-rt".equalsIgnoreCase(arg)) {
                    return true;
                }
            }
        }
        
        // 检查系统属性
        String configured = System.getProperty("duckdb.source.realtime");
        if ("true".equalsIgnoreCase(configured)) {
            return true;
        }
        
        return false;
    }
    
    /**
     * 解析目标QPS参数
     * 优先级：命令行参数 "--qps=xxx" > 系统属性 > 默认值
     */
    private static int resolveTargetQps(String[] args) {
        // 检查命令行参数
        if (args != null) {
            for (String arg : args) {
                if (arg.startsWith("--qps=")) {
                    return Integer.parseInt(arg.substring(6));
                }
            }
        }
        
        // 检查系统属性
        String configured = System.getProperty("duckdb.generation.qps");
        if (configured != null && !configured.isBlank()) {
            return Integer.parseInt(configured);
        }
        
        // 返回默认值
        return PerfConfig.DATA_GENERATION_QPS;
    }
}