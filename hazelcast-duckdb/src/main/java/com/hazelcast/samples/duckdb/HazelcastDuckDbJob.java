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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public final class HazelcastDuckDbJob {

    private HazelcastDuckDbJob() {
    }

    /**
     * 运行 DuckDB 作业（默认回放1次）
     */
    public static List<Map<String, Object>> run(HazelcastInstance hazelcastInstance,
                                                List<EcommerceOrderBatch> batches) {
        return run(hazelcastInstance, batches, 1);
    }

    /**
     * 运行 DuckDB 作业（使用预生成数据）
     *
     * @param hazelcastInstance Hazelcast 实例
     * @param batches           批次数据列表
     * @param repeatCount       流重复回放次数（0表示无限）
     * @return 宽表查询结果
     */
    public static List<Map<String, Object>> run(HazelcastInstance hazelcastInstance,
                                                List<EcommerceOrderBatch> batches,
                                                int repeatCount) {
        if (repeatCount < 0) {
            throw new IllegalArgumentException("repeatCount must be >= 0 (0 means infinite)");
        }
        
        // 打印配置摘要
        System.out.println(PerfConfig.getConfigSummary());
        
        // 重置统计收集器
        StatsCollector.getInstance().reset();
        
        // 清空结果列表
        hazelcastInstance.getList(PerfConfig.RESULT_LIST_NAME).clear();
        
        // 构建 Pipeline
        Pipeline pipeline = Pipeline.create();
        
        // 构建流源
        StreamSource<EcommerceOrderBatch> source = buildStreamingSource(batches, repeatCount);
        
        return runWithSource(hazelcastInstance, pipeline, source);
    }
    
    /**
     * 运行 DuckDB 作业（使用实时高性能数据源）
     * 特点：实时生成新数据，QPS可调节，不会成为性能瓶颈
     *
     * @param hazelcastInstance Hazelcast 实例
     * @param targetQps         目标QPS（每秒生成的批次数），0表示无限制
     * @return 宽表查询结果（此方法不会返回，因为数据源无限生成数据）
     */
    public static List<Map<String, Object>> runWithRealtimeSource(HazelcastInstance hazelcastInstance,
                                                                  int targetQps) {
        // 打印配置摘要
        System.out.println(PerfConfig.getConfigSummary());
        System.out.printf("=== 高性能实时数据源配置 ===\n");
        System.out.printf("目标QPS: %d (0表示无限制)\n", targetQps);
        System.out.printf("============================\n");
        
        // 重置统计收集器
        StatsCollector.getInstance().reset();
        
        // 清空结果列表
        hazelcastInstance.getList(PerfConfig.RESULT_LIST_NAME).clear();
        
        // 构建 Pipeline
        Pipeline pipeline = Pipeline.create();
        
        // 使用高性能实时数据源
        StreamSource<EcommerceOrderBatch> source = HighPerformanceDataGenerator.createSource(targetQps);
        
        // 读取流并处理
        StreamStage<EcommerceOrderBatch> stage = pipeline.readFrom(source).withoutTimestamps();
        
        // 使用共享服务模式
        if (PerfConfig.SHARED_SERVICE_MODE) {
            stage.mapUsingService(
                    ServiceFactories.sharedService(
                            ignored -> createDuckDbOperator(),
                            closeOperator()),
                    (operator, batch) -> processBatchWithOperator(operator, batch))
            .flatMap(iterable -> Traversers.traverseIterable(iterable))
            .writeTo(Sinks.list(PerfConfig.RESULT_LIST_NAME));
        } else {
            stage.mapUsingService(
                    ServiceFactories.nonSharedService(
                            ignored -> createDuckDbOperator(),
                            closeOperator()),
                    (operator, batch) -> processBatchWithOperator(operator, batch))
            .flatMap(iterable -> Traversers.traverseIterable(iterable))
            .writeTo(Sinks.list(PerfConfig.RESULT_LIST_NAME));
        }
        
        // 提交作业但不等待完成（无限流）
        hazelcastInstance.getJet().newJob(pipeline);
        
        // 等待用户中断
        System.out.println("\n作业已启动，按 Ctrl+C 停止...");
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return null;
    }
    
    /**
     * 使用指定数据源运行作业
     */
    private static List<Map<String, Object>> runWithSource(HazelcastInstance hazelcastInstance,
                                                           Pipeline pipeline,
                                                           StreamSource<EcommerceOrderBatch> source) {
        // 读取流并处理
        StreamStage<EcommerceOrderBatch> stage = pipeline.readFrom(source).withoutTimestamps();
        
        // 根据配置选择服务模式
        if (PerfConfig.SHARED_SERVICE_MODE) {
            // 共享服务模式：所有processor共享同一个DuckDB连接
            stage.mapUsingService(
                    ServiceFactories.sharedService(
                            ignored -> createDuckDbOperator(),
                            closeOperator()),
                    (operator, batch) -> processBatchWithOperator(operator, batch))
            .flatMap(iterable -> Traversers.traverseIterable(iterable))
            .writeTo(Sinks.list(PerfConfig.RESULT_LIST_NAME));
        } else {
            // 非共享服务模式：每个processor拥有独立的DuckDB连接
            stage.mapUsingService(
                    ServiceFactories.nonSharedService(
                            ignored -> createDuckDbOperator(),
                            closeOperator()),
                    (operator, batch) -> processBatchWithOperator(operator, batch))
            .flatMap(iterable -> Traversers.traverseIterable(iterable))
            .writeTo(Sinks.list(PerfConfig.RESULT_LIST_NAME));
        }
        
        // 提交作业并等待完成
        hazelcastInstance.getJet().newJob(pipeline).join();
        
        // 打印汇总统计
        StatsCollector.getInstance().printSummary();
        
        // 返回结果
        return new ArrayList<>(hazelcastInstance.getList(PerfConfig.RESULT_LIST_NAME));
    }

    /**
     * 构建流数据源
     *
     * @param batches     批次数据列表
     * @param repeatCount 重复次数（0表示无限）
     * @return 流数据源
     */
    private static StreamSource<EcommerceOrderBatch> buildStreamingSource(List<EcommerceOrderBatch> batches,
                                                                           int repeatCount) {
        List<EcommerceOrderBatch> copy = new ArrayList<>(batches);
        return Sources.streamFromProcessor(
                "ecommerce-stream",
                ProcessorMetaSupplier.preferLocalParallelismOne(
                        (SupplierEx<Processor>) () -> new StreamBatchProcessor(copy, repeatCount)));
    }

    /**
     * 创建服务关闭函数
     */
    private static ConsumerEx<DuckDbOperator> closeOperator() {
        return operator -> {
            try {
                // 在关闭前强制刷新缓冲区，确保所有累积数据都被写入
                operator.flush();
                operator.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close DuckDbOperator", e);
            }
        };
    }

    /**
     * 根据配置创建DuckDB Operator
     * 支持三种模式：
     * - insert模式：使用PreparedStatement批量插入（兼容模式）
     * - copy模式：使用COPY命令批量写入（高性能）
     * - arrow模式：使用Arrow VectorSchemaRoot零拷贝导入（极致性能）
     */
    private static DuckDbOperator createDuckDbOperator() {
        try {
            if ("arrow".equalsIgnoreCase(PerfConfig.WRITE_MODE)) {
                System.out.println("[INFO] 使用 ARROW 模式写入 DuckDB（极致性能，零拷贝）");
                return new ArrowModeDuckDbOperator();
            } else if ("copy".equalsIgnoreCase(PerfConfig.WRITE_MODE)) {
                System.out.println("[INFO] 使用 COPY 模式写入 DuckDB（高性能）");
                return new CopyModeDuckDbOperator();
            } else {
                System.out.println("[INFO] 使用 INSERT 模式写入 DuckDB（兼容模式）");
                return new EcommerceDuckDbOperator();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DuckDbOperator", e);
        }
    }

    /**
     * 使用Operator处理批次数据
     */
    private static List<Map<String, Object>> processBatchWithOperator(DuckDbOperator operator, EcommerceOrderBatch batch) {
        try {
            return operator.processBatch(batch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process batch", e);
        }
    }

    /**
     * 流批处理器
     * 负责按间隔发射批次数据，支持重复回放
     */
    static final class StreamBatchProcessor extends AbstractProcessor {

        private final List<EcommerceOrderBatch> sourceBatches;
        private Iterator<EcommerceOrderBatch> iterator;
        private long nextEmitNanos;
        private EcommerceOrderBatch pendingBatch;
        private final long emitIntervalNanos;
        private final int repeatCount;
        private int completedLoops;

        private StreamBatchProcessor(List<EcommerceOrderBatch> sourceBatches, int repeatCount) {
            this.sourceBatches = sourceBatches;
            this.iterator = sourceBatches.iterator();
            this.emitIntervalNanos = PerfConfig.STREAM_EMIT_INTERVAL_MS * 1_000_000L;
            this.repeatCount = repeatCount;
            this.nextEmitNanos = System.nanoTime();
        }

        @Override
        public boolean isCooperative() {
            // 非协作模式：允许执行阻塞操作（如等待发射间隔）
            return false;
        }

        @Override
        public boolean complete() {
            // 确保有待发射的批次
            if (!ensurePendingBatch()) {
                return true;
            }
            
            // 控制发射速率
            long now = System.nanoTime();
            if (now < nextEmitNanos) {
                LockSupport.parkNanos(nextEmitNanos - now);
                return false;
            }
            
            // 尝试发射批次
            if (!tryEmit(pendingBatch)) {
                return false;
            }
            
            // 更新状态
            pendingBatch = null;
            nextEmitNanos = System.nanoTime() + emitIntervalNanos;
            
            // 检查是否还有数据
            if (iterator.hasNext()) {
                return false;
            }
            
            // 检查是否需要重复回放
            if (repeatCount == 0) {
                // 无限循环
                return false;
            }
            
            // 检查是否完成所有回放
            return completedLoops + 1 >= repeatCount;
        }

        /**
         * 确保有待发射的批次
         */
        private boolean ensurePendingBatch() {
            if (pendingBatch != null) {
                return true;
            }
            
            // 如果当前迭代器还有数据，获取下一个批次
            if (iterator.hasNext()) {
                pendingBatch = iterator.next();
                return true;
            }
            
            // 如果是无限循环模式，重置迭代器
            if (repeatCount == 0) {
                completedLoops++;
                iterator = sourceBatches.iterator();
                return ensurePendingBatch();
            }
            
            // 检查是否还有剩余回放次数
            if (completedLoops + 1 >= repeatCount) {
                return false;
            }
            
            // 重置迭代器进行下一次回放
            completedLoops++;
            iterator = sourceBatches.iterator();
            return ensurePendingBatch();
        }
    }
}