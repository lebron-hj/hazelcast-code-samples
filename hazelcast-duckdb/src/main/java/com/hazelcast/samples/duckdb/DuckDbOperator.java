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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * DuckDB操作器接口
 * 定义了处理批次数据的标准方法
 */
public interface DuckDbOperator {
    
    /**
     * 处理单个批次数据
     * 
     * @param batch 订单批次数据
     * @return 查询结果列表
     * @throws SQLException SQL异常
     */
    List<Map<String, Object>> processBatch(EcommerceOrderBatch batch) throws SQLException;
    
    /**
     * 强制刷新所有缓存的数据
     * 
     * @return 刷新后累积的宽表查询结果
     * @throws SQLException SQL异常
     */
    List<Map<String, Object>> flush() throws SQLException;
    
    /**
     * 关闭资源
     */
    void close() throws Exception;
}