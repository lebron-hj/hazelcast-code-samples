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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class DuckDbMongoOfficialDemo {
    public static void main(String[] args) {
        // 官方推荐连接参数：设置内存限制，避免 OOM
        String duckDbUrl = "jdbc:duckdb:memory:";

        try (Connection conn = DriverManager.getConnection(duckDbUrl);
             Statement stmt = conn.createStatement()) {

            // 步骤1：查询扩展支持的版本/来源
            printExtensionInfo(stmt, "mongo");

            // 步骤2：安装 mongo 扩展（仅首次执行，会自动下载）
            stmt.execute("INSTALL mongo FROM 'http://community-extensions.duckdb.org';");

            // 步骤3：加载 mongo 扩展（每次连接必须执行）
            stmt.execute("LOAD mongo;");

            // 步骤4：ATTACH MongoDB 服务（官方核心语法）
            // 支持标准 MongoDB 连接串格式，包括认证和 Atlas 集群
//            String mongoConnStr = "mongodb://admin:123456@localhost:27017"; // 官方示例格式
            String mongoConnStr = "mongodb://localhost:27017/tapdata-develop"; // 官方示例格式
            String attachSql = String.format("ATTACH '%s' AS mongo_db (TYPE MONGO);", mongoConnStr);
            stmt.execute(attachSql); // 官方推荐方式

            String databaseName = "tapdata-develop";
            String collectionName = "students";
            String mongoTable = "mongo_db.\"" + databaseName + "\"." + collectionName;

            // 步骤5：验证 DuckDB 外部表 SQL 支持范围
            boolean createSupported = tryExecute(conn, "CREATE TABLE IF NOT EXISTS " + mongoTable
                    + " (student_id INTEGER, name VARCHAR, age INTEGER, grade VARCHAR)", "CREATE TABLE");
            boolean insertSupported = tryExecute(conn, "INSERT INTO " + mongoTable
                    + " (student_id, name, age, grade) VALUES (1, 'Alice', 18, 'A')", "INSERT");
            boolean updateSupported = tryExecute(conn, "UPDATE " + mongoTable
                    + " SET age = 21, grade = 'A+' WHERE student_id = 1", "UPDATE");
            boolean deleteSupported = tryExecute(conn, "DELETE FROM " + mongoTable + " WHERE student_id = 1", "DELETE");

            if (!createSupported || !insertSupported || !updateSupported || !deleteSupported) {
                System.out.println("提示: DuckDB MongoDB 外部表不支持部分 DDL/DML，改用 MongoDB 原生 Java API 执行。");
            }

            // 步骤6：MongoDB 外部表全链路（建表/插入/修改/删除/查询/加速/删除表）
            if (!createSupported || !insertSupported || !updateSupported || !deleteSupported) {
                runMongoNativeFlow(mongoConnStr, databaseName, collectionName);
                executeAndPrint(conn, "SELECT * FROM mongo_clear_cache();");
//                executeAndPrint(conn, "DETACH mongo_db");
//                stmt.execute(attachSql);
            } else {
                executeAndPrint(conn, "INSERT INTO " + mongoTable
                        + " (student_id, name, age, grade) VALUES "
                        + "(1, 'Alice', 18, 'A'), (2, 'Bob', 19, 'B'), (3, 'Cindy', 20, 'A')");
                executeAndPrint(conn, "UPDATE " + mongoTable + " SET age = 21, grade = 'A+' WHERE student_id = 3");
                executeAndPrint(conn, "DELETE FROM " + mongoTable + " WHERE student_id = 2");
            }

            executeAndPrint(conn, "SELECT student_id, name, age, grade FROM " + mongoTable + " ORDER BY student_id");

            // 步骤7：加速查询（导入本地表 + 索引）
            String cacheSql = """
                CREATE TABLE IF NOT EXISTS local_students AS
                SELECT student_id, name, age, grade FROM %s
            """.formatted(mongoTable);
            executeAndPrint(conn, cacheSql);
            executeAndPrint(conn, "CREATE INDEX IF NOT EXISTS idx_local_students_id ON local_students(student_id)");
            executeAndPrint(conn, "SELECT student_id, name, age, grade FROM local_students ORDER BY student_id");

            // 步骤8：清理缓存与外部表
            executeAndPrint(conn, "DROP TABLE IF EXISTS local_students");

            // 步骤9：（可选）清除 MongoDB schema 缓存（集合结构变更时执行）
            executeAndPrint(conn, "SELECT * FROM mongo_clear_cache();");

            // 步骤10：程序末尾清理 MongoDB 集合
            dropMongoCollection(mongoConnStr, databaseName, collectionName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printExtensionInfo(Statement stmt, String extensionName) throws Exception {
        String sql = "SELECT extension_name, extension_version, installed_from, install_mode "
                + "FROM duckdb_extensions()";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString("extension_name");
                if (!extensionName.equalsIgnoreCase(name)) {
                    continue;
                }
                System.out.printf("扩展: %s | 版本: %s | 来源: %s | 模式: %s%n",
                        name,
                        rs.getString("extension_version"),
                        rs.getString("installed_from"),
                        rs.getString("install_mode"));
            }
        }
    }

    private static void executeAndPrint(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            boolean hasResultSet = stmt.execute(sql);
            System.out.println("执行SQL: " + sql.replace("\n", " ").trim());
            if (!hasResultSet) {
                System.out.println("影响行数: " + stmt.getUpdateCount());
                return;
            }
            try (ResultSet rs = stmt.getResultSet()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) {
                            row.append(" | ");
                        }
                        row.append(rs.getMetaData().getColumnLabel(i)).append("=").append(rs.getString(i));
                    }
                    System.out.println(row);
                }
            }
        }
    }

    private static boolean tryExecute(Connection conn, String sql, String label) {
        try {
            executeAndPrint(conn, sql);
            return true;
        } catch (Exception e) {
            System.out.println("DuckDB 外部表暂不支持: " + label + " | " + e.getMessage());
            return false;
        }
    }

    private static void runMongoNativeFlow(String mongoConnStr, String databaseName, String collectionName) {
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase database = client.getDatabase(databaseName);
            if (!collectionExists(database, collectionName)) {
                database.createCollection(collectionName);
            }
            System.out.println("使用 MongoDB 原生 Java API 创建Collection 成功.");
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.deleteMany(new Document());
            collection.insertMany(java.util.List.of(
                    new Document("student_id", 1).append("name", "Alice").append("age", 18).append("grade", "A"),
                    new Document("student_id", 2).append("name", "Bob").append("age", 19).append("grade", "B"),
                    new Document("student_id", 3).append("name", "Cindy").append("age", 20).append("grade", "A")
            ));
            System.out.println("使用 MongoDB 原生 Java API inert 成功.");
            collection.updateOne(Filters.eq("student_id", 3), Updates.combine(
                    Updates.set("age", 21),
                    Updates.set("grade", "A+")
            ));
            System.out.println("使用 MongoDB 原生 Java API update 成功.");
            collection.deleteOne(Filters.eq("student_id", 2));
            System.out.println("使用 MongoDB 原生 Java API delete 成功.");
        } catch (Exception e) {
            System.out.println("MongoDB 原生操作失败: " + e.getMessage());
        }
    }

    private static boolean collectionExists(MongoDatabase database, String collectionName) {
        for (String name : database.listCollectionNames()) {
            if (collectionName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void dropMongoCollection(String mongoConnStr, String databaseName, String collectionName) {
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase database = client.getDatabase(databaseName);
            if (!collectionExists(database, collectionName)) {
                System.out.println("MongoDB 集合不存在，无需清理: " + databaseName + "." + collectionName);
                return;
            }
            database.getCollection(collectionName).drop();
            System.out.println("MongoDB 集合已清理: " + databaseName + "." + collectionName);
        } catch (Exception e) {
            System.out.println("MongoDB 集合清理失败: " + e.getMessage());
        }
    }
}