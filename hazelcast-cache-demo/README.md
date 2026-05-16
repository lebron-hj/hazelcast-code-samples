# hazelcast-cache-demo

基于 **Hazelcast JCache（JSR-107）** 实现强一致性读穿透 / 写穿透缓存的 Spring Boot 演示项目。

- **缓存层**：Hazelcast 嵌入式节点，通过 JCache 标准 API（`CacheLoader` / `CacheWriter`）由框架保证写顺序，无需手动补偿。
- **持久层**：默认 MongoDB，可插拔扩展为 JDBC / Fluss / RocksDB。
- **可观测**：Micrometer 指标（命中率、写延迟），通过 Spring Boot Actuator 暴露。

---

## 目录

1. [快速启动](#1-快速启动)
2. [整体架构](#2-整体架构)
3. [强一致性机制](#3-强一致性机制)
4. [REST API](#4-rest-api)
5. [配置参考](#5-配置参考)
6. [测试指南](#6-测试指南)
7. [代码结构](#7-代码结构)
8. [扩展指南](#8-扩展指南)

---

## 1 快速启动

**前置依赖**：Java 17+、Maven 3.8+、Docker

```bash
# ① 启动 MongoDB
docker run -d --name mongo-cache-demo -p 27017:27017 mongo:6.0

# ② 启动应用（在 hazelcast-cache-demo 目录下）
mvn spring-boot:run
```

应用就绪后监听 `http://localhost:8080`。

```bash
# 写入一条商品（write-through：先写 MongoDB，再写 Hazelcast 缓存）
curl -s -X POST http://localhost:8080/api/v1/products \
     -H 'Content-Type: application/json' \
     -d '{"id":"p1","name":"iPhone 16","price":7999}' | jq .

# 查询（第一次触发 read-through，之后命中缓存）
curl -s http://localhost:8080/api/v1/products/p1 | jq .

# 查看命中率指标
curl -s 'http://localhost:8080/actuator/metrics/cache.hit.count' | jq .
```

---

## 2 整体架构

```
HTTP 请求
    │
    ▼
ProductController              REST 入口：@Valid 校验、DTO ↔ Entity 转换
    │
    ▼
ProductCacheService            服务门面：二次校验、统一设置 updatedAt、委托策略
    │
    ├─── ReadThroughStrategy   读策略：containsKey 采样指标 → get 触发 read-through
    │
    └─── StrictWriteThroughStrategy   写策略：per-key JVM 锁 → 委托 JCache
                  │
                  ▼
        JCacheHazelcastProvider       CacheProvider 适配层，封装 javax.cache.Cache
                  │
                  ▼
        Hazelcast JCache（javax.cache.Cache）
                  │
          ┌───────┴────────┐
          ▼                ▼
  ProductCacheLoader  ProductCacheWriter    JCache 框架自动回调（读未命中 / 写之前）
          │                │
          └───────┬────────┘
                  ▼
          DbRepository（接口）
                  │
                  ▼
          MongoDbRepository              Spring Data MongoDB 实现
                  │
                  ▼
              MongoDB
```

### Bean 装配依赖链

```
HazelcastConfig  ──────────────────► HazelcastInstance
                                           │
RepositoryConfig ──► DbRepository          │ propertiesByInstanceItself
                          │                ▼
                          └──► JCacheConfig ──► javax.cache.Cache
                                               ──► JCacheHazelcastProvider
                                                         │
PluginConfig ────────────────────────────────────────────┘
    └──► CacheProvider Bean
              │
StrategyConfig ──► ReadStrategy / WriteStrategy
                         │
              ProductCacheService
```

> `RepositoryConfig` 从 `PluginConfig` 中拆出，是为了打断 `JCacheConfig → DbRepository ← PluginConfig` 的循环依赖。

---

## 3 强一致性机制

### 3.1 写穿透（write-through）

JCache 框架在执行 `cache.put()` / `cache.remove()` **之前**，自动调用 `ProductCacheWriter` 的回调方法，先写 DB，DB 成功后才更新缓存条目；若 DB 失败则缓存保持不变，**无需手动补偿**。

```
策略层                   JCache 框架内部                    结果
────────────────────────────────────────────────────────────────────
cacheProvider.put()  →  ① ProductCacheWriter.write()   →  DB 写成功
                            → dbRepository.insert/update    ↓
                         ② 更新缓存条目                    缓存与 DB 一致

                         若 ① 抛 CacheWriterException
                         → ② 取消                          缓存保持旧值（无需回滚）
```

### 3.2 读穿透（read-through）

`cache.get(key)` 未命中时，JCache 框架自动调用 `ProductCacheLoader.load(key)` 从 DB 加载，回填缓存后返回；调用方无感知。

```
cacheProvider.get()  →  命中   →  直接返回（记录 hit 指标）
                     →  未命中 →  ① ProductCacheLoader.load()
                                      → dbRepository.selectById()
                                  ② 结果回填缓存
                                  → 返回（记录 miss 指标）
```

### 3.3 各操作一致性对照

| 操作 | DB 写入时机 | 缓存更新时机 | DB 失败时缓存状态 |
|------|-------------|--------------|------------------|
| **INSERT** | `CacheWriter.write()` 中，缓存写入前 | DB 成功后写入 | **不变**（无需补偿） |
| **UPDATE** | `CacheWriter.write()` 中，缓存替换前 | DB 成功后替换 | **保持旧值** |
| **DELETE** | `CacheWriter.delete()` 中，缓存移除前 | DB 成功后移除 | **条目保留** |
| **GET 命中** | 不访问 DB | 直接返回缓存值 | — |
| **GET 未命中** | `CacheLoader.load()` 中 | DB 成功后回填 | **不写入** |

### 3.4 与旧版手动策略的对比

| 维度 | 旧版（IMap 手动控制） | 当前版（JCache 框架） |
|------|----------------------|----------------------|
| 写顺序保证 | 代码中 `if-else + try-catch` 手动维护 | JSR-107 框架规范强制保证 |
| DB 失败补偿 | 需手动调用 `dbRepository.delete()` 回滚 | 框架不更新缓存，无需任何补偿代码 |
| 并发 load 去重 | 策略层双重检查锁（double-checked locking） | Hazelcast 分区级内部去重 |
| 策略层 DB 依赖 | `StrictWriteThroughStrategy` 注入并直接调用 `DbRepository` | 策略层不依赖 `DbRepository`，只调用 `CacheProvider` |

---

## 4 REST API

基础路径：`/api/v1/products`

| 方法 | 路径 | 说明 | 执行顺序 |
|------|------|------|---------|
| `POST` | `/api/v1/products` | 创建商品 | DB insert → 缓存写入 |
| `PUT` | `/api/v1/products/{id}` | 更新商品 | DB update → 缓存替换 |
| `DELETE` | `/api/v1/products/{id}` | 删除商品 | DB delete → 缓存移除 |
| `GET` | `/api/v1/products/{id}` | 查询单个商品 | 命中直接返回；未命中 DB 加载后回填 |
| `GET` | `/api/v1/products?ids=a,b,c` | 批量查询 | 逐 key 触发 read-through |

### HTTP 状态码

| 状态码 | 触发条件 |
|--------|---------|
| `200 OK` | GET / PUT 成功 |
| `201 Created` | POST 成功 |
| `204 No Content` | DELETE 成功 |
| `400 Bad Request` | 字段校验失败（`BadRequestException`） |
| `404 Not Found` | 商品不存在（`EntityNotFoundException`） |
| `500 Internal Server Error` | 缓存一致性异常（`CacheConsistencyException`） |

### 请求示例

```bash
# 创建
curl -s -X POST http://localhost:8080/api/v1/products \
     -H 'Content-Type: application/json' \
     -d '{"id":"p1","name":"iPhone 16","price":7999}' | jq .

# 查询
curl -s http://localhost:8080/api/v1/products/p1 | jq .

# 更新
curl -s -X PUT http://localhost:8080/api/v1/products/p1 \
     -H 'Content-Type: application/json' \
     -d '{"id":"p1","name":"iPhone 16 Pro","price":8999}' | jq .

# 删除
curl -s -X DELETE http://localhost:8080/api/v1/products/p1

# 批量查询
curl -s 'http://localhost:8080/api/v1/products?ids=p1,p2,p3' | jq .

# 指标查询
curl -s 'http://localhost:8080/actuator/metrics/cache.hit.count' | jq .
curl -s 'http://localhost:8080/actuator/metrics/cache.write.duration' | jq .
```

---

## 5 配置参考

### 完整配置（application.yml）

```yaml
cache-framework:
  cache:
    enabled: true            # 总开关；false 时所有操作直连 DB（降级）
    provider: hazelcast      # 缓存提供者：hazelcast（默认）| rocksdb | redis
    name: products           # Hazelcast IMap / JCache Cache 名称
    ttl-seconds: 300         # 条目 TTL（秒），0 表示永不过期
    max-size: 10000          # 每节点最大条目数，超出按 LRU 淘汰

  hazelcast:
    cluster-name: cache-demo-cluster  # 集群名称，同名节点才能相互发现
    backup-count: 1                   # 数据副本数（单节点时无实际效果）
    port: 5701                        # 节点监听端口
    auto-increment-port: true         # 端口冲突时自动递增
    multicast-enabled: false          # 关闭 multicast，避免意外组网
    members: []                       # tcp-ip 成员列表（非空时启用点对点组网）

  database:
    repository: mongo        # DB 仓库类型：mongo（默认）| jdbc | fluss

  strategy:
    mode: READ_WRITE_THROUGH    # 策略模式，见下表
    read-through-enabled: true  # 是否启用读穿透
    write-through-enabled: true # 是否启用写穿透

spring:
  data:
    mongodb:
      uri: mongodb://localhost:27017/cachedb
```

### 策略模式

| `strategy.mode` | 读路径 | 写路径 | 适用场景 |
|-----------------|--------|--------|----------|
| `READ_WRITE_THROUGH`（默认） | JCache read-through | JCache write-through | 生产强一致性 |
| `READ_THROUGH_ONLY` | JCache read-through | 直连 DB | 写多读少，缓存仅加速查询 |
| `WRITE_THROUGH_ONLY` | 直连 DB | JCache write-through | 不容忍读到缓存旧值 |
| `DIRECT_DB` | 直连 DB | 直连 DB | 缓存完全禁用 / 降级 |

### Micrometer 指标

| 指标名 | 类型 | 标签 | 含义 |
|--------|------|------|------|
| `cache.hit.count` | Counter | `map`、`provider` | 缓存命中次数 |
| `cache.miss.count` | Counter | `map`、`provider` | 缓存未命中次数 |
| `cache.write.duration` | Timer | `operation`、`provider` | write-through 端到端延迟（含 DB 写入） |
| `storage.read.duration` | Timer | `repository` | 存储层读取延迟（降级路径） |
| `storage.write.duration` | Timer | `operation`、`repository` | 存储层写入延迟（降级路径） |

---

## 6 测试指南

### 测试分层

| 测试类 | 类型 | 验证重点 |
|--------|------|---------|
| `ProductCacheLoaderTest` | 单元 | `load()` 从 DB 加载、`null` 过滤、`CacheLoaderException` 包装 |
| `ProductCacheWriterTest` | 单元 | `write()` 的 insert/update 分支判断、`iterator.remove()` 协议、DB 失败时抛 `CacheWriterException` |
| `StrictWriteThroughStrategyTest` | 单元 | 策略层只调用 `cacheProvider.put/delete`，不直接调用 DB；异常包装为 `CacheConsistencyException` |
| `ReadThroughStrategyTest` | 单元 | 命中/未命中指标采集、缓存禁用降级到 DB |
| `ProductControllerTest` | 单元（MockMvc） | REST 端点、HTTP 状态码 |
| `MongoStorageBackendIT` | 集成（Testcontainers） | 在真实 MongoDB 容器中验证完整 CRUD 链路 |

### 运行命令

```bash
# 仅单元测试（无需 Docker）
mvn test

# 单元 + 集成测试（需要 Docker 运行 MongoDB 容器）
mvn verify

# 运行指定测试类
mvn -Dtest=ProductCacheWriterTest,StrictWriteThroughStrategyTest test

# 运行单个测试方法
mvn -Dtest=ProductCacheWriterTest#write_shouldCallDbInsertWhenKeyIsNew test
```

### 强一致性验证清单

- [x] `ProductCacheWriterTest#write_shouldCallDbInsertWhenKeyIsNew` — DB insert 在缓存写入前执行
- [x] `ProductCacheWriterTest#write_shouldCallDbUpdateWhenKeyExists` — DB update 在缓存替换前执行
- [x] `ProductCacheWriterTest#write_shouldWrapDbExceptionAsCacheWriterException` — DB 失败阻止缓存更新
- [x] `ProductCacheWriterTest#delete_shouldCallDbDelete` — DB delete 在缓存移除前执行
- [x] `ProductCacheWriterTest#delete_shouldWrapDbExceptionAsCacheWriterException` — DB 失败阻止缓存移除
- [x] `StrictWriteThroughStrategyTest#jcache_insertShouldDelegateToCacheProviderPut` — 策略层不直接调用 DB
- [x] `StrictWriteThroughStrategyTest#jcache_insertShouldWrapExceptionAsCacheConsistencyException` — 异常正确包装
- [x] `ReadThroughStrategyTest#shouldRecordHitAndReturnCachedValueOnCacheHit` — 命中指标正确
- [x] `ReadThroughStrategyTest#shouldRecordMissWhenJCacheLoaderBackfillsFromDb` — 未命中指标正确

---

## 7 代码结构

```
src/main/java/com/xxx/cache/
│
├── entity/
│   └── Product.java                      领域实体；实现 Serializable 供 Hazelcast 序列化
│
├── core/
│   ├── provider/
│   │   ├── CacheProvider.java            缓存操作抽象接口（put/get/delete/containsKey）
│   │   ├── impl/
│   │   │   ├── JCacheHazelcastProvider.java   ★ 生产默认：封装 javax.cache.Cache
│   │   │   └── HazelcastCacheProvider.java    直接操作 IMap，不带 write-through
│   │   └── extensions/
│   │       ├── RocksDbProvider.java      扩展占位
│   │       └── RedisProvider.java        扩展占位
│   │
│   ├── repository/
│   │   ├── DbRepository.java             持久层抽象接口（insert/update/delete/select）
│   │   ├── impl/
│   │   │   ├── MongoDbRepository.java    ★ 生产默认：Spring Data MongoDB 实现
│   │   │   ├── MongoProductDocument.java MongoDB 文档对象（隔离领域层与 @Document）
│   │   │   └── SpringMongoProductRepository.java  MongoRepository 接口
│   │   └── extensions/
│   │       ├── JdbcRepository.java       扩展占位
│   │       └── FlussRepository.java      扩展占位
│   │
│   ├── jcache/                           ★ 强一致性核心（JCache 框架回调）
│   │   ├── ProductCacheLoader.java       read-through 回调：未命中时从 DB 加载并回填
│   │   ├── ProductCacheWriter.java       write-through 回调：写 DB 后框架才更新缓存
│   │   ├── ProductCacheLoaderFactory.java
│   │   └── ProductCacheWriterFactory.java
│   │
│   └── strategy/
│       ├── ReadStrategy.java             读策略接口
│       ├── WriteStrategy.java            写策略接口
│       ├── StrategyMode.java             策略模式枚举
│       └── impl/
│           ├── ReadThroughStrategy.java          ★ containsKey 采样 + JCache read-through
│           ├── StrictWriteThroughStrategy.java   ★ per-key 锁 + 委托 JCache write-through
│           ├── DirectDbReadStrategy.java         降级：绕过缓存直连 DB
│           └── DirectDbWriteStrategy.java        降级：绕过缓存直连 DB
│
├── service/
│   ├── CacheService.java                 服务门面接口
│   └── ProductCacheService.java          实现：校验 + 设置时间戳 + 委托策略
│
├── controller/
│   ├── ProductController.java            REST 控制器：/api/v1/products
│   └── GlobalExceptionHandler.java       统一异常 → HTTP 状态码映射
│
├── metrics/
│   └── CacheMetricsRecorder.java         Micrometer 封装：命中计数 + 写延迟计时
│
├── util/
│   └── KeyLockManager.java               per-key ReentrantLock，防并发写冲突
│
├── exception/
│   ├── CacheConsistencyException.java    write-through 失败（DB 写入失败）
│   ├── EntityNotFoundException.java      记录不存在 → HTTP 404
│   └── BadRequestException.java          参数非法 → HTTP 400
│
└── config/
    ├── CoreConfig.java                   @EnableConfigurationProperties
    ├── CacheFrameworkProperties.java     配置属性类（prefix: cache-framework）
    ├── HazelcastConfig.java              创建 HazelcastInstance，配置网络 / IMap
    ├── JCacheConfig.java                 ★ 创建 javax.cache.Cache，绑定 Loader/Writer
    ├── RepositoryConfig.java             DbRepository Bean（从 PluginConfig 拆出以避免循环依赖）
    ├── PluginConfig.java                 CacheProvider Bean（根据配置选择实现）
    └── StrategyConfig.java               Read/Write 策略 Bean（根据 mode 选择实现）
```

---

## 8 扩展指南

### 新增缓存提供者（以 Redis 为例）

1. 在 `core/provider/extensions/` 下新建 `RedisProvider` 实现 `CacheProvider<String, Product>`（含 `containsKey`）。
2. 在 `PluginConfig.cacheProvider()` 中添加分支：`"redis".equalsIgnoreCase(provider)`。
3. 在 `CacheFrameworkProperties` 中添加 Redis 连接属性。

### 新增存储后端（以 JDBC 为例）

1. 在 `core/repository/extensions/` 下实现 `JdbcRepository` 的所有方法。
2. 在 `RepositoryConfig.dbRepository()` 中添加分支：`"jdbc".equalsIgnoreCase(repository)`。
3. `update()` / `delete()` 在 key 不存在时**必须**抛 `EntityNotFoundException`，使 `ProductCacheWriter` 将其包装为 `CacheWriterException`，从而阻止缓存更新。

### 切换到多节点 Hazelcast 集群

```yaml
cache-framework:
  hazelcast:
    multicast-enabled: false
    members:
      - 192.168.1.10:5701
      - 192.168.1.11:5701
```

> **注意**：多节点场景下，`ProductCacheLoaderFactory` / `ProductCacheWriterFactory` 中标注 `transient` 的 `DbRepository` 字段需改为可序列化的代理（例如通过 Hazelcast `UserContext` 在各节点持有同一 `DbRepository` 引用），否则跨节点反序列化时工厂字段将为 `null`。
