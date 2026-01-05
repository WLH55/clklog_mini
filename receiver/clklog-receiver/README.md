# ClkLog Receiver 模块文档

## 概述

ClkLog Receiver 是数据采集服务，负责接收来自 SDK（Web/Mobile）的用户行为数据，解析后直接写入 ClickHouse 数据库。

## 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CLKLOG RECEIVER 数据流                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SDK (Web/Mobile)                                                           │
│       │                                                                     │
│       │ HTTP POST /api/gp                                                   │
│       │ Content: application/x-www-form-urlencoded                          │
│       │ data=<Base64+GZIP>                                                  │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  KafkaController.gp()  ← HTTP 接收端点                               │   │
│  │                                                                       │   │
│  │  步骤1: extractLog()                                                 │   │
│  │   - 解析请求体 (data_list/data/crc/gzip 参数)                        │   │
│  │   - Base64 解码 + GZIP 解压                                          │   │
│  │   - 提取客户端 IP (支持代理头部)                                     │   │
│   │   - 提取 User-Agent                                                  │   │
│  │   - 记录原始日志到 /log 目录                                          │   │
│  │                                                                       │   │
│  │  步骤2: saveSensorsDataToClickHouse()                                │   │
│  │   - 解析神策 JSON 数组/对象                                           │   │
│  │   - SensorsDataMapper.mapToSensorsEvents()                           │   │
│  │   - 验证数据有效性                                                     │   │
│  │   - 批量写入 ClickHouse sensors_events 表                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LogReceiveProcessBoss (后台消费线程池 - 当前未启用)                  │   │
│  │                                                                       │   │
│  │  启动 N 个线程从 logQueue 消费数据:                                  │   │
│  │  - 从队列取出 QueryCriteria                                          │   │
│  │  - 聚合最多 1000 条                                                   │   │
│  │  - 根据 enable-simple-version 配置:                                 │   │
│  │    • true  → saveToClickHouse() (旧版 log_analysis 表)              │   │
│  │    • false → enqueueKafka() (发送到 Kafka)                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. KafkaController
**文件:** `com.zcunsoft.controller.KafkaController`

HTTP 接收入口，处理来自 SDK 的数据上报。

| 方法 | 路径 | 功能 |
|------|------|------|
| `gp()` | `/api/gp` | 接收神策数据，解析并写入 ClickHouse |

```java
@RequestMapping(value = "api/gp", method = {RequestMethod.GET, RequestMethod.POST})
public ResponseEntity<String> gp(QueryCriteria queryCriteria, HttpServletRequest request) {
    // 1. 解析请求体
    receiveService.extractLog(queryCriteria, request);
    // 2. 直接写入 ClickHouse
    receiveService.saveSensorsDataToClickHouse(queryCriteria);
    return new ResponseEntity<>(HttpStatus.OK);
}
```

### 2. ReceiveServiceImpl
**文件:** `com.zcunsoft.services.ReceiveServiceImpl`

核心服务实现，提供数据处理功能。

**主要方法:**

| 方法 | 功能 |
|------|------|
| `extractLog()` | 解析 HTTP 请求体，提取 data、IP、UA |
| `saveSensorsDataToClickHouse()` | 解析神策数据并写入 `sensors_events` 表 |
| `doSaveToSensorsEvents()` | 批量插入 ClickHouse |
| `analysisRegionFromIp()` | IP 地址解析地理位置（使用 IP2Location） |
| `analysisData()` | 解析神策数据为 `LogBean`（旧版） |
| `enqueueKafka()` | 发送数据到 Kafka（未启用） |

### 3. SensorsDataMapper
**文件:** `com.zcunsoft.util.SensorsDataMapper`

将神策 JSON 数据映射为 `SensorsEventsLogBean`。

**主要方法:**

| 方法 | 功能 |
|------|------|
| `mapToSensorsEvents()` | 将神策 JSON 映射到 `SensorsEventsLogBean` |
| `parseEventParams()` | 解析 `event_params` 中的自定义参数 |
| `isValid()` | 验证数据有效性 |

### 4. ConstsDataHolder
**文件:** `com.zcunsoft.handlers.ConstsDataHolder`

内存数据持有者，存储运行时配置。

| 字段 | 类型 | 说明 |
|------|------|------|
| `htForCity` | `ConcurrentMap` | 城市名称映射（英文→中文） |
| `logQueue` | `BlockingQueue` | 日志队列（当前未使用） |
| `htProjectSetting` | `ConcurrentMap` | 项目配置 |

### 5. LogReceiveProcessBoss
**文件:** `com.zcunsoft.daemon.LogReceiveProcessBoss`

后台消费者线程池（当前未启用）。

从 `logQueue` 消费数据，批量处理后写入 ClickHouse 或发送到 Kafka。

## 数据模型

### QueryCriteria
HTTP 请求封装对象。

```java
- data: String          // Base64+GZIP 的神策 JSON
- data_list: String     // 数据列表（与 data 二选一）
- crc: String          // 校验码
- gzip: String         // 是否 GZIP 压缩
- clientIp: String     // 客户端 IP
- ua: String          // User-Agent
```

### SensorsEventsLogBean
神策事件数据模型，对应 `sensors_events` 表。

```java
// 基本事件信息
- _track_id: Long           // 跟踪 ID
- time: Long               // 事件时间戳（毫秒）
- type: String             // 事件类型（track/profile_set）
- distinct_id: String      // 用户唯一标识
- anonymous_id: String     // 匿名用户 ID
- event: String            // 事件名称
- _flush_time: Long        // 发送时间戳

// 用户身份信息
- identity_anonymous_id: String
- identity_android_id: String

// SDK 信息
- lib_method: String       // SDK 集成方式
- lib: String              // SDK 类型
- lib_version: String      // SDK 版本
- app_version: String      // 应用版本
- lib_detail: String       // SDK 详细信息

// 自定义事件参数
- town_name: String        // 小镇名称
- town_action: String      // 小镇操作

// 神策预置属性
- is_first_day: Boolean
- os: String
- os_version: String
- manufacturer: String
- model: String
- brand: String
- screen_width: Integer
- screen_height: Integer
- timezone_offset: Integer
- app_id: String
- app_name: String
- wifi: Boolean
- network_type: String
- lib_plugin_version: List<String>
- device_id: String

// 原始 JSON
- raw_identities: String
- raw_lib: String
- raw_properties: String
- raw_event_params: String
```

## 工作流程详解

### 1. SDK 数据上报

```json
POST /api/gp
Content-Type: application/x-www-form-urlencoded

data=eyJfdHJhY2tfaWQiOi0xODIxNzY3Mjcy... (Base64)
```

### 2. 请求解析 (extractLog)

```java
// 1. 提取参数
data=dataFinal, decodedString = URLDecoder.decode(queryCriteria.getData())

// 2. Base64 解码
byte[] byteArrayNEW = Base64.getDecoder().decode(decodedString)

// 3. GZIP 解压
if (queryCriteria.getGzip().equals("1")) {
    dataFinal = GZIPUtils.uncompressToString(byteArrayNEW)
}

// 4. 提取 IP 和 UA
String ip = getIpAddr(request)  // 支持代理头部
String ua = request.getHeader("user-agent")

// 5. 记录原始日志
storeLogger.info(ip + "," + dataFinal)
```

### 3. 数据映射 (SensorsDataMapper)

```java
// 神策原始 JSON
{
  "_track_id": -1821767272,
  "time": 1767595420728,
  "type": "track",
  "distinct_id": "6df69289707d75b5",
  "event": "town_pic",
  "lib": {
    "$lib_method": "code",
    "$lib": "Android",
    "$lib_version": "6.8.4"
  },
  "properties": {
    "event_params": "[{\"key\":\"town_name\",\"value\":{\"string_value\":\"foodStreet\"}}]",
    "$os": "Android",
    "$manufacturer": "GOOGLE",
    ...
  }
}

// 映射为 SensorsEventsLogBean
bean.set_track_id(-1821767272L)
bean.setTime(1767595420728L)
bean.setEvent("town_pic")
// ... 解析所有字段
```

### 4. 写入 ClickHouse (doSaveToSensorsEvents)

```sql
INSERT INTO sensors_events (
    _track_id, time, type, distinct_id, anonymous_id, event, _flush_time,
    identity_anonymous_id, identity_android_id,
    lib_method, lib, lib_version, app_version, lib_detail,
    town_name, town_action,
    ...
) VALUES (
    ?, fromUnixTimestamp64Milli(?, 3), ?, ?, ?, ?, fromUnixTimestamp64Milli(?, 3),
    ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?,
    ...
)
```

## 配置说明

**文件:** `src/main/resources/application.yml`

```yaml
server:
  port: 8080  # 服务端口

# Redis 配置（存储城市映射、项目配置）
spring:
  redis:
    host: localhost
    port: 6379

# Kafka 配置（当前未使用）
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      topic: clklog

# ClickHouse 配置
  datasource:
    clickhouse:
      jdbc-url: jdbc:clickhouse://localhost:8123/default
      username: default
      password: clklogpwd

receiver:
  thread-count: 2                      # 后台线程数（未启用）
  project-list: firebase-analytics     # 项目列表
  resource-path: D:\coding\clklog\receiver\clklog-receiver
  enable-simple-version: false         # false=发Kafka, true=直写ClickHouse(旧表)
```

## ClickHouse 表结构

**表名:** `sensors_events`

```sql
CREATE TABLE IF NOT EXISTS sensors_events
(
    -- 基本事件信息
    _track_id Int64,
    time DateTime64(3),
    type String,
    distinct_id String,
    anonymous_id String,
    event String,
    _flush_time DateTime64(3),

    -- 用户身份信息
    identity_anonymous_id String,
    identity_android_id String,

    -- SDK 信息
    lib_method String,
    lib String,
    lib_version String,
    app_version String,
    lib_detail String,

    -- 自定义事件参数
    town_name String,
    town_action String,

    -- 神策预置属性
    is_first_day Bool,
    os String,
    os_version String,
    manufacturer String,
    model String,
    brand String,
    screen_width UInt16,
    screen_height UInt16,
    timezone_offset Int32,
    app_id String,
    app_name String,
    wifi Bool,
    network_type String,
    lib_plugin_version Array(String),
    device_id String,

    -- 原始 JSON
    raw_identities String,
    raw_lib String,
    raw_properties String,
    raw_event_params String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDate(time))
ORDER BY (toDate(time), event, distinct_id, time)
TTL toDate(time) + INTERVAL 1 YEAR;
```

## 启动与运行

### 构建

```bash
cd receiver/clklog-receiver
mvn clean package -DskipTests
```

### 运行

```bash
java -jar target/clklog-receiver-*.jar
```

### 验证

```bash
# 发送测试数据
curl -X POST http://localhost:8080/api/gp \
  -d "data=eyJfdHJhY2tfaWQiOjEyMywidGltZSI6MTY3NjU5NTQyMDcyOC..."
```

## 依赖组件

| 组件 | 版本 | 用途 |
|------|------|------|
| ClickHouse | 23.x | 数据存储 |
| Redis | 7.0 | 配置缓存 |
| Kafka | 3.4.1 | 消息队列（未启用） |
| IP2Location | - | IP 地理位置解析 |

## 注意事项

1. **当前简化架构:** 数据直接写入 ClickHouse，绕过了 Kafka 队列和后台消费线程
2. **IP 解析:** 首次查询从 IP 库获取，后续从 Redis 缓存读取
3. **批量写入:** 每批最多写入 1000 条记录
4. **日志备份:** 原始日志记录到 `/log` 目录
5. **时区:** 时间戳使用毫秒级，存储为 `DateTime64(3)`
