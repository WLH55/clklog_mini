# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在此代码仓库中工作时提供指导。

## 项目概述

ClkLog 是一个开源的用户行为分析平台的数据采集与处理核心模块，用于记录和分析用户在网页、移动应用和微信小程序上的交互行为。

**当前架构：** 数据采集与流处理管道

**核心数据流：**
```
Web/Mobile SDKs → Receiver 服务 → Redis/Kafka → Simple Consumer → ClickHouse
```

> **注意：** 本仓库仅包含数据采集和处理核心模块，移除了 API 层、前端 UI、数据库初始化和系统配置管理服务。

## 模块结构

项目包含以下核心模块：

- **receiver/clklog-receiver** - 数据采集服务，从 SDK 收集日志并转发到 Kafka
- **processing/clklog-processing** - Apache Flink 流处理任务（可选，可用 simple-consumer 替代）
- **simple-consumer/** - 简单的 Kafka 消费者，用于替代 Flink 进行数据消费和处理
- **clklog_doc/** - 部署配置和文档

## 构建命令

### 后端服务 (Java/Maven)

每个 Java 模块使用 Maven 独立构建：

```bash
# 构建 receiver 模块
cd receiver/clklog-receiver
mvn clean package -DskipTests

# 构建 processing 模块（Flink，可选）
cd processing/clklog-processing
mvn clean package -DskipTests

# 构建 simple-consumer 模块
cd simple-consumer
mvn clean package -DskipTests
```

**Java 版本：** JDK 8
**Kafka 版本：** 3.4.1

## 部署

系统使用 Docker Compose 进行部署：

```bash
cd clklog_doc

# 完整部署（包含 Flink）
docker-compose -f docker-compose-clklog-full.yml up -d

# 简单部署（不包含 Flink）
docker-compose -f docker-compose-clklog-simple.yml up -d

# 仅基础基础设施
docker-compose -f docker-compose-clklog-full-base.yml up -d

# 最小化部署（推荐用于 simple-consumer）
docker-compose -f docker-compose-clklog-minimal.yml up -d
```

**必需的环境变量**（在 .env 或 shell 中设置）：
- `CLKLOG_LOG_DB` - ClickHouse 数据库名称
- `CK_USER_NAME` - ClickHouse 用户名
- `CK_USER_PWD` - ClickHouse 密码
- `MYSQL_ROOT_PASSWORD` - MySQL root 密码
- `PROJECT_NAME` - 项目标识符
- `PROCESSING_JOB_ID` - Flink 任务的唯一 ID（仅 Flink 模式需要）

**Simple Consumer 额外环境变量：**
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka 服务器地址（默认：localhost:9092）
- `KAFKA_TOPIC` - 消费的主题名称（默认：clklog）
- `KAFKA_GROUP_ID` - 消费者组 ID（默认：simple-consumer-group）

## 核心技术

**后端：**
- Apache Kafka 3.4.1（消息队列）
- Apache Flink 1.16.1（流处理 - 可选）
- ClickHouse JDBC 0.4.6（分析数据库）
- Redis 7.0（缓存和队列）
- Jackson 2.13.4（JSON 处理）

## 数据处理架构

### Simple Consumer (simple-consumer/)

**入口类：** `com.zcunsoft.clklog.simple.consumer.SimpleKafkaConsumer`

**关键特性：**
- 从 Kafka 主题 "clklog" 消费数据
- 轻量级替代方案，避免 Flink 的复杂部署
- 支持通过环境变量配置 Kafka 连接参数
- 解析 JSON 消息并提取 event 和 distinct_id 字段
- 可扩展添加数据处理和 ClickHouse 写入逻辑

**优势：**
- 部署简单，无需 Flink 集群
- 资源占用低
- 易于定制和调试

### Flink 处理 (processing/clklog-processing) - 可选

**入口：** `com.zcunsoft.clklog.analysis.entry.JieXiJson`

**关键特性：**
- 从 Kafka 主题 "clklog" 消费数据
- 10 秒处理窗口后批量写入 ClickHouse
- 使用 IP 地理位置和设备分析丰富数据
- 启用检查点以保证容错性
- 并行度：2（可配置）

> **架构建议：** 参考文档（Flink使用说明与原理详解.md），对于简单的 ETL 场景（消费 → 丰富 → 写入），使用 simple-consumer 是更合适的选择，可以避免 Flink 的过度设计问题。

### 数据采集 (receiver/clklog-receiver)

- 接收来自 Web/Mobile SDK 的 JSON 日志
- 解析 User-Agent 进行设备检测
- 执行 IP 地理位置查询
- 在发送到 Kafka 之前将数据缓冲到 Redis
- 将原始日志写入 `/log` 目录进行备份

## 配置文件

**后端配置：**
- Receiver 使用 `application.yml` 或 `application.properties`
- Flink 配置：`clklog_dc_config/processing/config.properties`
- Simple Consumer 通过环境变量配置

**部署配置：**
- 网关：`clklog_dc_config/gateway/full/`
- Docker Compose 配置：`clklog_doc/docker-compose-*.yml`

## 开发说明

### SDK 集成

系统基于神策分析 SDK。SDK 数据格式决定了 ClickHouse 中的 schema。修改数据结构时：
1. 查看 SDK 文档了解数据格式约束
2. 验证 ClickHouse schema 的向后兼容性
3. 更新 receiver 和 consumer 中的解析逻辑

### ClickHouse Schema

- 分析数据存储在时间序列优化的表中
- 数据库名称：`clklog`（可通过 `CLKLOG_LOG_DB` 配置）
- Schema 定义需要在 ClickHouse 中预先创建
- 使用批量插入以提高性能

### Simple Consumer 开发

当前实现仅消费和打印日志，扩展功能时需要：
1. 添加 ClickHouse JDBC 连接和写入逻辑
2. 实现数据丰富（IP 地理位置查询、设备分析等）
3. 添加批量处理和错误处理机制
4. 考虑添加监控和健康检查

## 重要架构决策

1. **简化架构：** 移除了 API 层、前端 UI 等模块，聚焦于数据采集和处理核心功能
2. **替代方案：** 提供 simple-consumer 作为 Flink 的轻量级替代方案
3. **消息队列：** 所有日志数据通过 Kafka 流转以实现解耦
4. **无状态服务：** Receiver 服务是无状态的；所有状态存储在数据库或 Redis 中
5. **Docker 原生：** 设计为完全在 Docker 中运行，支持外部配置挂载

## 许可证合规

**许可证：** AGPLv3.0

**关键要求：**
- 任何修改必须开源
- 使用此代码的网络服务必须向接收者提供源代码
- 商业集成需要商业许可证或符合 AGPLv3.0

贡献代码时不要删除或修改许可证头。
