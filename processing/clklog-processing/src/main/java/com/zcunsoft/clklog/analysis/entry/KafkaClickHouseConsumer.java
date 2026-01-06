package com.zcunsoft.clklog.analysis.entry;

import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBean;
import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBeanCollection;
import com.zcunsoft.clklog.analysis.enricher.LogEnricher;
import com.zcunsoft.clklog.analysis.utils.ClickHouseUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 到 ClickHouse 消费者
 * <p>
 * 功能：
 * 1. 从 Kafka 消费日志数据
 * 2. 解析为 SensorsEventsLogBean
 * 3. 批量写入 ClickHouse sensors_events 表（混合模式：1000 条或 10 秒）
 * 4. 手动提交 Kafka offset
 */
public class KafkaClickHouseConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClickHouseConsumer.class);

    private final Properties config;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int pollTimeoutMs;
    private final int maxPollRecords;

    private KafkaConsumer<String, String> consumer;
    private Connection clickhouseConn;
    private PreparedStatement clickhousePst;
    private LogEnricher logEnricher;

    // 批次缓冲区
    private final List<SensorsEventsLogBean> batchBuffer = new ArrayList<>();

    // 时间跟踪
    private long lastFlushTime = System.currentTimeMillis();

    // 统计信息
    private final AtomicLong totalConsumed = new AtomicLong(0);
    private final AtomicLong totalWritten = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // 关闭标志
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * ClickHouse 插入 SQL - sensors_events 表
     */
    private static final String INSERT_SQL = "INSERT INTO sensors_events (" +
            "_track_id, time, type, distinct_id, anonymous_id, event, _flush_time, " +
            "identity_anonymous_id, identity_android_id, " +
            "lib_method, lib, lib_version, app_version, lib_detail, " +
            "town_name, town_action, " +
            "is_first_day, os, os_version, manufacturer, model, brand, " +
            "screen_width, screen_height, timezone_offset, app_id, app_name, " +
            "wifi, network_type, lib_plugin_version, device_id, " +
            "raw_identities, raw_lib, raw_properties, raw_event_params" +
            ") VALUES (" +
            "?, fromUnixTimestamp64Milli(?, 3), ?, ?, ?, ?, fromUnixTimestamp64Milli(?, 3), " +
            "?, ?, " +
            "?, ?, ?, ?, ?, " +
            "?, ?, " +
            "?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, " +
            "?, ?, ?, ?)";

    /**
     * 构造函数
     *
     * @param configFile 配置文件路径
     */
    public KafkaClickHouseConsumer(String configFile) {
        this.config = loadConfig(configFile);

        // 读取配置参数
        this.batchSize = Integer.parseInt(config.getProperty("consumer.batch.size", "1000"));
        this.batchIntervalMs = Long.parseLong(config.getProperty("consumer.batch.interval.ms", "10000"));
        this.pollTimeoutMs = Integer.parseInt(config.getProperty("consumer.poll.timeout.ms", "1000"));
        this.maxPollRecords = Integer.parseInt(config.getProperty("consumer.max.poll.records", "500"));

        logger.info("KafkaClickHouseConsumer initialized with batch size: {}, interval: {}ms",
                batchSize, batchIntervalMs);
    }

    /**
     * 加载配置文件
     * 优先从 classpath 加载，失败则从文件路径加载
     */
    private Properties loadConfig(String configFile) {
        Properties props = new Properties();

        // 1. 先尝试从 classpath 加载
        try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (is != null) {
                props.load(is);
                logger.info("Config loaded from classpath: config.properties");
                return props;
            }
        } catch (Exception e) {
            logger.debug("Failed to load config from classpath, trying file path: {}", configFile);
        }

        // 2. classpath 加载失败，从文件路径加载
        try (java.io.FileInputStream fis = new java.io.FileInputStream(configFile)) {
            props.load(fis);
            logger.info("Config loaded from file: {}", configFile);
        } catch (Exception e) {
            logger.error("Failed to load config file: {}", configFile, e);
            throw new RuntimeException("Failed to load config file. Please ensure config.properties exists in classpath or at: " + configFile, e);
        }
        return props;
    }

    /**
     * 初始化组件
     */
    public void initialize() throws SQLException {
        // 初始化 LogEnricher
        logEnricher = new LogEnricher();

        // 初始化 ClickHouse 连接
        String clickhouseHost = config.getProperty("clickhouse.host");
        String clickhouseDb = config.getProperty("clickhouse.database");
        String clickhouseUsername = config.getProperty("clickhouse.username");
        String clickhousePwd = config.getProperty("clickhouse.password");
        clickhouseConn = ClickHouseUtil.getConn(clickhouseHost, clickhouseDb, clickhouseUsername, clickhousePwd);
        clickhousePst = clickhouseConn.prepareStatement(INSERT_SQL);
        logger.info("ClickHouse connection established");

        // 初始化 Kafka Consumer
        consumer = createKafkaConsumer();
        String topic = config.getProperty("kafka.clklog-topic", "clklog");
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("Kafka consumer subscribed to topic: {}", topic);
    }

    /**
     * 创建 Kafka Consumer
     */
    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();

        // 基础配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.server"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("kafka.clklog-group-id"));

        // 手动提交 offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 序列化配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 从最早的消息开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 批量拉取配置
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        // 心跳配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");

        return new KafkaConsumer<>(props);
    }

    /**
     * 启动消费循环
     */
    public void run() {
        logger.info("Starting Kafka consumer loop...");

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    // 检查是否需要定时刷新
                    checkAndFlush();
                    continue;
                }

                // 处理记录
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processRecord(record.value());
                        totalConsumed.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Error processing record: {}", record.value(), e);
                        totalErrors.incrementAndGet();
                    }
                }

                // 检查是否需要刷新
                checkAndFlush();

                // 手动提交 offset
                consumer.commitSync();
            }

            // 退出前刷新剩余数据
            flush();

        } catch (Exception e) {
            logger.error("Error in consumer loop", e);
            throw new RuntimeException("Error in consumer loop", e);
        } finally {
            shutdown();
        }
    }

    /**
     * 处理单条记录
     */
    private void processRecord(String record) {
        SensorsEventsLogBeanCollection collection = logEnricher.enrich(record);
        List<SensorsEventsLogBean> beans = collection.getData();

        if (beans != null && !beans.isEmpty()) {
            batchBuffer.addAll(beans);
        }
    }

    /**
     * 检查并刷新批次
     */
    private void checkAndFlush() {
        long currentTime = System.currentTimeMillis();
        long timeSinceLastFlush = currentTime - lastFlushTime;

        // 条件1: 缓冲区大小达到批次大小
        boolean sizeReached = batchBuffer.size() >= batchSize;

        // 条件2: 时间间隔达到批次间隔
        boolean timeReached = timeSinceLastFlush >= batchIntervalMs;

        if (sizeReached || timeReached) {
            if (!batchBuffer.isEmpty()) {
                flush();
            }
            lastFlushTime = currentTime;
        }
    }

    /**
     * 刷新批次到 ClickHouse
     */
    private void flush() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        try {
            // 批量插入
            for (SensorsEventsLogBean bean : batchBuffer) {
                setPreparedStatementParameters(clickhousePst, bean);
                clickhousePst.addBatch();
            }

            // 执行批量插入
            int[] results = clickhousePst.executeBatch();

            // 清空缓冲区
            int writtenCount = batchBuffer.size();
            batchBuffer.clear();
            totalWritten.addAndGet(writtenCount);

            logger.debug("Flushed {} records to ClickHouse", writtenCount);

        } catch (Exception e) {
            logger.error("Error flushing batch to ClickHouse", e);
            totalErrors.addAndGet(batchBuffer.size());
            batchBuffer.clear();
        }
    }

    /**
     * 设置 PreparedStatement 参数
     */
    private void setPreparedStatementParameters(PreparedStatement pst, SensorsEventsLogBean bean) throws SQLException {
        // _track_id, time, type, distinct_id, anonymous_id, event, _flush_time
        pst.setObject(1, bean.get_track_id());
        pst.setObject(2, bean.getTime());
        pst.setString(3, bean.getType());
        pst.setString(4, bean.getDistinct_id());
        pst.setString(5, bean.getAnonymous_id());
        pst.setString(6, bean.getEvent());
        pst.setObject(7, bean.get_flush_time());

        // identity_anonymous_id, identity_android_id
        pst.setString(8, bean.getIdentity_anonymous_id());
        pst.setString(9, bean.getIdentity_android_id());

        // lib_method, lib, lib_version, app_version, lib_detail
        pst.setString(10, bean.getLib_method());
        pst.setString(11, bean.getLib());
        pst.setString(12, bean.getLib_version());
        pst.setString(13, bean.getApp_version());
        pst.setString(14, bean.getLib_detail());

        // town_name, town_action
        pst.setString(15, bean.getTown_name());
        pst.setString(16, bean.getTown_action());

        // is_first_day, os, os_version, manufacturer, model, brand
        pst.setObject(17, bean.getIs_first_day());
        pst.setString(18, bean.getOs());
        pst.setString(19, bean.getOs_version());
        pst.setString(20, bean.getManufacturer());
        pst.setString(21, bean.getModel());
        pst.setString(22, bean.getBrand());

        // screen_width, screen_height, timezone_offset, app_id, app_name
        pst.setObject(23, bean.getScreen_width());
        pst.setObject(24, bean.getScreen_height());
        pst.setObject(25, bean.getTimezone_offset());
        pst.setString(26, bean.getApp_id());
        pst.setString(27, bean.getApp_name());

        // wifi, network_type, lib_plugin_version, device_id
        pst.setObject(28, bean.getWifi());
        pst.setString(29, bean.getNetwork_type());
        pst.setString(30, bean.getLib_plugin_version() != null ? String.join(",", bean.getLib_plugin_version()) : null);
        pst.setString(31, bean.getDevice_id());

        // raw_identities, raw_lib, raw_properties, raw_event_params
        pst.setString(32, bean.getRaw_identities());
        pst.setString(33, bean.getRaw_lib());
        pst.setString(34, bean.getRaw_properties());
        pst.setString(35, bean.getRaw_event_params());
    }

    /**
     * 关闭消费者
     */
    public void shutdown() {
        logger.info("Shutting down KafkaClickHouseConsumer...");

        running.set(false);

        // 打印统计信息
        logger.info("Statistics - Consumed: {}, Written: {}, Errors: {}",
                totalConsumed.get(), totalWritten.get(), totalErrors.get());

        // 关闭 LogEnricher
        if (logEnricher != null) {
            logEnricher.close();
        }

        // 关闭 ClickHouse 连接
        try {
            if (clickhousePst != null) {
                clickhousePst.close();
            }
            if (clickhouseConn != null) {
                clickhouseConn.close();
            }
        } catch (Exception e) {
            logger.error("Error closing ClickHouse connection", e);
        }

        // 关闭 Kafka Consumer
        if (consumer != null) {
            consumer.close();
        }

        logger.info("KafkaClickHouseConsumer shutdown complete");
    }

    /**
     * 主入口
     */
    public static void main(String[] args) {
        printBanner();

        String configFile = System.getProperty("user.dir") + File.separator + "config.properties";

        if (args.length > 0) {
            configFile = args[0];
        }

        logger.info("Config file location: {}", configFile);

        KafkaClickHouseConsumer consumer = new KafkaClickHouseConsumer(configFile);

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("========================================");
            logger.info("Shutdown hook triggered");
            consumer.shutdown();
            logger.info("========================================");
        }));

        try {
            consumer.initialize();
            consumer.run();
        } catch (Exception e) {
            logger.error("Fatal error in KafkaClickHouseConsumer", e);
            System.exit(1);
        }
    }

    /**
     * 打印启动横幅
     */
    private static void printBanner() {
        System.out.println();
        System.out.println("========================================");
        System.out.println("  ClkLog Kafka ClickHouse Consumer");
        System.out.println("  Version: 1.2.0");
        System.out.println("========================================");
        System.out.println();
    }
}
