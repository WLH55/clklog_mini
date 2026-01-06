package com.zcunsoft.clklog.analysis.entry;

import com.zcunsoft.clklog.analysis.bean.LogBean;
import com.zcunsoft.clklog.analysis.bean.LogBeanCollection;
import com.zcunsoft.clklog.analysis.cfg.RedisSettings;
import com.zcunsoft.clklog.analysis.enricher.LogEnricher;
import com.zcunsoft.clklog.analysis.utils.ClickHouseUtil;
import org.apache.commons.lang3.StringUtils;
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
 * 2. 解析并丰富数据（IP 地理位置、User-Agent 等）
 * 3. 批量写入 ClickHouse（混合模式：1000 条或 10 秒）
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
    private final List<LogBean> batchBuffer = new ArrayList<>();

    // 时间跟踪
    private long lastFlushTime = System.currentTimeMillis();

    // 统计信息
    private final AtomicLong totalConsumed = new AtomicLong(0);
    private final AtomicLong totalWritten = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // 关闭标志
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * ClickHouse 插入 SQL
     */
    private static final String INSERT_SQL = "insert into log_analysis (" +
            "distinct_id,typeContext,event,time,track_id,flush_time,identity_cookie_id,lib,lib_method,lib_version," +
            "timezone_offset,screen_height,screen_width,viewport_height,viewport_width,referrer,url,url_path,title,latest_referrer," +
            "latest_search_keyword,latest_traffic_source_type,is_first_day,is_first_time,referrer_host,log_time,stat_date,stat_hour,element_id," +
            "project_name,client_ip,country,province,city,app_id,app_name," +
            "app_state,app_version,brand,browser,browser_version,carrier,device_id,element_class_name,element_content,element_name," +
            "element_position,element_selector,element_target_url,element_type,first_channel_ad_id,first_channel_adgroup_id,first_channel_campaign_id,first_channel_click_id,first_channel_name,latest_landing_page," +
            "latest_referrer_host,latest_scene,latest_share_method,latest_utm_campaign,latest_utm_content,latest_utm_medium,latest_utm_source,latest_utm_term,latitude,longitude," +
            "manufacturer,matched_key,matching_key_list,model,network_type,os,os_version,receive_time,screen_name,screen_orientation," +
            "short_url_key,short_url_target,source_package_name,track_signup_original_id,user_agent,utm_campaign,utm_content,utm_matching_type,utm_medium,utm_source," +
            "utm_term,viewport_position,wifi,kafka_data_time,project_token,crc,is_compress,event_duration,user_key," +
            "is_logined,download_channel,event_session_id,raw_url,create_time,app_crashed_reason" +
            ") values (" +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?)";

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
        RedisSettings redisSetting = createRedisSettings();
        String ipFileLocation = config.getProperty("processing-file-location", "");
        String globalAppCode = config.getProperty("global.app.code", "clklog-global");
        logEnricher = new LogEnricher(redisSetting, ipFileLocation, globalAppCode);

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
     * 创建 Redis 配置
     */
    private RedisSettings createRedisSettings() {
        RedisSettings redisSetting = new RedisSettings();
        redisSetting.setHost(config.getProperty("redis.host"));
        redisSetting.setPort(Integer.parseInt(config.getProperty("redis.port", "6379")));

        String redisPwd = config.getProperty("redis.password", "");
        if (StringUtils.isBlank(redisPwd)) {
            redisSetting.setPassword(null);
        } else {
            redisSetting.setPassword(redisPwd);
        }

        int redisDatabase = Integer.parseInt(config.getProperty("redis.database", "0"));
        if (redisDatabase >= 0) {
            redisSetting.setDatabase(redisDatabase);
        }

        RedisSettings.Pool pool = new RedisSettings.Pool();
        pool.setMaxActive(Integer.parseInt(config.getProperty("redis.pool.max-active", "3")));
        pool.setMaxIdle(Integer.parseInt(config.getProperty("redis.pool.max-idle", "3")));
        pool.setMinIdle(Integer.parseInt(config.getProperty("redis.pool.min-idle", "0")));
        pool.setMaxWait(Integer.parseInt(config.getProperty("redis.pool.max-wait", "-1")));
        redisSetting.setPool(pool);

        String sentinelMaster = config.getProperty("redis.sentinel.master");
        if (sentinelMaster != null) {
            RedisSettings.Sentinel sentinel = new RedisSettings.Sentinel();
            sentinel.setMaster(sentinelMaster);
            sentinel.setNodes(config.getProperty("redis.sentinel.nodes"));
            redisSetting.setSentinel(sentinel);
        }

        return redisSetting;
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
        LogBeanCollection collection = logEnricher.enrich(record);
        List<LogBean> logBeans = collection.getData();

        if (logBeans != null && !logBeans.isEmpty()) {
            batchBuffer.addAll(logBeans);
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
            for (LogBean logBean : batchBuffer) {
                setPreparedStatementParameters(clickhousePst, logBean);
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
    private void setPreparedStatementParameters(PreparedStatement pst, LogBean value) throws SQLException {
        pst.setString(1, value.getDistinctId());
        pst.setString(2, value.getTypeContext());
        pst.setString(3, value.getEvent());
        pst.setString(4, String.valueOf(value.getTime()));
        pst.setString(5, value.getTrackId());
        pst.setString(6, String.valueOf(value.getFlushTime()));
        pst.setString(7, value.getIdentityCookieId());
        pst.setString(8, value.getLib());
        pst.setString(9, value.getLibMethod());
        pst.setString(10, value.getLibVersion());
        pst.setString(11, value.getTimezoneOffset());
        pst.setString(12, value.getScreenHeight());
        pst.setString(13, value.getScreenWidth());
        pst.setString(14, value.getViewportHeight());
        pst.setString(15, value.getViewportWidth());
        pst.setString(16, value.getReferrer());
        pst.setString(17, value.getUrl());
        pst.setString(18, value.getUrlPath());
        pst.setString(19, value.getTitle());
        pst.setString(20, value.getLatestReferrer());
        pst.setString(21, value.getLatestSearchKeyword());
        pst.setString(22, value.getLatestTrafficSourceType());
        pst.setString(23, value.getIsFirstDay());
        pst.setString(24, value.getIsFirstTime());
        pst.setString(25, value.getReferrerHost());
        pst.setTimestamp(26, value.getLogTime());
        pst.setDate(27, value.getStatDate() != null ? java.sql.Date.valueOf(value.getStatDate()) : null);
        pst.setString(28, value.getStatHour());
        pst.setString(29, value.getElementId());
        pst.setString(30, value.getProjectName());
        pst.setString(31, value.getClientIp());
        pst.setString(32, value.getCountry());
        pst.setString(33, value.getProvince());
        pst.setString(34, value.getCity());
        pst.setString(35, value.getAppId());
        pst.setString(36, value.getAppName());
        pst.setString(37, value.getAppState());
        pst.setString(38, value.getAppVersion());
        pst.setString(39, value.getBrand());
        pst.setString(40, value.getBrowser());
        pst.setString(41, value.getBrowserVersion());
        pst.setString(42, value.getCarrier());
        pst.setString(43, value.getDeviceId());
        pst.setString(44, value.getElementClassName());
        pst.setString(45, value.getElementContent());
        pst.setString(46, value.getElementName());
        pst.setString(47, value.getElementPosition());
        pst.setString(48, value.getElementSelector());
        pst.setString(49, value.getElementTargetUrl());
        pst.setString(50, value.getElementType());
        pst.setString(51, value.getFirstChannelAdId());
        pst.setString(52, value.getFirstChannelAdgroupId());
        pst.setString(53, value.getFirstChannelCampaignId());
        pst.setString(54, value.getFirstChannelClickId());
        pst.setString(55, value.getFirstChannelName());
        pst.setString(56, value.getLatestLandingPage());
        pst.setString(57, value.getLatestReferrerHost());
        pst.setString(58, value.getLatestScene());
        pst.setString(59, value.getLatestShareMethod());
        pst.setString(60, value.getLatestUtmCampaign());
        pst.setString(61, value.getLatestUtmContent());
        pst.setString(62, value.getLatestUtmMedium());
        pst.setString(63, value.getLatestUtmSource());
        pst.setString(64, value.getLatestUtmTerm());

        if (value.getLatitude() == null) {
            pst.setObject(65, null);
        } else {
            pst.setDouble(65, value.getLatitude());
        }
        if (value.getLongitude() == null) {
            pst.setObject(66, null);
        } else {
            pst.setDouble(66, value.getLongitude());
        }

        pst.setString(67, value.getManufacturer());
        pst.setString(68, value.getMatchedKey());
        pst.setString(69, value.getMatchingKeyList());
        pst.setString(70, value.getModel());
        pst.setString(71, value.getNetworkType());
        pst.setString(72, value.getOs());
        pst.setString(73, value.getOsVersion());
        pst.setString(74, value.getReceiveTime());
        pst.setString(75, value.getScreenName());
        pst.setString(76, value.getScreenOrientation());
        pst.setString(77, value.getShortUrlKey());
        pst.setString(78, value.getShortUrlTarget());
        pst.setString(79, value.getSourcePackageName());
        pst.setString(80, value.getTrackSignupOriginalId());
        pst.setString(81, value.getUserAgent());
        pst.setString(82, value.getUtmCampaign());
        pst.setString(83, value.getUtmContent());
        pst.setString(84, value.getUtmMatchingType());
        pst.setString(85, value.getUtmMedium());
        pst.setString(86, value.getUtmSource());
        pst.setString(87, value.getUtmTerm());

        if (value.getViewportPosition() == null) {
            pst.setObject(88, null);
        } else {
            pst.setInt(88, value.getViewportPosition());
        }

        pst.setString(89, value.getWifi());
        pst.setString(90, value.getKafkaDataTime());
        pst.setString(91, value.getProjectToken());
        pst.setString(92, value.getCrc());
        pst.setString(93, value.getIsCompress());
        pst.setDouble(94, value.getEventDuration() != null ? value.getEventDuration() : 0D);
        pst.setString(95, value.getUserKey());
        pst.setInt(96, value.getIsLogined() != null ? value.getIsLogined() : 0);
        pst.setString(97, value.getDownloadChannel());
        pst.setString(98, value.getEventSessionId());
        pst.setString(99, value.getRawUrl());
        pst.setString(100, value.getCreateTime());
        pst.setString(101, value.getAppCrashedReason());
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
