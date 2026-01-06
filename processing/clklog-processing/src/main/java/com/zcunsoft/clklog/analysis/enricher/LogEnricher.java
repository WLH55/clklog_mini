package com.zcunsoft.clklog.analysis.enricher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.zcunsoft.clklog.analysis.bean.LogBean;
import com.zcunsoft.clklog.analysis.bean.LogBeanCollection;
import com.zcunsoft.clklog.analysis.bean.ProjectSetting;
import com.zcunsoft.clklog.analysis.bean.Region;
import com.zcunsoft.clklog.analysis.cfg.RedisSettings;
import com.zcunsoft.clklog.analysis.utils.ExtractUtil;
import com.zcunsoft.clklog.analysis.utils.IPUtil;
import com.zcunsoft.clklog.analysis.utils.ObjectMapperUtil;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 日志数据丰富器
 * <p>
 * 功能：
 * 1. 解析 JSON 日志为 LogBean 对象
 * 2. 解析 User-Agent 获取设备信息
 * 3. 查询 IP 地理位置信息（带 Redis 缓存）
 * 4. 定时刷新 Redis 配置缓存
 */
public class LogEnricher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private AbstractUserAgentAnalyzer userAgentAnalyzer;

    private Jedis jedis;

    private JedisPool jedisSinglePool;

    private JedisSentinelPool jedisSentinelPool;

    private RedisSettings redisSetting;

    private IPUtil ipUtil;

    private HashMap<String, ProjectSetting> htProjectSetting;

    private ScheduledExecutorService scheduler;

    /**
     * 城市中英文映射表
     */
    private Map<String, String> cityMap;

    private final String globalAppCode;

    /**
     * 构造函数
     *
     * @param redisSetting   Redis 配置
     * @param ipFileLocation IP 库文件位置
     * @param globalAppCode  全局应用编码
     */
    public LogEnricher(RedisSettings redisSetting, String ipFileLocation, String globalAppCode) {
        this.redisSetting = redisSetting;
        this.globalAppCode = globalAppCode;

        try {
            // 初始化 IP 工具
            this.ipUtil = new IPUtil(ipFileLocation);
            this.ipUtil.loadIpFile();

            // 初始化 UserAgent 分析器
            this.userAgentAnalyzer = UserAgentAnalyzer.newBuilder()
                    .withField(UserAgent.AGENT_NAME)
                    .withField(UserAgent.AGENT_NAME_VERSION)
                    .withField(UserAgent.DEVICE_NAME)
                    .withField(UserAgent.DEVICE_BRAND)
                    .withField(UserAgent.OPERATING_SYSTEM_NAME)
                    .withField(UserAgent.OPERATING_SYSTEM_NAME_VERSION)
                    .hideMatcherLoadStats()
                    .withCache(10000)
                    .build();

            // 创建 Redis 连接池
            createRedisPool();

            // 加载缓存
            loadCache();

            // 定时刷新缓存
            startCacheRefreshScheduler();

            logger.info("LogEnricher initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize LogEnricher", e);
            throw new RuntimeException("Failed to initialize LogEnricher", e);
        }
    }

    /**
     * 丰富单条日志数据
     *
     * @param line Kafka 原始消息
     * @return 丰富后的 LogBean 集合
     */
    public LogBeanCollection enrich(String line) {
        List<LogBean> logBeanList = ExtractUtil.extractToLogBeanList(line, globalAppCode, userAgentAnalyzer, htProjectSetting);

        if (!logBeanList.isEmpty()) {
            // 批量查询 IP 地理位置缓存
            List<String> clientIpList = logBeanList.stream()
                    .map(LogBean::getClientIp)
                    .distinct()
                    .collect(Collectors.toList());

            List<String> ipInfoList = jedis.hmget("ClientIpRegionHash", clientIpList.toArray(new String[0]));

            // 为每个 logBean 设置地理位置信息
            for (LogBean logBean : logBeanList) {
                if (StringUtils.isNotBlank(logBean.getClientIp())) {
                    Region region = null;
                    int index = clientIpList.indexOf(logBean.getClientIp());
                    String regionInfo = ipInfoList.get(index);

                    if (StringUtils.isNotBlank(regionInfo)) {
                        // 从缓存获取
                        region = parseRegionFromCache(logBean.getClientIp(), regionInfo);
                    } else {
                        // 实时解析并缓存
                        region = ipUtil.analysisRegionFromIp(logBean.getClientIp());
                        region = ExtractUtil.translateRegion(region, cityMap);
                        String sbRegion = region.getClientIp() + "," + region.getCountry() + "," + region.getProvince() + "," + region.getCity();
                        jedis.hset("ClientIpRegionHash", region.getClientIp(), sbRegion);
                    }

                    logBean.setCountry(region.getCountry());
                    logBean.setProvince(region.getProvince());
                    logBean.setCity(region.getCity());
                }
            }
        }

        LogBeanCollection logBeanCollection = new LogBeanCollection();
        logBeanCollection.setData(logBeanList);
        return logBeanCollection;
    }

    /**
     * 从缓存解析地理位置信息
     */
    private Region parseRegionFromCache(String clientIp, String regionInfo) {
        Region region = new Region();
        region.setClientIp(clientIp);

        String[] regionArr = regionInfo.split(",", -1);
        if (regionArr.length == 4) {
            region.setCountry(regionArr[1]);
            region.setProvince(regionArr[2]);
            region.setCity(regionArr[3]);
        }

        return region;
    }

    /**
     * 加载项目配置和城市映射表缓存
     */
    private void loadCache() {
        Jedis jedisForCache = null;
        try {
            if (jedisSinglePool != null) {
                jedisForCache = jedisSinglePool.getResource();
                jedisForCache.select(redisSetting.getDatabase());
                logger.info("jedisForCache created by single redis pool");
            } else if (jedisSentinelPool != null) {
                jedisForCache = jedisSentinelPool.getResource();
                jedisForCache.select(redisSetting.getDatabase());
                logger.info("jedisForCache created by sentinel redis pool");
            }

            if (jedisForCache != null) {
                // 加载城市中英文映射表
                cityMap = jedisForCache.hgetAll("CityEngChsMapKey");
                logger.info("cityMap size " + cityMap.size());

                // 加载项目配置
                htProjectSetting = loadProjectSetting(jedisForCache);
                logger.info("htProjectSetting size " + htProjectSetting.size());
            }
        } catch (Exception ex) {
            logger.error("loadCache err ", ex);
        } finally {
            if (jedisForCache != null) {
                try {
                    jedisForCache.close();
                } catch (Exception e) {
                    logger.error("close jedisForCache err ", e);
                }
            }
        }
    }

    /**
     * 加载项目配置
     */
    private HashMap<String, ProjectSetting> loadProjectSetting(Jedis jedis1) {
        HashMap<String, ProjectSetting> projectSettingHashMap = new HashMap<>();
        try {
            String projectSettingContent = jedis1.get("ProjectSettingKey");
            if (StringUtils.isNotBlank(projectSettingContent)) {
                TypeReference<HashMap<String, ProjectSetting>> htProjectSettingTypeReference = new TypeReference<HashMap<String, ProjectSetting>>() {
                };
                ObjectMapperUtil mapper = new ObjectMapperUtil();
                projectSettingHashMap = mapper.readValue(projectSettingContent, htProjectSettingTypeReference);

                for (Map.Entry<String, ProjectSetting> item : projectSettingHashMap.entrySet()) {
                    item.getValue().setPathRuleList(ExtractUtil.extractPathRule(item.getValue().getUriPathRules()));
                }
            }
        } catch (Exception ex) {
            logger.error("load ProjectSetting error", ex);
        }
        return projectSettingHashMap;
    }

    /**
     * 创建 Redis 连接池
     */
    private void createRedisPool() {
        JedisPoolConfig poolConfig = createJedisPoolConfig();

        if (redisSetting.getSentinel() != null) {
            String[] nodeList = redisSetting.getSentinel().getNodes().split(",", -1);
            Set<String> sentinels = new HashSet<>(Arrays.asList(nodeList));
            jedisSentinelPool = new JedisSentinelPool(
                    redisSetting.getSentinel().getMaster(),
                    sentinels,
                    poolConfig,
                    5000,
                    redisSetting.getPassword()
            );
            jedis = jedisSentinelPool.getResource();
            jedis.select(redisSetting.getDatabase());
            logger.info("open sentinel redis ok");
        } else {
            jedisSinglePool = new JedisPool(
                    poolConfig,
                    redisSetting.getHost(),
                    redisSetting.getPort(),
                    5000,
                    redisSetting.getPassword()
            );
            jedis = jedisSinglePool.getResource();
            jedis.select(redisSetting.getDatabase());
            logger.info("open single redis ok");
        }
    }

    /**
     * 创建 Redis 连接池配置
     */
    private JedisPoolConfig createJedisPoolConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        RedisSettings.Pool props = redisSetting.getPool();
        config.setMaxTotal(props.getMaxActive());
        config.setMaxIdle(props.getMaxIdle());
        config.setMinIdle(props.getMinIdle());
        config.setMaxWait(Duration.ofMillis(props.getMaxWait()));
        return config;
    }

    /**
     * 启动定时刷新缓存任务
     */
    private void startCacheRefreshScheduler() {
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r, "load-cache-thread");
            thread.setUncaughtExceptionHandler((t, ex) -> {
                logger.error("Thread " + t + " got uncaught exception: ", ex);
            });
            return thread;
        });

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                loadCache();
            } catch (Exception ex) {
                logger.error("Exception occurred when refreshing cache: " + ex);
            }
        }, 60, 60, TimeUnit.SECONDS);

        logger.info("Cache refresh scheduler started, interval: 60 seconds");
    }

    /**
     * 关闭资源
     */
    public void close() {
        try {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            logger.error("Error shutting down scheduler", e);
        }

        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                logger.error("Error closing jedis", e);
            }
        }

        if (jedisSinglePool != null) {
            try {
                jedisSinglePool.close();
            } catch (Exception e) {
                logger.error("Error closing jedisSinglePool", e);
            }
        }

        if (jedisSentinelPool != null) {
            try {
                jedisSentinelPool.close();
            } catch (Exception e) {
                logger.error("Error closing jedisSentinelPool", e);
            }
        }

        logger.info("LogEnricher closed");
    }
}
