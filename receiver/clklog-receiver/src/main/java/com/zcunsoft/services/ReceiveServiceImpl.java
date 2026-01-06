package com.zcunsoft.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.ip2location.IP2Location;
import com.ip2location.IPResult;
import com.zcunsoft.cfg.KafkaSetting;
import com.zcunsoft.cfg.ReceiverSetting;
import com.zcunsoft.cfg.RedisConstsConfig;
import com.zcunsoft.handlers.ConstsDataHolder;
import com.zcunsoft.model.*;
import com.zcunsoft.util.*;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

@Service
public class ReceiveServiceImpl implements IReceiveService {

    private final InetAddressValidator validator = InetAddressValidator.getInstance();

    private final ConstsDataHolder constsDataHolder;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Logger storeLogger = LogManager.getLogger("com.zcunsoft.store");

    private final ObjectMapperUtil objectMapper;


    private final StringRedisTemplate queueRedisTemplate;


    private final AbstractUserAgentAnalyzer userAgentAnalyzer;

    private final IP2Location locIpV4 = new IP2Location();

    private final IP2Location locIpV6 = new IP2Location();

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final TypeReference<HashMap<String, ProjectSetting>> htProjectSettingTypeReference = new TypeReference<HashMap<String, ProjectSetting>>() {
    };

    private final ReceiverSetting serverSettings;

    private final KafkaSetting kafkaSetting;

    /**
     * redis常量配置.
     */
    private final RedisConstsConfig redisConstsConfig;

    public ReceiveServiceImpl(ConstsDataHolder constsDataHolder, ObjectMapperUtil objectMapper, StringRedisTemplate queueRedisTemplate, AbstractUserAgentAnalyzer userAgentAnalyzer, JdbcTemplate clickHouseJdbcTemplate, ReceiverSetting serverSettings, KafkaSetting kafkaSetting, RedisConstsConfig redisConstsConfig) {
        this.objectMapper = objectMapper;
        this.queueRedisTemplate = queueRedisTemplate;
        this.userAgentAnalyzer = userAgentAnalyzer;
        this.constsDataHolder = constsDataHolder;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.serverSettings = serverSettings;
        this.kafkaSetting = kafkaSetting;
        this.redisConstsConfig = redisConstsConfig;
        String binIpV4file = getResourcePath() + File.separator + "iplib" + File.separator + "IP2LOCATION-LITE-DB3.BIN";

        try {
            locIpV4.Open(binIpV4file, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String binIpV6file = getResourcePath() + File.separator + "iplib" + File.separator + "IP2LOCATION-LITE-DB3.IPV6.BIN";

        try {
            locIpV6.Open(binIpV6file, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void extractLog(QueryCriteria queryCriteria, HttpServletRequest request) {
            String bodyString = getBodyString(request);
            String[] bodyStringList = bodyString.split("&");
            if (bodyStringList.length == 1 && !bodyString.equals("")) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(bodyString);

                    queryCriteria.setGzip(jsonNode.get("gzip").asText());
                    queryCriteria.setData_list(jsonNode.get("data_list").asText());
                    queryCriteria.setData(jsonNode.get("data").asText());
                    queryCriteria.setCrc(jsonNode.get("crc").asText());
                } catch (Exception exception) {
                }
            }
            for (String s : bodyStringList) {
                String[] elm = s.split("=");
                if (elm[0].equals("data_list")) {
                    queryCriteria.setData_list(elm[1]);
                } else if (elm[0].equals("data")) {
                    queryCriteria.setData(elm[1]);
                } else if (elm[0].equals("crc")) {
                    queryCriteria.setCrc(elm[1]);
                } else if (elm[0].equals("gzip")) {
                    queryCriteria.setGzip(elm[1]);
                }
            }
            if (queryCriteria.getCrc() == null) {
                queryCriteria.setCrc("");
            }
            String ip = getIpAddr(request);
            try {
                String dataFinal, decodedString = null;
                if (queryCriteria.getData_list() != null) {
                    if (Pattern.matches(".*\\+.*|.*\\/.*|.*=.*", queryCriteria.getData_list())) {
                        decodedString = queryCriteria.getData_list();
                    } else {
                        decodedString = URLDecoder.decode(queryCriteria.getData_list());
                    }
                } else if (queryCriteria.getData() != null) {
                    if (Pattern.matches(".*\\+.*|.*\\/.*|.*=.*", queryCriteria.getData())) {
                        decodedString = queryCriteria.getData();
                    } else {
                        decodedString = URLDecoder.decode(queryCriteria.getData());
                    }
                } else {
                    logger.error("data为空");
                }
                if (decodedString != null) {
                    Base64.Decoder decoder = Base64.getDecoder();
                    byte[] byteArrayNEW = decoder.decode(decodedString);

                    if (queryCriteria.getGzip().equals("1")) {
                        dataFinal = GZIPUtils.uncompressToString(byteArrayNEW);
                    } else {
                        dataFinal = new String(byteArrayNEW);
                    }
                    queryCriteria.setData(dataFinal);
                    queryCriteria.setClientIp(ip);
                    String ua = request.getHeader("user-agent");
                    if (ua == null) {
                        ua = request.getHeader("User-Agent");
                    }
                    queryCriteria.setUa(ua);
                    queryCriteria.setData(dataFinal);
                    // 数据解析完成，由控制器放入内存队列
                    constsDataHolder.getLogQueue().put(queryCriteria);
                    storeLogger.info(ip + "," + dataFinal);
                }
            } catch (Exception e) {
                String logData = queryCriteria.toString();
                logger.error(logData, e);
            }


    }

    private String getBodyString(HttpServletRequest request) {
        ServletInputStream servletInputStream = null;
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = null;
        try {
            servletInputStream = request.getInputStream();
            reader = new BufferedReader(new InputStreamReader((InputStream) servletInputStream, StandardCharsets.UTF_8));
            String line = "";
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (servletInputStream != null) {
                try {
                    servletInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }

    private String getIpAddr(HttpServletRequest request) {
        String ipAddress = null;
        try {
            ipAddress = request.getHeader("x-forwarded-for");
            if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
                ipAddress = request.getHeader("Proxy-Client-IP");
            }
            if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
                ipAddress = request.getHeader("WL-Proxy-Client-IP");
            }
            if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
                ipAddress = request.getRemoteAddr();
                if (ipAddress.equals("127.0.0.1")) {

                    InetAddress inet = null;
                    try {
                        inet = InetAddress.getLocalHost();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                    if (inet != null) {
                        ipAddress = inet.getHostAddress();
                    }
                }
            }
            if (ipAddress != null && ipAddress.length() > 15) {
                if (ipAddress.indexOf(",") > 0) {
                    ipAddress = ipAddress.substring(0, ipAddress.indexOf(","));
                }
            }
        } catch (Exception e) {
            ipAddress = "";
        }

        return ipAddress;
    }

    /**
     * @param clientIp 客户端IP
     * @return 地域信息
     */
    @Override
    public Region analysisRegionFromIp(String clientIp) {
        Region region = new Region();
        region.setClientIp(clientIp);
        String regionInfo = (String) queueRedisTemplate.opsForHash().get(redisConstsConfig.getClientIpRegionHashKey(), clientIp);
        if (regionInfo == null) {
            /* redis里没有clientIp的信息,则从IP库分析 */
            region = analysisRegionFromIpBaseOnIp2Loc(clientIp);
            if (region != null) {
                region = ExtractUtil.translateRegion(region, constsDataHolder.getHtForCity());
                String sbRegion = region.getClientIp() + "," + region.getCountry() + "," + region.getProvince() + "," + region.getCity();
                queueRedisTemplate.opsForHash().put(redisConstsConfig.getClientIpRegionHashKey(), region.getClientIp(), sbRegion);
            }
        } else {
            String[] regionArr = regionInfo.split(",", -1);
            if (regionArr.length == 4) {
                region.setCountry(regionArr[1]);
                region.setProvince(regionArr[2]);
                region.setCity(regionArr[3]);
            }
        }
        return region;
    }

    /**
     * @param clientIp 客户端IP
     * @return 地域信息
     */
    @Override
    public Region analysisRegionFromIpBaseOnIp2Loc(String clientIp) {
        IPResult rec = null;
        if (validator.isValidInet4Address(clientIp)) {
            rec = analysisIp(true, clientIp);
        } else if (validator.isValidInet6Address(clientIp)) {
            rec = analysisIp(false, clientIp);
        }
        Region region = null;
        if (rec != null && "OK".equalsIgnoreCase(rec.getStatus())) {
            String country = rec.getCountryShort().toLowerCase(Locale.ROOT);
            String province = rec.getRegion().toLowerCase(Locale.ROOT);
            String city = rec.getCity().toLowerCase(Locale.ROOT);
            if ("-".equalsIgnoreCase(country)) {
                country = "";
            }
            if ("-".equalsIgnoreCase(province)) {
                province = "";
            }
            if ("-".equalsIgnoreCase(city)) {
                city = "";
            }
            if (StringUtils.isNotBlank(country)) {
                if (country.equalsIgnoreCase("TW")) {
                    country = "cn";
                    province = "taiwan";
                }
                if (country.equalsIgnoreCase("hk")) {
                    country = "cn";
                    province = "hongkong";
                    city = "hongkong";
                }
                if (country.equalsIgnoreCase("mo")) {
                    country = "cn";
                    province = "macau";
                    city = "macau";
                }
            }
            region = new Region();
            region.setClientIp(clientIp);
            region.setCountry(country);
            region.setProvince(province);
            region.setCity(city);
        }
        return region;
    }

    private IPResult analysisIp(boolean isIpV4, String clientIp) {
        IPResult rec = null;
        try {
            if (isIpV4) {
                rec = locIpV4.IPQuery(clientIp);
            } else {
                rec = locIpV6.IPQuery(clientIp);
            }
        } catch (Exception e) {
            logger.error("analysisIp error ", e);
        }
        return rec;
    }

    @Override
    public void loadCity() {
        try {
            /* 从redis读取城市中英文对照表，保存在本地缓存 */
            Map<Object, Object> entryMap = queueRedisTemplate.opsForHash().entries(redisConstsConfig.getCityEngChsMapKey());

            ConcurrentMap<String, String> htForCity = constsDataHolder.getHtForCity();
            for (Map.Entry<Object, Object> entry : entryMap.entrySet()) {
                htForCity.put(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (Exception ex) {
            logger.error("load City err", ex);
        }
    }

    @Override
    public void loadProjectSetting() {
        try {
            HashMap<String, ProjectSetting> projectSettingHashMap = getProjectSetting();
            constsDataHolder.getHtProjectSetting().putAll(projectSettingHashMap);
        } catch (Exception ex) {
            logger.error("load ProjectSetting err", ex);
        }
    }

    /**
     * 获取项目配置.
     *
     * @return 项目配置
     */
    private HashMap<String, ProjectSetting> getProjectSetting() {
        HashMap<String, ProjectSetting> projectSettingHashMap = new HashMap<>();
        try {
            /* 从redis读取项目配置 */
            String projectSettingContent = queueRedisTemplate.opsForValue().get(redisConstsConfig.getProjectSettingKey());
            if (StringUtils.isNotBlank(projectSettingContent)) {
                projectSettingHashMap = objectMapper.readValue(projectSettingContent,
                        htProjectSettingTypeReference);

                for (Map.Entry<String, ProjectSetting> item : projectSettingHashMap.entrySet()) {
                    item.getValue().setPathRuleList(ExtractUtil.extractPathRule(item.getValue().getUriPathRules()));
                }
            }
        } catch (Exception ex) {
            logger.error("get ProjectSetting err", ex);
        }
        return projectSettingHashMap;
    }

    @Override
    public void enqueueKafka(List<QueryCriteria> queryCriteriaList) {
        try {
            KafkaProducerUtil producerKafka = KafkaProducerUtil.getInstance(kafkaSetting);

            for (QueryCriteria queryCriteria : queryCriteriaList) {
                String dataFinal = queryCriteria.getData();
                if (dataFinal != null && !dataFinal.trim().isEmpty()) {
                    // 格式: timestamp,crc,gzip,clientIp,data
                    String logData = String.valueOf(System.currentTimeMillis()) + ',' +
                            queryCriteria.getCrc() + ',' +
                            queryCriteria.getGzip() + ',' +
                            queryCriteria.getClientIp() + ',' +
                            dataFinal;
                    producerKafka.sendMessgae(kafkaSetting.getProducer().getTopic(), logData);
                }
            }
        } catch (Exception ex) {
            logger.error("enqueueKafka error", ex);
        }
    }



    /**
     * 批量解析神策数据并写入 sensors_events 表
     * 优化：将多个 QueryCriteria 的数据合并后批量写入
     *
     * @param queryCriteriaList 查询条件列表
     */
    @Override
    public void batchSaveSensorsDataToClickHouse(List<QueryCriteria> queryCriteriaList) {
        if (queryCriteriaList == null || queryCriteriaList.isEmpty()) {
            return;
        }

        // 收集所有的 SensorsEventsLogBean
        List<SensorsEventsLogBean> allBeanList = new ArrayList<>();

        for (QueryCriteria queryCriteria : queryCriteriaList) {
            if (queryCriteria == null || queryCriteria.getData() == null) {
                continue;
            }

            try {
                // 解析 JSON 数组
                JsonNode array = objectMapper.readTree(queryCriteria.getData());

                if (array.isArray()) {
                    for (JsonNode jsonNode : array) {
                        SensorsEventsLogBean bean = SensorsDataMapper.mapToSensorsEvents(jsonNode);
                        if (SensorsDataMapper.isValid(bean)) {
                            allBeanList.add(bean);
                        }
                    }
                } else {
                    SensorsEventsLogBean bean = SensorsDataMapper.mapToSensorsEvents(array);
                    if (SensorsDataMapper.isValid(bean)) {
                        allBeanList.add(bean);
                    }
                }

            } catch (Exception ex) {
                logger.error("解析 QueryCriteria 数据失败: " + queryCriteria.getClientIp(), ex);
            }
        }

        // 批量写入 ClickHouse
        if (!allBeanList.isEmpty()) {
            doSaveToSensorsEvents(allBeanList);
        }
    }

    /**
     * 批量写入 sensors_events 表
     *
     * @param beanList SensorsEventsLogBean 列表
     */
    private void doSaveToSensorsEvents(List<SensorsEventsLogBean> beanList) {
        // 将毫秒时间戳转换为 DateTime64(3) 格式（除以1000得到秒）
        String sql = "INSERT INTO sensors_events (" +
                "_track_id, time, type, distinct_id, anonymous_id, event, _flush_time," +
                "identity_anonymous_id, identity_android_id," +
                "lib_method, lib, lib_version, app_version, lib_detail," +
                "town_name, town_action," +
                "is_first_day, os, os_version, manufacturer, model, brand," +
                "screen_width, screen_height, timezone_offset, app_id, app_name," +
                "wifi, network_type, lib_plugin_version, device_id," +
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

        clickHouseJdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement pst, int i) throws SQLException {
                SensorsEventsLogBean bean = beanList.get(i);

                int idx = 1;

                // 基本事件信息
                pst.setLong(idx++, nvlLong(bean.get_track_id()));
                pst.setLong(idx++, nvlLong(bean.getTime()));
                pst.setString(idx++, nvl(bean.getType()));
                pst.setString(idx++, nvl(bean.getDistinct_id()));
                pst.setString(idx++, nvl(bean.getAnonymous_id()));
                pst.setString(idx++, nvl(bean.getEvent()));
                pst.setLong(idx++, nvlLong(bean.get_flush_time()));

                // 用户身份信息
                pst.setString(idx++, nvl(bean.getIdentity_anonymous_id()));
                pst.setString(idx++, nvl(bean.getIdentity_android_id()));

                // SDK 信息
                pst.setString(idx++, nvl(bean.getLib_method()));
                pst.setString(idx++, nvl(bean.getLib()));
                pst.setString(idx++, nvl(bean.getLib_version()));
                pst.setString(idx++, nvl(bean.getApp_version()));
                pst.setString(idx++, nvl(bean.getLib_detail()));

                // 自定义事件参数
                pst.setString(idx++, nvl(bean.getTown_name()));
                pst.setString(idx++, nvl(bean.getTown_action()));

                // 神策预置属性
                pst.setBoolean(idx++, nvlBoolean(bean.getIs_first_day()));
                pst.setString(idx++, nvl(bean.getOs()));
                pst.setString(idx++, nvl(bean.getOs_version()));
                pst.setString(idx++, nvl(bean.getManufacturer()));
                pst.setString(idx++, nvl(bean.getModel()));
                pst.setString(idx++, nvl(bean.getBrand()));
                pst.setInt(idx++, nvlInt(bean.getScreen_width()));
                pst.setInt(idx++, nvlInt(bean.getScreen_height()));
                pst.setInt(idx++, nvlInt(bean.getTimezone_offset()));
                pst.setString(idx++, nvl(bean.getApp_id()));
                pst.setString(idx++, nvl(bean.getApp_name()));
                pst.setBoolean(idx++, nvlBoolean(bean.getWifi()));
                pst.setString(idx++, nvl(bean.getNetwork_type()));

                // lib_plugin_version 数组
                List<String> plugins = bean.getLib_plugin_version();
                if (plugins != null && !plugins.isEmpty()) {
                    // 使用 join 构建数组字符串: ['v1','v2']
                    StringBuilder sb = new StringBuilder("[");
                    for (int j = 0; j < plugins.size(); j++) {
                        if (j > 0) sb.append(",");
                        sb.append("'").append(plugins.get(j).replace("'", "\\'")).append("'");
                    }
                    sb.append("]");
                    pst.setString(idx++, sb.toString());
                } else {
                    pst.setString(idx++, "[]");
                }
                pst.setString(idx++, nvl(bean.getDevice_id()));

                // 原始 JSON 字段
                pst.setString(idx++, nvl(bean.getRaw_identities()));
                pst.setString(idx++, nvl(bean.getRaw_lib()));
                pst.setString(idx++, nvl(bean.getRaw_properties()));
                pst.setString(idx++, nvl(bean.getRaw_event_params()));
            }

            @Override
            public int getBatchSize() {
                return beanList.size();
            }
        });

        logger.info("成功写入 sensors_events 表 {} 条记录", beanList.size());
    }

    /**
     * 将 null 转换为空字符串
     *
     * @param value 输入值
     * @return 空字符串或原值
     */
    private String nvl(String value) {
        return value == null ? "" : value;
    }

    /**
     * 将 null 转换为 0
     *
     * @param value 输入值
     * @return 0 或原值
     */
    private int nvlInt(Integer value) {
        return value == null ? 0 : value;
    }

    /**
     * 将 null 转换为 0L
     *
     * @param value 输入值
     * @return 0L 或原值
     */
    private long nvlLong(Long value) {
        return value == null ? 0L : value;
    }

    /**
     * 将 null 转换为 false
     *
     * @param value 输入值
     * @return false 或原值
     */
    private boolean nvlBoolean(Boolean value) {
        return value == null ? false : value;
    }

    private String getResourcePath() {
        if (StringUtils.isBlank(serverSettings.getResourcePath())) {
            return System.getProperty("user.dir");
        } else {
            return serverSettings.getResourcePath();
        }
    }
}
