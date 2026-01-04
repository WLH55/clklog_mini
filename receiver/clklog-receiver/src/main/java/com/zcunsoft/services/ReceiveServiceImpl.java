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
                    // 最小可执行单元：不再放入队列，直接在控制器中处理
                    // constsDataHolder.getLogQueue().put(queryCriteria);
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
    public List<LogBean> analysisData(QueryCriteria queryCriteria) {
        List<LogBean> logBeanList = new ArrayList<>();
        Region region = analysisRegionFromIp(queryCriteria.getClientIp());
        String ua = queryCriteria.getUa();
        try {
            JsonNode array = objectMapper.readTree(queryCriteria.getData());

            ProjectSetting projectSetting = constsDataHolder.getHtProjectSetting().get("clklog-global");
            if (constsDataHolder.getHtProjectSetting().containsKey(queryCriteria.getProject())) {
                projectSetting = constsDataHolder.getHtProjectSetting().get(queryCriteria.getProject());
            }
            if (array.isArray()) {
                for (JsonNode jn : array) {
                    LogBean logBean = ExtractUtil.extractToLogBean(jn, userAgentAnalyzer, projectSetting, region, queryCriteria);
                    if (ExtractUtil.filterData(logBean, projectSetting)) {
                        logBeanList.add(logBean);
                    }
                }

            } else {
                LogBean logBean = ExtractUtil.extractToLogBean(array, userAgentAnalyzer, projectSetting, region, queryCriteria);
                if (ExtractUtil.filterData(logBean, projectSetting)) {
                    logBeanList.add(logBean);
                }
            }
        } catch (Exception ex) {
            logger.error("analysisData err", ex);
        }

        return logBeanList;
    }

    @Override
    public void saveToClickHouse(List<QueryCriteria> queryCriteriaList) {
        List<LogBean> allList = new ArrayList<>();
        for (QueryCriteria queryCriteria : queryCriteriaList) {
            List<LogBean> logBeanList = analysisData(queryCriteria);
            if (!logBeanList.isEmpty()) {
                allList.addAll(logBeanList);
            }
        }
        if (!allList.isEmpty()) {
            doSaveToClickHouse(allList);
        }
    }

    private void doSaveToClickHouse(List<LogBean> logBeanList) {

        String sql = "insert into log_analysis (distinct_id,typeContext,event,time,track_id,flush_time,identity_cookie_id,lib,lib_method,lib_version," +
                "timezone_offset,screen_height,screen_width,viewport_height,viewport_width,referrer,url,url_path,title,latest_referrer," +
                "latest_search_keyword,latest_traffic_source_type,is_first_day,is_first_time,referrer_host,log_time,stat_date,stat_hour,element_id," +
                "project_name,client_ip,country,province,city,app_id,app_name," +
                "app_state,app_version,brand,browser,browser_version,carrier,device_id,element_class_name,element_content,element_name," +
                "element_position,element_selector,element_target_url,element_type,first_channel_ad_id,first_channel_adgroup_id,first_channel_campaign_id,first_channel_click_id,first_channel_name,latest_landing_page," +
                "latest_referrer_host,latest_scene,latest_share_method,latest_utm_campaign,latest_utm_content,latest_utm_medium,latest_utm_source,latest_utm_term,latitude,longitude," +
                "manufacturer,matched_key,matching_key_list,model,network_type,os,os_version,receive_time,screen_name,screen_orientation," +
                "short_url_key,short_url_target,source_package_name,track_signup_original_id,user_agent,utm_campaign,utm_content,utm_matching_type,utm_medium,utm_source," +
                "utm_term,viewport_position,wifi,kafka_data_time,project_token,crc,is_compress,event_duration,user_key," +
                "is_logined,download_channel,event_session_id,raw_url,create_time,app_crashed_reason)" +
                " values " +
                "(?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?)";

        clickHouseJdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement pst, int i) throws SQLException {
                LogBean value = logBeanList.get(i);
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
                pst.setDate(27, java.sql.Date.valueOf(value.getStatDate()));
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
                pst.setObject(65, value.getLatitude());
                pst.setObject(66, value.getLongitude());
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
                pst.setObject(88, value.getViewportPosition());
                pst.setString(89, value.getWifi());
                pst.setString(90, value.getKafkaDataTime());
                pst.setString(91, value.getProjectToken());
                pst.setString(92, value.getCrc());
                pst.setString(93, value.getIsCompress());
                pst.setDouble(94, value.getEventDuration());
                pst.setString(95, value.getUserKey());
                pst.setInt(96, value.getIsLogined());
                pst.setString(97, value.getDownloadChannel());
                pst.setString(98, value.getEventSessionId());
                pst.setString(99, value.getRawUrl());
                pst.setString(100, value.getCreateTime());
                pst.setString(101, value.getAppCrashedReason());
            }

            @Override
            public int getBatchSize() {
                return logBeanList.size();
            }
        });
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
                    String logData = String.valueOf(System.currentTimeMillis()) + ',' + queryCriteria.getProject() + ',' + queryCriteria.getToken() + ',' + queryCriteria.getCrc() + ',' + queryCriteria.getGzip() + ',' + queryCriteria.getClientIp() + ',' + dataFinal;
                    producerKafka.sendMessgae(kafkaSetting.getProducer().getTopic(), logData);
                }
            }
        } catch (Exception ex) {
            logger.error("enqueueKafka error", ex);
        }
    }

    /**
     * 解析神策数据并直接写入 events_flat 表
     *
     * @param queryCriteria 查询条件
     */
    @Override
    public void saveSensorsDataToClickHouse(QueryCriteria queryCriteria) {
        if (queryCriteria == null || queryCriteria.getData() == null) {
            logger.error("queryCriteria 或 data 为空");
            return;
        }

        try {
            // 解析 JSON 数组
            JsonNode array = objectMapper.readTree(queryCriteria.getData());

            List<EventsFlatLogBean> beanList = new ArrayList<>();

            if (array.isArray()) {
                for (JsonNode jsonNode : array) {
                    EventsFlatLogBean bean = SensorsDataMapper.mapToEventsFlat(jsonNode);
                    if (com.zcunsoft.util.SensorsDataMapper.isValid(bean)) {
                        beanList.add(bean);
                    }
                }
            } else {
                EventsFlatLogBean bean = SensorsDataMapper.mapToEventsFlat(array);
                if (SensorsDataMapper.isValid(bean)) {
                    beanList.add(bean);
                }
            }

            if (!beanList.isEmpty()) {
                doSaveToEventsFlat(beanList);
            }

        } catch (Exception ex) {
            logger.error("saveSensorsDataToClickHouse error", ex);
        }
    }

    /**
     * 批量写入 events_flat 表
     *
     * @param beanList EventsFlatLogBean 列表
     */
    private void doSaveToEventsFlat(List<EventsFlatLogBean> beanList) {
        String sql = "INSERT INTO events_flat (" +
                "_track_id, time, type, distinct_id, anonymous_id, event, _flush_time," +
                "lib_method, lib, lib_version, app_version," +
                "event_date, event_timestamp, event_name, user_pseudo_id, user_first_touch_timestamp, is_active_user," +
                "device_category, device_mobile_brand_name, device_mobile_model_name, device_mobile_marketing_name, " +
                "device_mobile_os_hardware_model, device_operating_system, device_operating_system_version, " +
                "device_language, device_time_zone_offset_seconds, device_is_limited_ad_tracking, " +
                "device_vendor_id, device_advertising_id, device_browser, device_browser_version," +
                "geo_city, geo_country, geo_continent, geo_region, geo_sub_continent, geo_metro," +
                "app_info_id, app_info_version, app_info_install_source, app_info_install_store, app_info_firebase_app_id," +
                "privacy_info_analytics_storage, privacy_info_ads_storage, privacy_info_uses_transient_token," +
                "is_first_day, os, os_version, manufacturer, model, brand, app_id, app_name, wifi, network_type, " +
                "screen_width, screen_height, timezone_offset, device_id," +
                "raw_event_params, raw_user_properties, raw_properties" +
                ") VALUES (" +
                "?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        clickHouseJdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement pst, int i) throws SQLException {
                com.zcunsoft.model.EventsFlatLogBean bean = beanList.get(i);

                int idx = 1;

                // 原始神策字段
                pst.setLong(idx++, nvlLong(bean.get_track_id()));
                pst.setLong(idx++, nvlLong(bean.getTime()));
                pst.setString(idx++, nvl(bean.getType()));
                pst.setString(idx++, nvl(bean.getDistinct_id()));
                pst.setString(idx++, nvl(bean.getAnonymous_id()));
                pst.setString(idx++, nvl(bean.getEvent()));
                pst.setLong(idx++, nvlLong(bean.get_flush_time()));

                // SDK 信息
                pst.setString(idx++, nvl(bean.getLib_method()));
                pst.setString(idx++, nvl(bean.getLib()));
                pst.setString(idx++, nvl(bean.getLib_version()));
                pst.setString(idx++, nvl(bean.getApp_version()));

                // Firebase 顶层字段
                pst.setString(idx++, nvl(bean.getEvent_date()));
                pst.setLong(idx++, nvlLong(bean.getEvent_timestamp()));
                pst.setString(idx++, nvl(bean.getEvent_name()));
                pst.setString(idx++, nvl(bean.getUser_pseudo_id()));
                pst.setLong(idx++, nvlLong(bean.getUser_first_touch_timestamp()));
                pst.setBoolean(idx++, nvlBoolean(bean.getIs_active_user()));

                // device 信息
                pst.setString(idx++, nvl(bean.getDevice_category()));
                pst.setString(idx++, nvl(bean.getDevice_mobile_brand_name()));
                pst.setString(idx++, nvl(bean.getDevice_mobile_model_name()));
                pst.setString(idx++, nvl(bean.getDevice_mobile_marketing_name()));
                pst.setString(idx++, nvl(bean.getDevice_mobile_os_hardware_model()));
                pst.setString(idx++, nvl(bean.getDevice_operating_system()));
                pst.setString(idx++, nvl(bean.getDevice_operating_system_version()));
                pst.setString(idx++, nvl(bean.getDevice_language()));
                pst.setInt(idx++, nvlInt(bean.getDevice_time_zone_offset_seconds()));
                pst.setString(idx++, nvl(bean.getDevice_is_limited_ad_tracking()));
                pst.setString(idx++, nvl(bean.getDevice_vendor_id()));
                pst.setString(idx++, nvl(bean.getDevice_advertising_id()));
                pst.setString(idx++, nvl(bean.getDevice_browser()));
                pst.setString(idx++, nvl(bean.getDevice_browser_version()));

                // geo 信息
                pst.setString(idx++, nvl(bean.getGeo_city()));
                pst.setString(idx++, nvl(bean.getGeo_country()));
                pst.setString(idx++, nvl(bean.getGeo_continent()));
                pst.setString(idx++, nvl(bean.getGeo_region()));
                pst.setString(idx++, nvl(bean.getGeo_sub_continent()));
                pst.setString(idx++, nvl(bean.getGeo_metro()));

                // app_info 信息
                pst.setString(idx++, nvl(bean.getApp_info_id()));
                pst.setString(idx++, nvl(bean.getApp_info_version()));
                pst.setString(idx++, nvl(bean.getApp_info_install_source()));
                pst.setString(idx++, nvl(bean.getApp_info_install_store()));
                pst.setString(idx++, nvl(bean.getApp_info_firebase_app_id()));

                // privacy_info 信息
                pst.setString(idx++, nvl(bean.getPrivacy_info_analytics_storage()));
                pst.setString(idx++, nvl(bean.getPrivacy_info_ads_storage()));
                pst.setString(idx++, nvl(bean.getPrivacy_info_uses_transient_token()));

                // 神策预置属性
                pst.setBoolean(idx++, nvlBoolean(bean.getIs_first_day()));
                pst.setString(idx++, nvl(bean.getOs()));
                pst.setString(idx++, nvl(bean.getOs_version()));
                pst.setString(idx++, nvl(bean.getManufacturer()));
                pst.setString(idx++, nvl(bean.getModel()));
                pst.setString(idx++, nvl(bean.getBrand()));
                pst.setString(idx++, nvl(bean.getApp_id()));
                pst.setString(idx++, nvl(bean.getApp_name()));
                pst.setBoolean(idx++, nvlBoolean(bean.getWifi()));
                pst.setString(idx++, nvl(bean.getNetwork_type()));
                pst.setInt(idx++, nvlInt(bean.getScreen_width()));
                pst.setInt(idx++, nvlInt(bean.getScreen_height()));
                pst.setInt(idx++, nvlInt(bean.getTimezone_offset()));
                pst.setString(idx++, nvl(bean.getDevice_id()));

                // 原始 JSON 字符串
                pst.setString(idx++, nvl(bean.getRaw_event_params()));
                pst.setString(idx++, nvl(bean.getRaw_user_properties()));
                pst.setString(idx++, nvl(bean.getRaw_properties()));
            }

            @Override
            public int getBatchSize() {
                return beanList.size();
            }
        });

        logger.info("成功写入 events_flat 表 {} 条记录", beanList.size());
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
