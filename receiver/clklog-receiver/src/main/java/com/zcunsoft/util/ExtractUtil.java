package com.zcunsoft.util;


import com.fasterxml.jackson.databind.JsonNode;
import com.zcunsoft.model.PathRule;
import com.zcunsoft.model.ProjectSetting;
import com.zcunsoft.model.Region;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import nl.basjes.parse.useragent.AgentField;
import nl.basjes.parse.useragent.UserAgent;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeanUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractUtil {
    private static final Logger logger = LogManager.getLogger(ExtractUtil.class);

    /**
     * A类IP的正则模式.
     */
    private static final Pattern IPPatternClassA = Pattern.compile("^([0-9]{1,3})\\.\\*\\.\\*\\.\\*$");
    /**
     * B类IP的正则模式.
     */
    private static final Pattern IPPatternClassB = Pattern.compile("^([0-9]{1,3})\\.([0-9]{1,3})\\.\\*\\.\\*$");
    /**
     * C类IP的正则模式.
     */
    private static final Pattern IPPatternClassC = Pattern
            .compile("^([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.\\*$");
    /**
     * 单个IP的正则模式.
     */
    private static final Pattern IPPatternSingle = Pattern
            .compile("^([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})$");

    /**
     * The time format.
     */
    private static final ThreadLocal<DateFormat> yMdHmsFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));


    public static ProjectSetting getProjectSetting(String projectName, HashMap<String, ProjectSetting> htProjectSetting) {
        ProjectSetting projectSetting = htProjectSetting.get("clklog-global");
        if (htProjectSetting.containsKey(projectName)) {
            projectSetting = htProjectSetting.get(projectName);
        }
        return projectSetting;
    }

    public static String excludeParamFromUrl(String excludedParams, String rawurl) {
        if (StringUtils.isNotBlank(excludedParams)) {
            String[] urlPairArray = rawurl.split("((?=[?#/&])|(?<=[?#/&]))", -1);
            StringBuilder parsedUrl = new StringBuilder();
            HashMap<String, String> delimiterMap = new HashMap<>();
            delimiterMap.put("/", "/");
            delimiterMap.put("?", "?");
            delimiterMap.put("&", "&");
            delimiterMap.put("#", "#");

            String[] paramsList = excludedParams.split("\n");
            for (String urlPair : urlPairArray) {
                if (delimiterMap.containsKey(urlPair)) {
                    parsedUrl.append(urlPair);
                } else {
                    String parseUrlPair = urlPair;
                    for (String params : paramsList) {
                        if (parseUrlPair.contains(params + "=")) {
                            parseUrlPair = parseUrlPair.replaceAll("^" + params + "=[\\w\\W]+", params + "=");
                        } else {
                            parseUrlPair = parseUrlPair.replaceAll("[\\w\\W]+=" + params + "$", "=" + params);
                        }
                    }
                    parsedUrl.append(parseUrlPair);
                }
            }
            return parsedUrl.toString();
        } else {
            return rawurl;
        }
    }

    /**
     * 排除url的指定参数.
     *
     * @param projectSetting 项目编码
     * @param rawurl         url地址
     * @param withParam      排除参数后的url是否需要带上key
     * @return 排除参数后的url
     */
    public static String excludeParamFromUrl(ProjectSetting projectSetting, String rawurl, boolean withParam) {
        String excludedParams = "";
        if (projectSetting != null) {
            excludedParams = projectSetting.getExcludedUrlParams();
        }

        String parsedUrl = rawurl;
        try {
            if (StringUtils.isNotBlank(excludedParams)) {
                String[] urlPairArray = rawurl.split("((?=[?#/&])|(?<=[?#/&]))", -1);
                StringBuilder sbParsedUrl = new StringBuilder();
                HashMap<String, String> delimiterMap = new HashMap<>();
                delimiterMap.put("/", "/");
                delimiterMap.put("?", "?");
                delimiterMap.put("&", "&");
                delimiterMap.put("#", "#");

                String[] paramsList = excludedParams.split("\n");
                for (String urlPair : urlPairArray) {
                    if (delimiterMap.containsKey(urlPair)) {
                        if (withParam || (!"?".equalsIgnoreCase(urlPair) && !"&".equalsIgnoreCase(urlPair))) {
                            sbParsedUrl.append(urlPair);
                        }
                    } else {
                        String parseUrlPair = urlPair;
                        for (String params : paramsList) {
                            if (parseUrlPair.contains(params + "=")) {
                                if (withParam) {
                                    parseUrlPair = parseUrlPair.replaceAll("^" + params + "=[\\w\\W]+", params + "=");
                                } else {
                                    parseUrlPair = "";
                                }
                            } else if (parseUrlPair.contains("=" + params)) {
                                if (withParam) {
                                    parseUrlPair = parseUrlPair.replaceAll("[\\w\\W]+=" + params + "$", "=" + params);
                                } else {
                                    parseUrlPair = "";
                                }
                            } else {
                                parseUrlPair = parseUrlPair.replaceAll("[\\w\\W]+=" + params + "$", "=" + params);
                            }
                        }
                        sbParsedUrl.append(parseUrlPair);
                    }
                }
                parsedUrl = sbParsedUrl.toString();
            }

            parsedUrl = processPathRule(parsedUrl, projectSetting);
        } catch (Exception ex) {
            logger.error("excludeParamFromUrl error ", ex);
        }

        return parsedUrl;
    }

    /**
     * 解析路径清洗规则
     *
     * @param pathRuleContentList 路径清洗规则
     * @return {@link List }<{@link PathRule }>
     */
    public static List<PathRule> extractPathRule(String pathRuleContentList) {
        List<PathRule> pathRuleList = new ArrayList<>();
        try {
            if (StringUtils.isNotBlank(pathRuleContentList)) {
                String[] pathRuleContentArr = pathRuleContentList.split("\n", -1);
                for (String pathRuleContent : pathRuleContentArr) {
                    String[] arr = pathRuleContent.split(",", -1);
                    if (arr.length > 1) {
                        PathRule pathRule = new PathRule();
                        pathRule.setTarget(arr[0]);
                        pathRule.setValue(pathRuleContent.substring(arr[0].length() + 1));
                        pathRuleList.add(pathRule);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("extractUrlRule error ", ex);
        }

        return pathRuleList;
    }

    /**
     * 解析url的路径.
     *
     * @param rawUrl 原url
     * @return 路径
     */
    public static String parseUrlPath(ProjectSetting projectSetting, String rawUrl) {
        String path = File.separator;

        try {
            int index = rawUrl.lastIndexOf("#");
            if (index != -1) {
                int index2 = rawUrl.indexOf("?", index + 1);
                if (index2 != -1) {
                    path = rawUrl.substring(index, index2);

                } else {
                    path = rawUrl.substring(index);
                }
            } else {
                if (rawUrl.startsWith("http://") || rawUrl.startsWith("https://")) {
                    URL url = new URL(rawUrl);
                    path = url.getPath();
                } else {
                    int index2 = rawUrl.indexOf("?");
                    if (index2 != -1) {
                        path = rawUrl.substring(0, index2);
                    } else {
                        path = rawUrl;
                    }
                }
            }
            path = processPathRule(path, projectSetting);
        } catch (Exception ex) {
            logger.error("parse url_path error", ex);
        }
        if (!path.startsWith("/") && path.contains("/")) {
            path = "/" + path;
        }
        return path;
    }

    public static String processPathRule(String rawUriPath, ProjectSetting projectSetting) {
        String uriPath = rawUriPath;
        if (projectSetting != null && projectSetting.getPathRuleList() != null) {
            for (PathRule rule : projectSetting.getPathRuleList()) {
                uriPath = uriPath.replaceAll(rule.getValue(), rule.getTarget());
            }
        }
        return uriPath;
    }

    /**
     * 从url解析站内搜索词.
     *
     * @param searchwordKey 搜索词关键字
     * @param rawurl        原url
     * @return 站内搜索词
     */
    public static String getSearchwordFromUrl(String searchwordKey, String rawurl) {
        String[] urlPairArray = rawurl.split("((?=[?#/&])|(?<=[?#/&]))", -1);

        HashMap<String, String> delimiterMap = new HashMap<>();
        delimiterMap.put("/", "/");
        delimiterMap.put("?", "?");
        delimiterMap.put("&", "&");
        delimiterMap.put("#", "#");

        String searchword = "";
        List<String> searchKeyList = new ArrayList<>(Arrays.asList(searchwordKey.split(",")));

        for (String urlPair : urlPairArray) {
            if (!delimiterMap.containsKey(urlPair)) {
                String[] kv = urlPair.split("=", -1);
                if (kv.length == 2) {
                    if (searchKeyList.contains(kv[0])) {
                        searchword = kv[1];
                    } else if (searchKeyList.contains(kv[1])) {
                        searchword = kv[0];
                    }
                    if (!searchword.isEmpty()) {
                        try {
                            searchword = URLDecoder.decode(searchword, "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            logger.error("encoding error", e);
                        }
                        break;
                    }
                }
            }
        }
        return searchword;
    }

    /**
     * 判断UA是否包含指定UA
     *
     * @param excludedUa 指定UA,多个用\n分开
     * @param uaToCheck  需要检查的UA
     * @return true:包含，false:不包含
     */
    public static boolean checkIfUaContainsExcludedUa(String excludedUa, String uaToCheck) {
        boolean isContains = false;
        String[] uaList = excludedUa.split("\n");
        for (String ua : uaList) {
            Pattern pattern = Pattern.compile(ua);
            Matcher prevMatcher = pattern.matcher(uaToCheck);
            if (prevMatcher.find()) {
                isContains = true;
                break;
            }
        }
        return isContains;
    }

    /**
     * 判断IP是否在指定IP段内
     *
     * @param excludedIp 指定IP段,多个用\n分开
     * @param ipToCheck  需要检查的IP
     * @return true:在IP段内,false:不在IP段内
     */
    public static boolean checkIfIpInExcludedIpList(String excludedIp, String ipToCheck) {
        boolean isIn = false;
        String[] ipList = excludedIp.split("\n");
        for (String ip : ipList) {
            CIDRUtils subnetUtils = null;
            try {
                String tempIp = convertToCIDR(ip);
                subnetUtils = new CIDRUtils(tempIp);
            } catch (Exception e) {
                logger.error("CIDRUtils err {}", e.getMessage());
            }
            if (subnetUtils != null) {
                try {
                    if (subnetUtils.isInRange(ipToCheck)) {
                        isIn = true;
                        break;
                    }
                } catch (Exception e) {
                    logger.error("isInRange err {}", e.getMessage());
                }
            }
        }
        return isIn;
    }

    /**
     * 转换IP段为CIDR.
     *
     * @param value IP段
     * @return CIDR
     */
    private static String convertToCIDR(String value) {
        if (IPPatternClassA.matcher(value).matches()) {
            return value.replace('*', '0') + "/8";
        }
        if (IPPatternClassB.matcher(value).matches()) {
            return value.replace('*', '0') + "/16";
        }
        if (IPPatternClassC.matcher(value).matches()) {
            return value.replace('*', '0') + "/24";
        }
        if (IPPatternSingle.matcher(value).matches()) {
            return value + "/32";
        }
        return value;
    }

    /**
     * 地域信息中英文转换.
     *
     * @param region    地域信息
     * @param htForCity 城市中英文映射表
     * @return 地域信息
     */
    public static Region translateRegion(Region region, Map<String, String> htForCity) {
        Region translatedRegion = null;
        if (region != null) {
            translatedRegion = new Region();
            BeanUtils.copyProperties(region, translatedRegion);

            String country = region.getCountry();
            String province = region.getProvince();
            String city = region.getCity();

            if (StringUtils.isNotBlank(province)) {
                String provinceKey = country + "_" + province;
                String cityKey = country + "_" + province + "_" + city;
                if (htForCity.containsKey(cityKey)) {
                    city = htForCity.get(cityKey);
                }
                if (htForCity.containsKey(provinceKey)) {
                    province = htForCity.get(provinceKey);
                }
            }

            if (htForCity.containsKey(country)) {
                country = htForCity.get(country);
            }
            translatedRegion.setCountry(country);
            translatedRegion.setProvince(province);
            translatedRegion.setCity(city);
        }
        return translatedRegion;
    }
}
