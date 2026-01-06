package com.zcunsoft.clklog.analysis.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 神策数据提取工具类
 * 从 Kafka 消息格式解析并生成 SensorsEventsLogBean
 */
public class SensorsExtractUtil {

    private static final Logger logger = LoggerFactory.getLogger(SensorsExtractUtil.class);

    /**
     * 从 Kafka 消息行提取 SensorsEventsLogBean 列表
     * Kafka 消息格式: timestamp,crc,gzip,clientIp,{JSON}
     *
     * @param line Kafka 原始消息
     * @return SensorsEventsLogBean 列表
     */
    public static List<SensorsEventsLogBean> extractToSensorsEventsList(String line) {
        List<SensorsEventsLogBean> beanList = new ArrayList<>();
        try {
            // 解析 Kafka 消息头部格式
            String[] arr = line.split(",", -1);

            if (arr.length >= 5) {
                // 提取 JSON 部分（从第 5 个逗号后开始）
                String jsonContext = extractJsonContext(line, arr);

                StandardObjectMapperUtil objectMapper = new StandardObjectMapperUtil();
                JsonNode json = objectMapper.readTree(jsonContext);

                // 处理数组或单个对象
                if (json instanceof ArrayNode) {
                    ArrayNode arrayNode = (ArrayNode) json;
                    for (JsonNode item : arrayNode) {
                        SensorsEventsLogBean bean = SensorsDataMapper.mapToSensorsEvents(item);
                        if (SensorsDataMapper.isValid(bean)) {
                            beanList.add(bean);
                        }
                    }
                } else {
                    SensorsEventsLogBean bean = SensorsDataMapper.mapToSensorsEvents(json);
                    if (SensorsDataMapper.isValid(bean)) {
                        beanList.add(bean);
                    }
                }
            } else {
                logger.warn("Invalid Kafka message format (expected at least 5 fields): {}", line);
            }
        } catch (Exception e) {
            logger.error("Error parsing Kafka message: {}", line, e);
        }

        return beanList;
    }

    /**
     * 从 Kafka 消息中提取 JSON 内容
     * 格式: timestamp,crc,gzip,clientIp,{JSON}
     *
     * @param line 完整的 Kafka 消息
     * @param arr  分割后的数组
     * @return JSON 字符串
     */
    private static String extractJsonContext(String line, String[] arr) {
        // 计算前 4 个字段的总长度（包括 4 个逗号）
        int headerLength = 0;
        for (int i = 0; i < 4; i++) {
            headerLength += arr[i].length();
        }
        headerLength += 4; // 4 个逗号

        return line.substring(headerLength);
    }

    /**
     * 验证数据是否应该被过滤
     *
     * @param bean 待验证的 bean
     * @return true=应该过滤, false=不过滤
     */
    public static boolean shouldFilter(SensorsEventsLogBean bean) {
        if (bean == null) {
            return true;
        }

        // 过滤超时的数据（flush_time - time > 60秒）
        if (bean.get_flush_time() != null && bean.getTime() != null) {
            long diff = bean.get_flush_time() - bean.getTime();
            if (diff > 60000 || bean.get_flush_time() <= 0 || bean.getTime() <= 0) {
                logger.debug("Filtering data due to invalid time: flush_time={}, time={}",
                    bean.get_flush_time(), bean.getTime());
                return true;
            }
        }

        // 过滤匿名 ID 或 distinct_id 为空的数据
        if (StringUtils.isBlank(bean.getAnonymous_id()) && StringUtils.isBlank(bean.getDistinct_id())) {
            logger.debug("Filtering data due to empty anonymous_id and distinct_id");
            return true;
        }

        return false;
    }
}
