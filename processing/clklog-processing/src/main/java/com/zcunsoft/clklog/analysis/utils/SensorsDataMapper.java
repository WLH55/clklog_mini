package com.zcunsoft.clklog.analysis.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 神策数据映射工具类
 * 将神策 JSON 数据映射到 SensorsEventsLogBean
 */
public class SensorsDataMapper {

    private static final Logger logger = LoggerFactory.getLogger(SensorsDataMapper.class);

    /**
     * 将神策 JSON 数据映射到 SensorsEventsLogBean
     * 对应 sensors_events 表结构
     *
     * @param jsonNode 神策 JSON 数据
     * @return SensorsEventsLogBean
     */
    public static SensorsEventsLogBean mapToSensorsEvents(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        }

        SensorsEventsLogBean bean = new SensorsEventsLogBean();

        try {
            // ========== 基本事件信息 ==========
            if (jsonNode.has("_track_id")) {
                bean.set_track_id(jsonNode.get("_track_id").asLong());
            }
            if (jsonNode.has("time")) {
                bean.setTime(jsonNode.get("time").asLong());
            }
            if (jsonNode.has("type")) {
                bean.setType(jsonNode.get("type").asText());
            }
            if (jsonNode.has("distinct_id")) {
                bean.setDistinct_id(jsonNode.get("distinct_id").asText());
            }
            if (jsonNode.has("anonymous_id")) {
                bean.setAnonymous_id(jsonNode.get("anonymous_id").asText());
            }
            if (jsonNode.has("event")) {
                bean.setEvent(jsonNode.get("event").asText());
            }
            if (jsonNode.has("_flush_time")) {
                bean.set_flush_time(jsonNode.get("_flush_time").asLong());
            }

            // ========== 用户身份信息 ==========
            if (jsonNode.has("identities")) {
                JsonNode identities = jsonNode.get("identities");
                bean.setRaw_identities(identities.toString());
                if (identities.has("$identity_anonymous_id")) {
                    bean.setIdentity_anonymous_id(identities.get("$identity_anonymous_id").asText());
                }
                if (identities.has("$identity_android_id")) {
                    bean.setIdentity_android_id(identities.get("$identity_android_id").asText());
                }
            }

            // ========== SDK 信息 ==========
            if (jsonNode.has("lib")) {
                JsonNode lib = jsonNode.get("lib");
                bean.setRaw_lib(lib.toString());
                if (lib.has("$lib_method")) {
                    bean.setLib_method(lib.get("$lib_method").asText());
                }
                if (lib.has("$lib")) {
                    bean.setLib(lib.get("$lib").asText());
                }
                if (lib.has("$lib_version")) {
                    bean.setLib_version(lib.get("$lib_version").asText());
                }
                if (lib.has("$app_version")) {
                    bean.setApp_version(lib.get("$app_version").asText());
                }
                if (lib.has("$lib_detail")) {
                    bean.setLib_detail(lib.get("$lib_detail").asText());
                }
            }

            // ========== 事件属性 ==========
            if (jsonNode.has("properties")) {
                JsonNode properties = jsonNode.get("properties");
                bean.setRaw_properties(properties.toString());

                // 解析 event_params（JSON 字符串格式的数组）
                if (properties.has("event_params")) {
                    String eventParamsStr = properties.get("event_params").asText();
                    bean.setRaw_event_params(eventParamsStr);
                }

                // ========== 神策预置属性 ==========
                if (properties.has("$is_first_day")) {
                    bean.setIs_first_day(properties.get("$is_first_day").asBoolean());
                }
                if (properties.has("$os")) {
                    bean.setOs(properties.get("$os").asText());
                }
                if (properties.has("$os_version")) {
                    bean.setOs_version(properties.get("$os_version").asText());
                }
                if (properties.has("$manufacturer")) {
                    bean.setManufacturer(properties.get("$manufacturer").asText());
                }
                if (properties.has("$model")) {
                    bean.setModel(properties.get("$model").asText());
                }
                if (properties.has("$brand")) {
                    bean.setBrand(properties.get("$brand").asText());
                }
                if (properties.has("$screen_width")) {
                    bean.setScreen_width(properties.get("$screen_width").asInt());
                }
                if (properties.has("$screen_height")) {
                    bean.setScreen_height(properties.get("$screen_height").asInt());
                }
                if (properties.has("$timezone_offset")) {
                    bean.setTimezone_offset(properties.get("$timezone_offset").asInt());
                }
                if (properties.has("$app_id")) {
                    bean.setApp_id(properties.get("$app_id").asText());
                }
                if (properties.has("$app_name")) {
                    bean.setApp_name(properties.get("$app_name").asText());
                }
                if (properties.has("$wifi")) {
                    bean.setWifi(properties.get("$wifi").asBoolean());
                }
                if (properties.has("$network_type")) {
                    bean.setNetwork_type(properties.get("$network_type").asText());
                }
                if (properties.has("$lib_plugin_version")) {
                    JsonNode pluginVersionNode = properties.get("$lib_plugin_version");
                    if (pluginVersionNode.isArray()) {
                        List<String> versions = new ArrayList<>();
                        for (JsonNode version : pluginVersionNode) {
                            versions.add(version.asText());
                        }
                        bean.setLib_plugin_version(versions);
                    }
                }
                if (properties.has("$device_id")) {
                    bean.setDevice_id(properties.get("$device_id").asText());
                }
            }

        } catch (Exception e) {
            logger.error("映射神策数据到 SensorsEventsLogBean 失败", e);
            return null;
        }

        return bean;
    }

    /**
     * 验证 SensorsEventsLogBean 是否有效
     *
     * @param bean 待验证的 bean
     * @return true=有效, false=无效
     */
    public static boolean isValid(SensorsEventsLogBean bean) {
        if (bean == null) {
            logger.error("SensorsEventsLogBean 为 null");
            return false;
        }

        if (StringUtils.isBlank(bean.getDistinct_id())) {
            logger.error("distinct_id 为空");
            return false;
        }

        if (StringUtils.isBlank(bean.getEvent())) {
            logger.error("event 为空");
            return false;
        }

        if (bean.getTime() == null || bean.getTime() <= 0) {
            logger.error("time 无效: {}", bean.getTime());
            return false;
        }

        return true;
    }
}
