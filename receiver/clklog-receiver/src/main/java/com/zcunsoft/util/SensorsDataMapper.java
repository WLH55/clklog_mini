package com.zcunsoft.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.zcunsoft.model.EventsFlatLogBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 神策数据映射工具类
 * 将神策 JSON 数据映射到 EventsFlatLogBean
 */
public class SensorsDataMapper {

    private static final Logger logger = LogManager.getLogger(SensorsDataMapper.class);

    /**
     * 将神策 JSON 数据映射到 EventsFlatLogBean
     *
     * @param jsonNode 神策 JSON 数据
     * @return EventsFlatLogBean
     */
    public static EventsFlatLogBean mapToEventsFlat(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        }

        EventsFlatLogBean bean = new EventsFlatLogBean();

        try {
            // ========== 原始神策字段 ==========
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

            // ========== SDK 信息（lib 对象展开）==========
            if (jsonNode.has("lib")) {
                JsonNode lib = jsonNode.get("lib");
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
            }

            // ========== properties 中的 Firebase 顶层字段 ==========
            if (jsonNode.has("properties")) {
                JsonNode properties = jsonNode.get("properties");

                // 保存完整的 properties 原始 JSON
                bean.setRaw_properties(properties.toString());

                // Firebase 顶层字段
                if (properties.has("event_date")) {
                    bean.setEvent_date(properties.get("event_date").asText());
                }
                if (properties.has("event_timestamp")) {
                    bean.setEvent_timestamp(properties.get("event_timestamp").asLong());
                }
                if (properties.has("event_name")) {
                    bean.setEvent_name(properties.get("event_name").asText());
                }
                if (properties.has("user_pseudo_id")) {
                    bean.setUser_pseudo_id(properties.get("user_pseudo_id").asText());
                }
                if (properties.has("user_first_touch_timestamp")) {
                    bean.setUser_first_touch_timestamp(properties.get("user_first_touch_timestamp").asLong());
                }
                if (properties.has("is_active_user")) {
                    bean.setIs_active_user(properties.get("is_active_user").asBoolean());
                }

                // ========== device 信息（展开）==========
                if (properties.has("device")) {
                    String deviceJson = properties.get("device").asText();
                    bean.setRaw_event_params(deviceJson); // event_params 原始 JSON（实际是 device）
                    try {
                        JsonNode deviceNode = new ObjectMapperUtil().readTree(deviceJson);
                        if (deviceNode.has("category")) {
                            bean.setDevice_category(deviceNode.get("category").asText());
                        }
                        if (deviceNode.has("mobile_brand_name")) {
                            bean.setDevice_mobile_brand_name(deviceNode.get("mobile_brand_name").asText());
                        }
                        if (deviceNode.has("mobile_model_name")) {
                            bean.setDevice_mobile_model_name(deviceNode.get("mobile_model_name").asText());
                        }
                        if (deviceNode.has("mobile_marketing_name")) {
                            bean.setDevice_mobile_marketing_name(deviceNode.get("mobile_marketing_name").asText());
                        }
                        if (deviceNode.has("mobile_os_hardware_model")) {
                            bean.setDevice_mobile_os_hardware_model(deviceNode.get("mobile_os_hardware_model").asText());
                        }
                        if (deviceNode.has("operating_system")) {
                            bean.setDevice_operating_system(deviceNode.get("operating_system").asText());
                        }
                        if (deviceNode.has("operating_system_version")) {
                            bean.setDevice_operating_system_version(deviceNode.get("operating_system_version").asText());
                        }
                        if (deviceNode.has("language")) {
                            bean.setDevice_language(deviceNode.get("language").asText());
                        }
                        if (deviceNode.has("time_zone_offset_seconds")) {
                            bean.setDevice_time_zone_offset_seconds(deviceNode.get("time_zone_offset_seconds").asInt());
                        }
                        if (deviceNode.has("is_limited_ad_tracking")) {
                            bean.setDevice_is_limited_ad_tracking(deviceNode.get("is_limited_ad_tracking").asText());
                        }
                        if (deviceNode.has("vendor_id")) {
                            bean.setDevice_vendor_id(deviceNode.get("vendor_id").asText());
                        }
                        if (deviceNode.has("advertising_id")) {
                            bean.setDevice_advertising_id(deviceNode.get("advertising_id").asText());
                        }
                        if (deviceNode.has("browser")) {
                            bean.setDevice_browser(deviceNode.get("browser").asText());
                        }
                        if (deviceNode.has("browser_version")) {
                            bean.setDevice_browser_version(deviceNode.get("browser_version").asText());
                        }
                    } catch (Exception e) {
                        logger.warn("解析 device JSON 失败: {}", deviceJson, e);
                    }
                }

                // ========== geo 信息（展开）==========
                if (properties.has("geo")) {
                    String geoJson = properties.get("geo").asText();
                    try {
                        JsonNode geoNode = new ObjectMapperUtil().readTree(geoJson);
                        if (geoNode.has("city")) {
                            bean.setGeo_city(geoNode.get("city").asText());
                        }
                        if (geoNode.has("country")) {
                            bean.setGeo_country(geoNode.get("country").asText());
                        }
                        if (geoNode.has("continent")) {
                            bean.setGeo_continent(geoNode.get("continent").asText());
                        }
                        if (geoNode.has("region")) {
                            bean.setGeo_region(geoNode.get("region").asText());
                        }
                        if (geoNode.has("sub_continent")) {
                            bean.setGeo_sub_continent(geoNode.get("sub_continent").asText());
                        }
                        if (geoNode.has("metro")) {
                            bean.setGeo_metro(geoNode.get("metro").asText());
                        }
                    } catch (Exception e) {
                        logger.warn("解析 geo JSON 失败: {}", geoJson, e);
                    }
                }

                // ========== app_info 信息（展开）==========
                if (properties.has("app_info")) {
                    JsonNode appInfo = properties.get("app_info");
                    if (appInfo.has("id")) {
                        bean.setApp_info_id(appInfo.get("id").asText());
                    }
                    if (appInfo.has("version")) {
                        bean.setApp_info_version(appInfo.get("version").asText());
                    }
                    if (appInfo.has("install_source")) {
                        bean.setApp_info_install_source(appInfo.get("install_source").asText());
                    }
                    if (appInfo.has("install_store")) {
                        bean.setApp_info_install_store(appInfo.get("install_store").asText());
                    }
                    if (appInfo.has("firebase_app_id")) {
                        bean.setApp_info_firebase_app_id(appInfo.get("firebase_app_id").asText());
                    }
                }

                // ========== privacy_info 信息（展开）==========
                if (properties.has("privacy_info")) {
                    JsonNode privacyInfo = properties.get("privacy_info");
                    if (privacyInfo.has("analytics_storage")) {
                        bean.setPrivacy_info_analytics_storage(privacyInfo.get("analytics_storage").asText());
                    }
                    if (privacyInfo.has("ads_storage")) {
                        bean.setPrivacy_info_ads_storage(privacyInfo.get("ads_storage").asText());
                    }
                    if (privacyInfo.has("uses_transient_token")) {
                        bean.setPrivacy_info_uses_transient_token(privacyInfo.get("uses_transient_token").asText());
                    }
                }

                // ========== 神策预置属性（用于兼容和扩展）==========
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
                if (properties.has("$screen_width")) {
                    bean.setScreen_width(properties.get("$screen_width").asInt());
                }
                if (properties.has("$screen_height")) {
                    bean.setScreen_height(properties.get("$screen_height").asInt());
                }
                if (properties.has("$timezone_offset")) {
                    bean.setTimezone_offset(properties.get("$timezone_offset").asInt());
                }
                if (properties.has("$device_id")) {
                    bean.setDevice_id(properties.get("$device_id").asText());
                }

                // ========== event_params 原始 JSON ==========
                if (properties.has("event_params")) {
                    bean.setRaw_event_params(properties.get("event_params").asText());
                }

                // ========== user_properties 原始 JSON（如果有）==========
                if (properties.has("user_properties")) {
                    bean.setRaw_user_properties(properties.get("user_properties").asText());
                }
            }

        } catch (Exception e) {
            logger.error("映射神策数据失败", e);
            return null;
        }

        return bean;
    }

    /**
     * 验证 EventsFlatLogBean 是否有效
     *
     * @param bean 待验证的 bean
     * @return true=有效, false=无效
     */
    public static boolean isValid(EventsFlatLogBean bean) {
        if (bean == null) {
            logger.error("EventsFlatLogBean 为 null");
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
