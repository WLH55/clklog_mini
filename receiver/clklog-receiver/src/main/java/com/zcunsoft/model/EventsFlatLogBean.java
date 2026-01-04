package com.zcunsoft.model;

import lombok.Data;

/**
 * 神策事件扁平化模型，对应 ClickHouse events_flat 表
 */
@Data
public class EventsFlatLogBean {

    // ========== 原始神策字段 ==========

    private Long _track_id;
    private Long time;
    private String type;
    private String distinct_id;
    private String anonymous_id;
    private String event;
    private Long _flush_time;

    // ========== SDK 信息（lib 对象展开）==========

    private String lib_method;
    private String lib;
    private String lib_version;
    private String app_version;

    // ========== Firebase 顶层字段 ==========

    private String event_date;
    private Long event_timestamp;
    private String event_name;
    private String user_pseudo_id;
    private Long user_first_touch_timestamp;
    private Boolean is_active_user;

    // ========== device 信息（展开）==========

    private String device_category;
    private String device_mobile_brand_name;
    private String device_mobile_model_name;
    private String device_mobile_marketing_name;
    private String device_mobile_os_hardware_model;
    private String device_operating_system;
    private String device_operating_system_version;
    private String device_language;
    private Integer device_time_zone_offset_seconds;
    private String device_is_limited_ad_tracking;
    private String device_vendor_id;
    private String device_advertising_id;
    private String device_browser;
    private String device_browser_version;

    // ========== geo 信息（展开）==========

    private String geo_city;
    private String geo_country;
    private String geo_continent;
    private String geo_region;
    private String geo_sub_continent;
    private String geo_metro;

    // ========== app_info 信息（展开）==========

    private String app_info_id;
    private String app_info_version;
    private String app_info_install_source;
    private String app_info_install_store;
    private String app_info_firebase_app_id;

    // ========== privacy_info 信息（展开）==========

    private String privacy_info_analytics_storage;
    private String privacy_info_ads_storage;
    private String privacy_info_uses_transient_token;

    // ========== 神策预置属性（用于兼容和扩展）==========

    private Boolean is_first_day;
    private String os;
    private String os_version;
    private String manufacturer;
    private String model;
    private String brand;
    private String app_id;
    private String app_name;
    private Boolean wifi;
    private String network_type;
    private Integer screen_width;
    private Integer screen_height;
    private Integer timezone_offset;
    private String device_id;

    // ========== 保留原始 JSON 字符串（用于调试和重放）==========

    private String raw_event_params;
    private String raw_user_properties;
    private String raw_properties;
}
