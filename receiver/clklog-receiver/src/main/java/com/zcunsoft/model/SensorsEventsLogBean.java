package com.zcunsoft.model;

import lombok.Data;

import java.util.List;

/**
 * 神策事件模型，对应 ClickHouse sensors_events 表
 */
@Data
public class SensorsEventsLogBean {

    // ========== 基本事件信息 ==========
    private Long _track_id;
    private Long time;
    private String type;
    private String distinct_id;
    private String anonymous_id;
    private String event;
    private Long _flush_time;

    // ========== 用户身份信息 ==========
    private String identity_anonymous_id;
    private String identity_android_id;

    // ========== SDK 信息 ==========
    private String lib_method;
    private String lib;
    private String lib_version;
    private String app_version;
    private String lib_detail;

    // ========== 事件属性 - 自定义事件参数 ==========
    private String town_name;
    private String town_action;

    // ========== 事件属性 - 预置属性 ==========
    private Boolean is_first_day;
    private String os;
    private String os_version;
    private String manufacturer;
    private String model;
    private String brand;
    private Integer screen_width;
    private Integer screen_height;
    private Integer timezone_offset;
    private String app_id;
    private String app_name;
    private Boolean wifi;
    private String network_type;
    private List<String> lib_plugin_version;
    private String device_id;

    // ========== 原始JSON字段（用于调试和扩展） ==========
    private String raw_identities;
    private String raw_lib;
    private String raw_properties;
    private String raw_event_params;
}
