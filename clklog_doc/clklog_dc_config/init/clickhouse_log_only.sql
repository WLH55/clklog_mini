-- =====================================================
-- ClickHouse 表结构设计 - Firebase 埋点数据存储
-- 方案一：扁平化存储（推荐）
-- =====================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS firebase_analytics;

USE firebase_analytics;

-- =====================================================
-- 1. 主表：扁平化存储解析后的埋点事件
-- =====================================================
CREATE TABLE IF NOT EXISTS events_flat
(
    -- ========== 原始神策字段 ==========
    _track_id UInt64,
    time DateTime64(3),
    type String,
    distinct_id String,
    anonymous_id String,
    event String,
    _flush_time DateTime64(3),

    -- ========== SDK 信息（lib 对象展开）==========
    lib_method String COMMENT 'from $lib_method',
    lib String COMMENT 'from $lib',
    lib_version String COMMENT 'from $lib_version',
    app_version String COMMENT 'from $app_version',

    -- ========== Firebase 顶层字段 ==========
    event_date String COMMENT 'YYYYMMDD 格式',
    event_timestamp UInt64 COMMENT '微秒时间戳',
    event_name String,
    user_pseudo_id String,
    user_first_touch_timestamp UInt64,
    is_active_user Bool,

    -- ========== device 信息（展开）==========
    device_category String,
    device_mobile_brand_name String,
    device_mobile_model_name String,
    device_mobile_marketing_name String,
    device_mobile_os_hardware_model String,
    device_operating_system String,
    device_operating_system_version String,
    device_language String,
    device_time_zone_offset_seconds Int32,
    device_is_limited_ad_tracking String,
    device_vendor_id Nullable(String),
    device_advertising_id Nullable(String),
    device_browser Nullable(String),
    device_browser_version Nullable(String),

    -- ========== geo 信息（展开）==========
    geo_city String,
    geo_country String,
    geo_continent String,
    geo_region String,
    geo_sub_continent String,
    geo_metro String,

    -- ========== app_info 信息（展开）==========
    app_info_id String,
    app_info_version String,
    app_info_install_source Nullable(String),
    app_info_install_store Nullable(String),
    app_info_firebase_app_id String,

    -- ========== privacy_info 信息（展开）==========
    privacy_info_analytics_storage String,
    privacy_info_ads_storage String,
    privacy_info_uses_transient_token String,

    -- ========== 神策预置属性（用于兼容和扩展）==========
    is_first_day Bool COMMENT 'from $is_first_day',
    os String COMMENT 'from $os',
    os_version String COMMENT 'from $os_version',
    manufacturer String COMMENT 'from $manufacturer',
    model String COMMENT 'from $model',
    brand String COMMENT 'from $brand',
    app_id String COMMENT 'from $app_id',
    app_name String COMMENT 'from $app_name',
    wifi Bool COMMENT 'from $wifi',
    network_type String COMMENT 'from $network_type',
    screen_width UInt16 COMMENT 'from $screen_width',
    screen_height UInt16 COMMENT 'from $screen_height',
    timezone_offset Int32 COMMENT 'from $timezone_offset',
    device_id String COMMENT 'from $device_id',
    lib_plugin_version Array(String) COMMENT 'from $lib_plugin_version',

    -- ========== 保留原始 JSON 字符串（用于调试和重放）==========
    raw_event_params String COMMENT 'event_params 原始 JSON 字符串',
    raw_user_properties String COMMENT 'user_properties 原始 JSON 字符串',
    raw_properties String COMMENT '完整的 properties 原始 JSON 字符串'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(event_timestamp / 1000000))
ORDER BY (event_date, event_name, distinct_id, event_timestamp)
SETTINGS index_granularity = 8192;

-- 创建常用查询的索引
ALTER TABLE events_flat
ADD INDEX IF NOT EXISTS idx_event_name event_name TYPE bloom_filter GRANULARITY 1;

ALTER TABLE events_flat
ADD INDEX IF NOT EXISTS idx_user_pseudo_id user_pseudo_id TYPE bloom_filter GRANULARITY 1;

-- =====================================================
-- 2. event_params 子表（用于分析事件参数）
-- =====================================================
CREATE TABLE IF NOT EXISTS event_params
(
    event_id UInt64 COMMENT '关联 _track_id',
    event_date String,
    event_name String,
    distinct_id String COMMENT '关联 distinct_id',
    param_key String,
    string_value Nullable(String),
    int_value Nullable(Int64),
    float_value Nullable(Float64),
    double_value Nullable(Float64),
    insert_time DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDate(parseDateTimeBestEffort(event_date, 'yyyyMMdd')))
ORDER BY (event_date, event_name, param_key, event_id)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 3. user_properties 子表（用于分析用户属性）
-- =====================================================
CREATE TABLE IF NOT EXISTS user_properties
(
    user_pseudo_id String,
    property_key String,
    string_value Nullable(String),
    int_value Nullable(Int64),
    float_value Nullable(Float64),
    double_value Nullable(Float64),
    set_timestamp_micros UInt64,
    updated_at DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_pseudo_id, property_key)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 4. 物化视图：每日事件统计
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_name)
AS SELECT
    event_date,
    event_name,
    count() AS event_count,
    countIf(is_active_user) AS active_user_count
FROM events_flat
GROUP BY event_date, event_name;
