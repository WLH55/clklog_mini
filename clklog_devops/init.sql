CREATE TABLE IF NOT EXISTS sensors_events 
 ( 
     _track_id Int64, 
     time DateTime64(3) COMMENT '事件发生时间（毫秒）', 
     event_date Date DEFAULT toDate(time) COMMENT '事件日期（用于分区）', 
     type String COMMENT '事件类型，如track、profile_set等', 
     distinct_id String COMMENT '用户唯一标识', 
     anonymous_id String COMMENT '匿名用户ID', 
     event String COMMENT '事件名称', 
     _flush_time DateTime64(3) COMMENT '事件发送时间', 
     
     identity_anonymous_id String COMMENT '身份信息中的匿名ID', 
     identity_android_id String COMMENT '身份信息中的Android设备ID', 
     
     lib_method String COMMENT 'SDK集成方式，如code', 
     lib String COMMENT 'SDK类型，如Android', 
     lib_version String COMMENT 'SDK版本号', 
     app_version String COMMENT '应用版本号', 
     lib_detail String COMMENT 'SDK详细信息', 
     
     town_name String COMMENT '小镇名称', 
     town_action String COMMENT '小镇操作', 
     
     is_first_day Bool COMMENT '是否为首次访问日', 
     os String COMMENT '操作系统名称', 
     os_version String COMMENT '操作系统版本', 
     manufacturer String COMMENT '设备制造商', 
     model String COMMENT '设备型号', 
     brand String COMMENT '设备品牌', 
     screen_width UInt16 COMMENT '屏幕宽度（像素）', 
     screen_height UInt16 COMMENT '屏幕高度（像素）', 
     timezone_offset Int32 COMMENT '时区偏移量（秒）', 
     app_id String COMMENT '应用包名', 
     app_name String COMMENT '应用名称', 
     wifi Bool COMMENT '是否使用WiFi网络', 
     network_type String COMMENT '网络类型', 
     lib_plugin_version Array(String) COMMENT 'SDK插件版本列表', 
     device_id String COMMENT '设备ID', 
     
     raw_identities String COMMENT '完整的identities原始JSON', 
     raw_lib String COMMENT '完整的lib原始JSON', 
     raw_properties String COMMENT '完整的properties原始JSON', 
     raw_event_params String COMMENT '完整的event_params原始JSON',

 ) 
 ENGINE = ReplacingMergeTree() 
 PARTITION BY toYYYYMM(event_date) 
 ORDER BY (_track_id, event_date, event, distinct_id, time) 
 TTL event_date + INTERVAL 1 YEAR 
 SETTINGS index_granularity = 8192;
