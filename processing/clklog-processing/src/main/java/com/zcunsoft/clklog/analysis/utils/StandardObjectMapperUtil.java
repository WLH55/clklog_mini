package com.zcunsoft.clklog.analysis.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 标准 Jackson ObjectMapper 工具类
 * 用于解析 JSON 数据（非 Flink shaded 版本）
 */
public class StandardObjectMapperUtil extends ObjectMapper {
    private static final long serialVersionUID = 1L;

    public StandardObjectMapperUtil() {
        super();
        configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
