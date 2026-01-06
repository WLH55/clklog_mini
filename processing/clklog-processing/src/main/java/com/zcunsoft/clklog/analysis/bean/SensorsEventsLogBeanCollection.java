package com.zcunsoft.clklog.analysis.bean;

import lombok.Data;

import java.util.List;

/**
 * 神策事件数据集合
 */
@Data
public class SensorsEventsLogBeanCollection {
    private List<SensorsEventsLogBean> data;
}
