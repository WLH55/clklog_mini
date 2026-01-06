package com.zcunsoft.clklog.analysis.enricher;

import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBean;
import com.zcunsoft.clklog.analysis.bean.SensorsEventsLogBeanCollection;
import com.zcunsoft.clklog.analysis.utils.SensorsExtractUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 日志数据丰富器
 * <p>
 * 功能：
 * 1. 解析 Kafka 消息为 SensorsEventsLogBean 对象
 */
public class LogEnricher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 构造函数
     */
    public LogEnricher() {
        logger.info("LogEnricher initialized successfully");
    }

    /**
     * 丰富单条日志数据
     *
     * @param line Kafka 原始消息
     * @return 丰富后的 SensorsEventsLogBean 集合
     */
    public SensorsEventsLogBeanCollection enrich(String line) {
        List<SensorsEventsLogBean> beanList = SensorsExtractUtil.extractToSensorsEventsList(line);

        SensorsEventsLogBeanCollection collection = new SensorsEventsLogBeanCollection();
        collection.setData(beanList);
        return collection;
    }

    /**
     * 关闭资源
     */
    public void close() {
        logger.info("LogEnricher closed");
    }
}
