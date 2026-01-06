package com.zcunsoft.cfg;

import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties("receiver")
public class ReceiverSetting {

    private int threadCount = 2;

    private boolean enableSimpleVersion;

    /**
     * 批量处理大小
     */
    private int batchSize = 1000;

    /**
     * 时间触发间隔（毫秒）
     */
    private long flushInterval = 5000;

    private String resourcePath = "";

    private String[] accessControlAllowOriginPatterns;

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public boolean isEnableSimpleVersion() {
        return enableSimpleVersion;
    }

    public void setEnableSimpleVersion(boolean enableSimpleVersion) {
        this.enableSimpleVersion = enableSimpleVersion;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
    }

    public String[] getAccessControlAllowOriginPatterns() {
        return accessControlAllowOriginPatterns;
    }

    public void setAccessControlAllowOriginPatterns(String[] accessControlAllowOriginPatterns) {
        this.accessControlAllowOriginPatterns = accessControlAllowOriginPatterns;
    }
}
