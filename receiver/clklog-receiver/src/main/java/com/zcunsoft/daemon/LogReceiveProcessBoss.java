package com.zcunsoft.daemon;

import com.zcunsoft.cfg.ReceiverSetting;
import com.zcunsoft.handlers.ConstsDataHolder;
import com.zcunsoft.model.QueryCriteria;
import com.zcunsoft.services.IReceiveService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Component
public class LogReceiveProcessBoss {
    private final Logger logger = LogManager.getLogger(this.getClass());
    ;

    @Resource
    private ConstsDataHolder constsDataHolder;

    @Resource
    private ReceiverSetting serverSettings;

    @Resource
    private IReceiveService ireceiveService;

    List<Thread> threadList = null;

    boolean running = false;

    @PostConstruct
    public void start() {
        threadList = new ArrayList<Thread>();
        if (serverSettings.getThreadCount() > 0) {
            running = true;
            for (int i = 0; i < serverSettings.getThreadCount(); i++) {
                int threadId = i;
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        work(threadId);
                    }
                }, "LogReceiveProcess-" + String.valueOf(i));

                thread.start();
                threadList.add(thread);
            }
        }
    }

    /**
     * 工作线程处理逻辑
     * 从队列消费数据，批量处理后写入 Kafka 或 ClickHouse
     * 触发条件：1. 满 1000 条  2. 超过 5 秒  3. 队列为空
     *
     * @param threadId 线程 ID
     */
    private void work(int threadId) {
        while (running) {
            BlockingQueue<QueryCriteria> queueForLog = constsDataHolder.getLogQueue();

            QueryCriteria log;
            try {
                // 阻塞等待第一条数据
                log = queueForLog.take();
            } catch (InterruptedException e) {
                return;
            }

            List<QueryCriteria> logList = new ArrayList<QueryCriteria>();
            logList.add(log);

            // 记录开始时间，用于时间触发
            long startTime = System.currentTimeMillis();
            // 从配置获取批量大小和时间触发间隔
            int batchSize = serverSettings.getBatchSize();
            long flushInterval = serverSettings.getFlushInterval();

            while (running) {
                try {
                    // 使用带超时的 poll，避免死循环
                    log = queueForLog.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);

                    if (log != null) {
                        logList.add(log);
                    }

                    // 检查触发条件
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    boolean shouldFlush = logList.size() >= batchSize ||  // 满批量大小
                            elapsedTime >= flushInterval ||                 // 超时
                            (log == null && logList.size() > 0);           // 队列为空且有数据

                    if (shouldFlush) {
                        handle(logList);
                        logList.clear();
                        startTime = System.currentTimeMillis();  // 重置计时器
                        break;  // 退出内层循环，重新阻塞等待
                    }

                } catch (InterruptedException e) {
                    // 队列空或中断，处理已有数据后退出
                    if (!logList.isEmpty()) {
                        handle(logList);
                        logList.clear();
                    }
                    return;
                } catch (Exception ex) {
                    logger.error("handle err ", ex);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

    /**
     * 处理批量数据
     * 根据 enable-simple-version 配置选择写入目标
     *
     * @param logList 待处理的数据列表
     */
    private void handle(List<QueryCriteria> logList) {
        if (serverSettings.isEnableSimpleVersion()) {
            // true → 写 ClickHouse (新版 sensors_events 表)
            ireceiveService.batchSaveSensorsDataToClickHouse(logList);
        } else {
            // false → 写 Kafka
            ireceiveService.enqueueKafka(logList);
        }
    }


    @PreDestroy
    public void stop() {
        running = false;
        for (Thread thread : threadList) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (logger.isInfoEnabled()) {
                logger.info(thread.getName() + " stopping...");
            }
        }
    }
}
