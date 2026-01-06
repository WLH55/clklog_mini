package com.zcunsoft.services;

import com.zcunsoft.model.QueryCriteria;
import com.zcunsoft.model.Region;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface IReceiveService {
    void enqueueKafka(List<QueryCriteria> queryCriteriaList);

    Region analysisRegionFromIp(String clientIp);

    Region analysisRegionFromIpBaseOnIp2Loc(String clientIp);

    void loadCity();

    void loadProjectSetting();

    void extractLog(QueryCriteria queryCriteria, HttpServletRequest request);



    /**
     * 批量解析神策数据并写入 sensors_events 表
     *
     * @param queryCriteriaList 查询条件列表
     */
    void batchSaveSensorsDataToClickHouse(List<QueryCriteria> queryCriteriaList);
}
