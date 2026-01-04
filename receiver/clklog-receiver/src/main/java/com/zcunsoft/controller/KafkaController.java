package com.zcunsoft.controller;


import com.zcunsoft.model.QueryCriteria;
import com.zcunsoft.services.IReceiveService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

@RestController
public class KafkaController {

    @Resource
    private IReceiveService receiveService;

    @RequestMapping(value = "api/gp", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseEntity<String> gp(QueryCriteria queryCriteria, HttpServletRequest request) {
        receiveService.extractLog(queryCriteria, request);

        // 最小可执行单元：解析完成后直接写入 ClickHouse
        receiveService.saveSensorsDataToClickHouse(queryCriteria);

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
