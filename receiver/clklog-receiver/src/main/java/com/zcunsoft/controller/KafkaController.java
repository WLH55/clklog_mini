package com.zcunsoft.controller;


import com.zcunsoft.handlers.ConstsDataHolder;
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

    @Resource
    private ConstsDataHolder constsDataHolder;

    @RequestMapping(value = "api/gp", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseEntity<String> gp(QueryCriteria queryCriteria, HttpServletRequest request) {
        // 解析日志数据
        receiveService.extractLog(queryCriteria, request);

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
