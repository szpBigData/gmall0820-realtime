package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sunzhipeng
 * @create 2021-03-21 15:48
 */
@Slf4j
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //将数据落盘
        log.info(jsonStr);
        //将数据发送至Kafka ODS主题
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }

}
