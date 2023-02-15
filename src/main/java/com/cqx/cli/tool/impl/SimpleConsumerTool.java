package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.kafka.KafkaConsumerUtil;
import com.cqx.common.utils.param.ParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

/**
 * @Description ：
 * @Author: ErIc
 */
@ToolImpl
public class SimpleConsumerTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerTool.class);
    private String topic;
    private KafkaConsumerUtil consumerUtil;
    private long fetchTimeOut;
    private int printCnt;
    private FileWriter writer;
    private String output;

    @Override
    public void init(Map param) throws Exception {
        topic = (String) param.get("topic");//获取话题
        if (topic == null) throw new NullPointerException("话题不能为空！请配置参数：topic");
        consumerUtil = new KafkaConsumerUtil(param);
        consumerUtil.subscribe(topic);

        fetchTimeOut = ParamUtil.setNumberValDefault(param, "kafkaconf.newland.fetch.timeout", 1L);
        printCnt = ParamUtil.setNumberValDefault(param, "kafkaconf.newland.print.cnt", 5);
        output = ParamUtil.setNumberValDefault(param, "record.output.path", "/home/edc_base/" + topic);
        File file = new File(output);
        if (file.exists()) {
            file.delete();
        }
        writer = new FileWriter(file);
    }

    @Override
    public void exec() throws Exception {
        int real_cnt = 0;
        boolean fetch = true;
        while (fetch) {
            List<String> records = consumerUtil.poll(fetchTimeOut);
            for (String record : records) {
                logger.info("record:{}", record);
                writer.write(record);
                real_cnt++;
                if (real_cnt >= printCnt) {
                    fetch = false;
                    break;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (consumerUtil != null) {
            consumerUtil.close();
        }
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public String getType() {
        return "simple_consumer";
    }

    @Override
    public String getDesc() {
        return "Consume string data From Kafka";
    }

    @Override
    public String getHelp() {
        return null;
    }


}
