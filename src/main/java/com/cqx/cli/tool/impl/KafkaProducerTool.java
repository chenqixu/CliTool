package com.cqx.cli.tool.impl;

import com.cqx.common.utils.kafka.KafkaProducerUtil;
import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * kafka生产者工具
 *
 * @author chenqixu
 */
@ToolImpl
public class KafkaProducerTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTool.class);

    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;
    private String topic;
    private String sendData;

    @Override
    public void init(Map param) throws Exception {
        topic = (String) param.get("topic");// 获取话题
        if (topic == null) throw new NullPointerException("话题不能为空！请配置参数：topic");

        kafkaProducerUtil = new KafkaProducerUtil<>(param);

        sendData = (String) param.get("send.data");// 获取发送的数据

        logger.info("要发送的话题：{}", topic);
        logger.info("要发送的测试数据：{}", sendData);
    }

    @Override
    public void exec() throws Exception {
        RecordMetadata recordMetadata = kafkaProducerUtil.send(topic, "test", sendData.getBytes()).get();
        logger.info("数据发送完成：{}", recordMetadata);
    }

    @Override
    public void close() throws Exception {
        if (kafkaProducerUtil != null) kafkaProducerUtil.close();
    }

    @Override
    public String getType() {
        return "kafka_producer";
    }

    @Override
    public String getDesc() {
        return "kafka生产者工具";
    }

    @Override
    public String getHelp() {
        return "开发参数参考：\n" +
                "  kafkaconf.bootstrap.servers: \"10.1.8.200:9092,10.1.8.201:9092,10.1.8.202:9092\"\n" +
                "  kafkaconf.key.serializer: \"org.apache.kafka.common.serialization.StringSerializer\"\n" +
                "  kafkaconf.value.serializer: \"org.apache.kafka.common.serialization.ByteArraySerializer\"\n" +
                "  kafkaconf.security.protocol: \"SASL_PLAINTEXT\"\n" +
                "  kafkaconf.sasl.mechanism: \"PLAIN\"\n" +
                "  kafkaconf.newland.kafka_username: 认证用户\n" +
                "  kafkaconf.newland.kafka_password: 认证用户的密码\n" +
                "  kafkaconf.acks: \"all\"\n" +
                "  kafkaconf.batch.size: \"131072\"\n" +
                "  kafkaconf.buffer.memory: \"67108864\"\n" +
                "  kafkaconf.linger.ms: \"100\"\n" +
                "  kafkaconf.max.request.size: \"10485760\"\n" +
                "  kafkaconf.retries: \"10\"\n" +
                "  kafkaconf.retry.backoff.ms: \"500\"\n" +
                "  topic: 话题\n" +
                "  send.data: 发送的测试数据";
    }
}
