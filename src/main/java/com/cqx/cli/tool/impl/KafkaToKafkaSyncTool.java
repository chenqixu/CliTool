package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.kafka.KafkaConsumerGRUtil;
import com.cqx.common.utils.kafka.KafkaProducerUtil;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 消费kafka话题数据，同步到另一个kafka话题
 *
 * @author chenqixu
 */
@ToolImpl
public class KafkaToKafkaSyncTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToKafkaSyncTool.class);
    // kafka消费工具
    private KafkaConsumerGRUtil kafkaConsumerGRUtil;
    // kafka生产工具
    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;
    private String read_topic;
    private String write_topic;
    private int sync_size;
    private TimeCostUtil tc = new TimeCostUtil();

    @Override
    public void init(Map param) throws Exception {
        // 获取数据话题
        read_topic = (String) param.get("read_topic");
        if (read_topic == null) throw new NullPointerException("数据话题不能为空！请配置参数：read_topic");
        Map source_kafka = (Map) param.get("source_kafka");
        if (source_kafka == null) throw new NullPointerException("数据话题集群参数为空！请配置参数：source_kafka");
        // 这里强制自动提交为true
        source_kafka.put("kafkaconf.enable.auto.commit", "true");
        kafkaConsumerGRUtil = new KafkaConsumerGRUtil(source_kafka);
        //订阅
        kafkaConsumerGRUtil.subscribe(read_topic);

        // 获取写入话题
        write_topic = (String) param.get("write_topic");
        if (write_topic == null) throw new NullPointerException("写入话题不能为空！请配置参数：write_topic");
        Map dist_kafka = (Map) param.get("dist_kafka");
        if (dist_kafka == null) throw new NullPointerException("写入话题集群参数为空！请配置参数：dist_kafka");
        kafkaProducerUtil = new KafkaProducerUtil<>(dist_kafka);

        // 获取同步大小
        sync_size = ParamUtil.setValDefault(param, "sync_size", 1);

        logger.info("【参数打印 | 开始】===================");
        logger.info("【数据话题 | read_topic】{}", read_topic);
        logger.info("【写入话题 | write_topic】{}", write_topic);
        logger.info("【同步大小 | sync_size】{}", sync_size);
        logger.info("【参数打印 | 结束】===================");
    }

    @Override
    public boolean execHasRet() throws Exception {
        int pollSize = 0;
        int nullSize = 0;
        tc.start();
        // 达到sync_size上限，或者空跑30次就结束
        while (pollSize < sync_size || nullSize < 30) {
            List<byte[]> datas = kafkaConsumerGRUtil.poll(1000L);
            if (datas != null && datas.size() > 0) {
                for (byte[] data : datas) {
                    kafkaProducerUtil.send(write_topic, data);
                    pollSize++;
                }
                logger.info("从数据话题：{}，消费到：{}条记录，写入生产话题：{}，耗时：{} ms"
                        , read_topic, datas.size(), write_topic, tc.stopAndGet());
            }
            // 避免cpu空跑
            SleepUtil.sleepMilliSecond(1);
            // 空跑计数
            nullSize++;
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        logger.info("资源释放……");
        if (kafkaConsumerGRUtil != null) kafkaConsumerGRUtil.close();
        if (kafkaProducerUtil != null) kafkaProducerUtil.close();
    }

    @Override
    public String getType() {
        return "kafka_to_kafka";
    }

    @Override
    public String getDesc() {
        return "消费kafka话题数据，同步到另一个kafka话题";
    }

    @Override
    public String getHelp() {
        return "达到sync_size上限，或者空跑30次就结束（约等于30秒）";
    }
}
