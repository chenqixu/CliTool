package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.kafka.KafkaConsumerGRUtil;
import com.cqx.common.utils.kafka.OggRecord;
import com.cqx.common.utils.param.ParamUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * KafkaConsumerTool
 *
 * @author chenqixu
 */
@ToolImpl
public class KafkaConsumerTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTool.class);

    private KafkaConsumerGRUtil kafkaConsumerGRUtil;
    private long fetchTimeOut;
    private int printCnt;
    private String[] print_fields;
    private String where_field;
    private String where_value;
    private String op_type;
    private String mode;

    @Override
    public void init(Map param) throws Exception {
        String topic = (String) param.get("topic");//获取话题
        if (topic == null) throw new NullPointerException("话题不能为空！请配置参数：topic");
        kafkaConsumerGRUtil = new KafkaConsumerGRUtil(param);
        kafkaConsumerGRUtil.subscribe(topic);//订阅

        String where_case = (String) param.get("where_case");//获取查询条件
        if (where_case != null) {
            String[] where_case_array = where_case.split("=", -1);
            if (where_case_array.length == 2) {
                where_field = where_case_array[0];
                where_value = where_case_array[1];
                logger.info("查询条件：{}={}", where_field, where_value);
            }
        }

        String print_field = (String) param.get("print_field");//获取输出字段
        logger.info("输出字段：{}", print_field);
        if (print_field != null) {
            print_fields = print_field.split(",", -1);
        }

        mode = (String) param.get("mode");//模式
        if (mode == null) mode = "normal";
        logger.info("执行模式：{}", mode);

        op_type = (String) param.get("op_type");// 操作类型
        logger.info("操作类型：{}", op_type);

        fetchTimeOut = ParamUtil.setNumberValDefault(param, "kafkaconf.newland.fetch.timeout", 1000L);
        logger.info("kafka每次消费时长：{} 毫秒", fetchTimeOut);

        printCnt = ParamUtil.setNumberValDefault(param, "kafkaconf.newland.print.cnt", 5);
        logger.info("打印记录数：{}", printCnt);
    }

    /**
     * <pre>
     *     查询模式
     *
     *     queryOggBefore，查询ogg的before里的数据
     *     queryOggAfter，查询ogg的after里的数据
     *     normal 或者 不填，用于查询扁平化里的数据，扁平化后，字段和op_type是同级的
     *     queryNoAVRO，不使用avro方式
     * </pre>
     *
     * @throws Exception
     */
    @Override
    public void exec() throws Exception {
        switch (mode) {
            case "queryOggBefore":
                //查询ogg before里的数据
                query("before", true);
                break;
            case "queryOggAfter":
                //查询ogg after里的数据
                query("after", true);
                break;
            case "queryNoAVRO":
                // 不使用avro方式
                query(null, false, false);
                break;
            case "normal":
            default:
                query(null, false);
                break;
        }
    }

    private void query(String ogg_type, boolean isOgg) {
        query(ogg_type, isOgg, true);
    }

    /**
     * 消费kafka，进行查询
     *
     * @param ogg_type        ogg中的类型，是before还是after
     * @param isOgg           是否ogg
     * @param isGenericRecord 是否avro
     */
    private void query(String ogg_type, boolean isOgg, boolean isGenericRecord) {
        int real_cnt = 0;
        boolean fetch = true;
        while (fetch) {
            // 适配avro
            if (isGenericRecord) {
                List<ConsumerRecord<String, byte[]>> consumerRecords = kafkaConsumerGRUtil.pollHasConsumerRecord(fetchTimeOut);
                for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
                    int ret = -1;
                    OggRecord oggRecord = kafkaConsumerGRUtil.getValueTryToChangeSchema(consumerRecord.value());
                    if (oggRecord.isRecord()) {
                        GenericRecord where_record = oggRecord.getGenericRecord();
                        GenericRecord print_record = null;
                        // 扁平化和ogg，操作类型都在第一层
                        Object _op_type = where_record.get("op_type");
                        if (isOgg) {
                            // before和after赋予print_record
                            print_record = where_record;
                            // 查询ogg，获取before或者after里的数据
                            where_record = queryOgg(ogg_type, where_record);
//                            logger.info("where_record：{}", where_record);
                        }
                        if (where_record != null) {
                            ret = queryByTypeAndWhereField(consumerRecord, where_record, print_record, _op_type);
                        }
                    } else {
                        logger.warn("不是Record！【topic-partition】：{}，【offset】：{}，【key】：{}，【new String(value)】：{}"
                                , consumerRecord.topic() + "-" + consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key()
                                , new String(consumerRecord.value()));
                    }
                    if (ret > 0) {
                        real_cnt++;
                        if (real_cnt >= printCnt) {
                            fetch = false;
                            break;
                        }
                    }
                }
            }
            // 非avro
            else {
                List<ConsumerRecord<String, byte[]>> consumerRecords = kafkaConsumerGRUtil.pollHasConsumerRecord(fetchTimeOut);
                for (ConsumerRecord<String, byte[]> entry : consumerRecords) {
                    // 这里把value转换成String，方便查看
                    int ret = queryByTypeAndWhereField(entry, new String(entry.value()), null, null);
                    if (ret > 0) {
                        real_cnt++;
                        if (real_cnt >= printCnt) {
                            fetch = false;
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * 查询ogg，获取before或者after里的数据
     *
     * @param ogg_type
     * @param record
     * @return
     */
    private GenericRecord queryOgg(String ogg_type, Object record) {
        if (record instanceof GenericRecord && (ogg_type.contains("before") || ogg_type.contains("after"))) {
            GenericRecord genericRecord = (GenericRecord) record;
            // before和after是独立的Record
            // 从before或者after中获取Record
            Object data = genericRecord.get(ogg_type);
            if (data != null) {
                return (GenericRecord) data;
            }
        }
        return null;
    }

    /**
     * 根据op_type、where条件对record进行查询&过滤
     *
     * @param consumerRecord
     * @param where_record
     * @param print_record
     * @param _op_type
     * @return
     */
    private int queryByTypeAndWhereField(ConsumerRecord<String, byte[]> consumerRecord
            , Object where_record
            , Object print_record
            , Object _op_type) {
        if (where_record instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) where_record;
            String op = (_op_type != null ? _op_type.toString().toLowerCase() : "");
//            logger.info("op_type is null：{}", this.op_type==null);
            // 有操作类型，并且有设置op_type
            if (op.equals(this.op_type) && filter(genericRecord)) {
                // 根据配置进行字段输出
                if (print_record != null) {
                    print(consumerRecord, print_record);
                } else {
                    print(consumerRecord, genericRecord);
                }
                return 1;
            }
            // 没有设置op_type
            else if (this.op_type == null && filter(genericRecord)) {
                // 根据配置进行字段输出
                if (print_record != null) {
                    print(consumerRecord, print_record);
                } else {
                    print(consumerRecord, genericRecord);
                }
                return 1;
            }
        } else {
            // 非avro的查询，全字段输出
            print(consumerRecord, where_record);
            return 1;
        }
        return 0;
    }

    /**
     * where条件过滤
     *
     * @param genericRecord
     * @return
     */
    private boolean filter(GenericRecord genericRecord) {
//        logger.info("filter，where_field：{}，where_value：{}", where_field, where_value);
        if (where_field != null && where_value != null) {
//            logger.info("where_field：{}，genericRecord：{}", where_field, genericRecord);
            String field_value = genericRecord.get(where_field).toString();
            if (field_value.equals(where_value)) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * 全部打印或者根据过滤字段进行打印
     * <p>
     * 注意，ogg查询的时候不能设置输出字段
     *
     * @param consumerRecord
     * @param record
     */
    private void print(ConsumerRecord<String, byte[]> consumerRecord, Object record) {
        StringBuilder sb = new StringBuilder();
        if (record instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) record;
            if (print_fields != null && print_fields.length > 0) {
                for (String key : print_fields)
                    sb.append(genericRecord.get(key) != null ? genericRecord.get(key).toString() : "null").append("|");
                logger.info("【topic-partition】：{}，【offset】：{}，【key】：{}，【value】：{}"
                        , consumerRecord.topic() + "-" + consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), sb.toString());
            } else {
                logger.info("【topic-partition】：{}，【offset】：{}，【key】：{}，【value】：{}"
                        , consumerRecord.topic() + "-" + consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), record);
            }
        } else {
            logger.info("【topic-partition】：{}，【offset】：{}，【key】：{}，【value】：{}"
                    , consumerRecord.topic() + "-" + consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), record);
        }
    }

    @Override
    public void close() throws Exception {
        if (kafkaConsumerGRUtil != null) kafkaConsumerGRUtil.close();
    }

    @Override
    public String getType() {
        return "kafka_consumer";
    }

    @Override
    public String getDesc() {
        return "Consumption data From Kafka";
    }

    @Override
    public String getHelp() {
        return "开发参数参考：\n" +
                "  kafkaconf.bootstrap.servers: \"10.1.8.200:9092,10.1.8.201:9092,10.1.8.202:9092\"\n" +
                "  kafkaconf.key.deserializer: \"org.apache.kafka.common.serialization.StringDeserializer\"\n" +
                "  kafkaconf.value.deserializer: \"org.apache.kafka.common.serialization.ByteArrayDeserializer\"\n" +
                "  kafkaconf.security.protocol: \"SASL_PLAINTEXT\"\n" +
                "  kafkaconf.sasl.mechanism: \"PLAIN\"\n" +
                "  kafkaconf.group.id: \"throughput_jstorm\"\n" +
                "  kafkaconf.enable.auto.commit: \"true\"\n" +
                "  kafkaconf.fetch.min.bytes: \"52428800\"\n" +
                "  kafkaconf.max.poll.records: \"12000\"\n" +
                "  kafkaconf.newland.kafka_username: admin\n" +
                "  kafkaconf.newland.kafka_password: admin\n" +
                "  schema_url: \"http://10.1.8.203:19090/nl-edc-cct-sys-ms-dev/SchemaService/getSchema?t=\"";
    }
}
