package com.cqx.cli.tool.impl;

import com.cqx.common.utils.kafka.SimpleClientConfiguration;
import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import kafka.admin.ConsumerGroupCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.util.Map;

/**
 * kafka消费组工具
 *
 * @author chenqixu
 */
@ToolImpl
public class KafkaGroupTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(KafkaGroupTool.class);
    private String bootstrap_servers;
    private String group_id;
    private String kafka_username;
    private String kafka_password;
    private String kafkaSecurityProtocol;
    private String consumergroups_properties;

    @Override
    public boolean execHasRet() throws Exception {
        String[] args = {
                "--bootstrap-server"
                , bootstrap_servers
                , "--group"
                , group_id
                , "--describe"
                , "--command-config"
                , consumergroups_properties
        };
        Configuration.setConfiguration(new SimpleClientConfiguration(
                kafka_username
                , kafka_password
                , kafkaSecurityProtocol));
        ConsumerGroupCommand.main(args);
        return true;
    }

    @Override
    public void init(Map param) throws Exception {
        bootstrap_servers = (String) param.get("bootstrap_servers");
        group_id = (String) param.get("group_id");
        kafka_username = (String) param.get("kafka_username");
        kafka_password = (String) param.get("kafka_password");
        kafkaSecurityProtocol = (String) param.get("sasl.mechanism");
        consumergroups_properties = (String) param.get("consumergroups_properties");
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public String getType() {
        return "kafka_group";
    }

    @Override
    public String getDesc() {
        return "Kafka Group Tool.";
    }

    @Override
    public String getHelp() {
        return null;
    }
}
