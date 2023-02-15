package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.param.ParamUtil;
import kafka.admin.AclCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * kafka权限工具
 *
 * @author chenqixu
 */
@ToolImpl
public class KafkaAclTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(KafkaAclTool.class);
    private final String TOPIC_MODEL = "TOPIC_MODEL";
    private final String GROUP_MODEL = "GROUP_MODEL";
    private String mode;
    private String zookeeper;
    private String topic;
    private String group;

    @Override
    public void init(Map param) throws Exception {
        // 默认是查询话题权限
        mode = ParamUtil.setValDefault(param, "mode", TOPIC_MODEL);
        zookeeper = (String) param.get("zookeeper");
        if (mode.equals(TOPIC_MODEL)) {
            topic = (String) param.get("topic");
        } else if (mode.equals(GROUP_MODEL)) {
            group = (String) param.get("group");
        }
    }

    @Override
    public boolean execHasRet() throws Exception {
        if (mode.equals(TOPIC_MODEL)) {
            String[] args = {
                    "--authorizer-properties"
                    , "zookeeper.connect=" + zookeeper
                    , "--list"
                    , "--topic"
                    , topic
            };
            AclCommand.main(args);
            return true;
        } else if (mode.equals(GROUP_MODEL)) {
            String[] args = {
                    "--authorizer-properties"
                    , "zookeeper.connect=" + zookeeper
                    , "--list"
                    , "--group"
                    , group
            };
            AclCommand.main(args);
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public String getType() {
        return "kafka_acl";
    }

    @Override
    public String getDesc() {
        return "kafka权限工具";
    }

    @Override
    public String getHelp() {
        return "开发参数参考：\n" +
                "  zookeeper: \"edc-mqc-01:2181\"\n" +
                "  #模式\n" +
                "  #TOPIC_MODEL，查看话题权限，默认\n" +
                "  #GROUP_MODEL，查看消费组权限\n" +
                "  mode: \"TOPIC_MODEL\"\n" +
                "  topic: \"USER_PRODUCT\"\n" +
                "  group: \"new_consumer_api\"";
    }
}
