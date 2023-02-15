package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.redis.RedisFactory;
import com.cqx.common.utils.redis.client.RedisClient;
import com.cqx.common.utils.redis.client.RedisPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * redis工具
 *
 * @author chenqixu
 */
@ToolImpl
public class RedisTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(RedisTool.class);
    // redis客户端
    private RedisClient redisClient;
    private String operation;
    private List<String> keyList;
    private boolean pipeline;

    @Override
    public boolean execHasRet() throws Exception {
        switch (operation) {
            case "get":
                if (pipeline) {
                    try (RedisPipeline redisPipeline = redisClient.openPipeline()) {
                        for (String key : keyList) {
                            redisPipeline.request_get(key);
                        }
                        List<Object> objectList = redisPipeline.get();
                        logger.info("size: {}，value: {}", objectList.size(), objectList);
                    }
                } else {
                    for (String key : keyList) {
                        String values = redisClient.get(key);
                        logger.info("{}，{}", key, values);
                    }
                }
                break;
            default:
                break;
        }
        return true;
    }

    @Override
    public void init(Map param) throws Exception {
        //----------------------------------------------
        // Redis
        //----------------------------------------------
        String redis_conf = (String) param.get("redis_conf");
        if (redis_conf == null || redis_conf.trim().length() == 0) {
            throw new RuntimeException("Redis配置redis_conf为空，请检查！");
        }
        logger.info("【参数】redis_conf：{}", redis_conf);

        // 6.2.7版本新增密码
        String password = (String) param.get("password");
        logger.info("【参数】password：{}", password);

        pipeline = ParamUtil.setValDefault(param, "pipeline", false);
        logger.info("【参数】pipeline：{}", pipeline);

        // 模式，默认集群模式
        int mode = ParamUtil.setValDefault(param, "mode", 1);
        logger.info("【参数】mode：{}", mode);

        redisClient = RedisFactory.builder()
                .setIp_ports(redis_conf)
                .setPassword(password)
                .setPipeline(pipeline)
                .setMode(mode)
                .build();

        operation = (String) param.get("operation");
        logger.info("【参数】operation：{}", operation);

        keyList = (List<String>) param.get("key");
        logger.info("【参数】key：{}", keyList);
    }

    @Override
    public void close() throws Exception {
        // 关闭Redis
        if (redisClient != null) redisClient.close();
    }

    @Override
    public String getType() {
        return "redis";
    }

    @Override
    public String getDesc() {
        return "Redis Tool.";
    }

    @Override
    public String getHelp() {
        return null;
    }
}
