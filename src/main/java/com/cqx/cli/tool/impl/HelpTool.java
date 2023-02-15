package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.ToolMain;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.util.TimeVarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HelpTool
 *
 * @author chenqixu
 */
@ToolImpl
public class HelpTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(HelpTool.class);

    @Override
    public void init(Map param) throws Exception {

    }

    @Override
    public void exec() throws Exception {
        logger.info("usage: clitool [task_id] COMMAND YAML [--param_name param_value]... [--list@param_name param_value]...");
        logger.info("");
        logger.info("Available COMMAND:");
        for (ITool value : ToolMain.getiToolMap().values()) {
            logger.info(String.format("  %-30s  %s", value.getType(), value.getDesc()));
        }
        logger.info("");
        logger.info("See 'clitool help COMMAND' for information on a specific command.");
        logger.info("");
        logger.info("--Time parameter description-----------------");
        TimeVarUtils.printlnDateHelp();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public String getType() {
        return "help";
    }

    @Override
    public String getDesc() {
        return "List available commands";
    }

    @Override
    public String getHelp() {
        return "See 'clitool help'.";
    }
}
