package com.cqx.cli.tool;

import java.util.Map;

/**
 * ITool
 *
 * @author chenqixu
 */
public interface ITool {
    void init(Map param) throws Exception;

    void exec() throws Exception;

    void close() throws Exception;

    String getType();

    String getDesc();

    String getHelp();
}
