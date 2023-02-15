package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.Utils;
import com.cqx.common.utils.cmd.ProcessBuilderFactory;
import com.cqx.common.utils.system.ArrayUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 脚本执行
 *
 * @author chenqixu
 */
@ToolImpl
public class ExecShellTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(ExecShellTool.class);
    private ProcessBuilderFactory processBuilderFactory;
    private List<String> cmds;

    @Override
    public void init(Map param) throws Exception {
        processBuilderFactory = new ProcessBuilderFactory(logger, "ExecShellTool");
        cmds = (List<String>) param.get("cmd");
        String fileEncoding = (String) param.get("file.encoding");
        logger.info("cmd：{}，fileEncoding：{}", cmds, fileEncoding);
        if (fileEncoding != null) System.setProperty("file.encoding", fileEncoding);
    }

    @Override
    public void exec() throws Exception {
        TimeCostUtil exec = new TimeCostUtil();
        String[] command;
        for (String cmd : cmds) {
            if (Utils.isWindow()) {
                command = ArrayUtil.arrayCopy(new String[]{"cmd.exe", "/c"}, cmd.split(" "));
            } else {
                command = new String[]{"/bin/sh", "-c", "cd /home/;" + cmd};
            }
            exec.start();
            int ret = processBuilderFactory.execCmd(command);
            if (ret != 0) throw new Exception(Arrays.asList(command) + " 执行异常！");
            logger.info("{} 执行完成，耗时：{}毫秒", Arrays.asList(command), exec.stopAndGet());
        }
    }

    @Override
    public void close() throws Exception {
        if (processBuilderFactory != null) processBuilderFactory.close();
    }

    @Override
    public String getType() {
        return "exec_shell";
    }

    @Override
    public String getDesc() {
        return "Execute shell script.";
    }

    @Override
    public String getHelp() {
        return "开发参考：\n" +
                "cmd: \"pwd\"";
    }
}
