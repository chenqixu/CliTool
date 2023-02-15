package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.ToolMain;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.util.TimeVarUtils;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 有向无环图，执行有返回值，失败则不执行下一步
 *
 * @author chenqixu
 */
@ToolImpl
public class DAGExecTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(DAGExecTool.class);
    private List<IToolBean> iToolBeans = new ArrayList<>();

    @Override
    public void init(Map param) throws Exception {
        List<Map> multiple_execs = (List<Map>) param.get("multiple_exec");
        for (Map multiple_exec : multiple_execs) {
            String name = (String) multiple_exec.get("name");
            String type = (String) multiple_exec.get("type");
            Map _param = (Map) param.get(name);

            // 时间变量替换
            String task_id = (String) param.get("task_id");
            if (task_id != null && task_id.length() > 0) {
                // 解析出时间，并计算时间偏移量，替换参数中所有的时间关键字
                TimeVarUtils timeVarUtils = new TimeVarUtils();
                timeVarUtils.parserTaskID(task_id, _param);
            }

            ITool iTool = ToolMain.getiToolByName(type);
            if (iTool instanceof AbstractTool) {
                iToolBeans.add(new IToolBean(name, (AbstractTool) iTool, _param));
                logger.info("处理任务->解析参数，名称：{}，类型：{}，参数：{}", name, type, _param);
            }
        }
    }

    @Override
    public void exec() throws Exception {
        TimeCostUtil tc = new TimeCostUtil();
        tc.start();
        int exec_count = 0;
        int all_task_count = iToolBeans.size();
        try {
            for (IToolBean iToolBean : iToolBeans) {
                // 执行失败，停止执行
                if (!iToolBean.run()) {
                    break;
                }
                exec_count++;
            }
        } catch (Exception e) {
            if (!(e instanceof SQLException))
                logger.error(e.getMessage(), e);
            throw e;
        }
        logger.info("处理任务->本次任务处理完成，一共{}个任务，处理成功了{}个任务，总共执行了：{}毫秒",
                all_task_count, exec_count, tc.stopAndGet());
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public String getType() {
        return "dag_exec";
    }

    @Override
    public String getDesc() {
        return "Execute multiple tasks in sequence, and stop in case of failure.";
    }

    @Override
    public String getHelp() {
        return "开发参考：\n" +
                "multiple_exec:\n" +
                "  - name: oracle_to_file1\n" +
                "    type: oracle_to_file\n" +
                "  - name: exec_shell1\n" +
                "    type: exec_shell\n" +
                "\n" +
                "oracle_to_file1:\n" +
                "  tab_fields: \"home_city,preexit_time,is_change_equip,onu_type\"\n" +
                "  tab_name: \"rl_broadband_users\"\n" +
                "  src_where:\n" +
                "  file_name: \"D:\\\\Document\\\\Workspaces\\\\Git\\\\FujianBI\\\\etl-jstorm\\\\nl-rt-jstorm-fujianbi-tool\\\\target\\\\data\"\n" +
                "  thread_num: 1\n" +
                "  dbbeans:\n" +
                "    - name: srcBean\n" +
                "      user_name: \"frtbase\"\n" +
                "      pass_word: \"frtbase\"\n" +
                "      tns: \"jdbc:oracle:thin:@10.1.8.204:1521/orapri\"\n" +
                "      dbType: \"ORACLE\"\n" +
                "exec_shell1:\n" +
                "  cmd: \"dir c:\\\\\"\n" +
                "  file.encoding: \"GBK\"";
    }

    class IToolBean {
        String name;
        AbstractTool iTool;
        Map param;

        IToolBean(String name, AbstractTool iTool, Map param) {
            this.name = name;
            this.iTool = iTool;
            this.param = param;
        }

        boolean run() throws Exception {
            boolean ret = false;
            if (iTool != null) {
                logger.info("处理任务->{}开始", name);
                TimeCostUtil tc = new TimeCostUtil();
                tc.start();
                try {
                    iTool.init(param);
                    ret = iTool.execHasRet();
                } finally {
                    iTool.close();
                }
                logger.info("处理任务->{}运行完成，结果：{}，耗时：{}毫秒", name, ret, tc.stopAndGet());
            } else {
                ret = false;
                throw new NullPointerException("处理任务->" + name + " 未能初始化，请检查配置！");
            }
            return ret;
        }
    }
}
