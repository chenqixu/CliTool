package com.cqx.cli.tool.impl;

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
 * 按顺序执行多个任务
 *
 * @author chenqixu
 */
@ToolImpl
public class MultipleExecTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(MultipleExecTool.class);
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

            iToolBeans.add(new IToolBean(name, ToolMain.getiToolByName(type), _param));
            logger.info("处理任务->解析参数，名称：{}，类型：{}，参数：{}", name, type, _param);
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
                iToolBean.run();
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
        return "multiple_exec";
    }

    @Override
    public String getDesc() {
        return "Perform multiple tasks in sequence.";
    }

    @Override
    public String getHelp() {
        return "参数说明：\n" +
                "multiple_exec:\n" +
                "  - name: 名称，业务说明简写\n" +
                "    type: 类型，需要符合工具说明里的[COMMAND]\n" +
                "  - name: 名称……\n" +
                "    type: 类型……\n" +
                "\n" +
                "名称\n" +
                "  对应的参数\n" +
                "名称……\n" +
                "  对应的参数……";
    }

    class IToolBean {
        String name;
        ITool iTool;
        Map param;

        IToolBean(String name, ITool iTool, Map param) {
            this.name = name;
            this.iTool = iTool;
            this.param = param;
        }

        void run() throws Exception {
            if (iTool != null) {
                logger.info("处理任务->{}开始", name);
                TimeCostUtil tc = new TimeCostUtil();
                tc.start();
                try {
                    iTool.init(param);
                    iTool.exec();
                } finally {
                    iTool.close();
                }
                logger.info("处理任务->{}运行完成，耗时：{}毫秒", name, tc.stopAndGet());
            } else {
                throw new NullPointerException("处理任务->" + name + " 未能初始化，请检查配置！");
            }
        }
    }
}
