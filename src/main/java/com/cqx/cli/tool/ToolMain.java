package com.cqx.cli.tool;

import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.util.TimeVarUtils;
import com.cqx.common.utils.config.YamlParser;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.system.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ToolMain
 *
 * @author chenqixu
 */
public class ToolMain {
    private static final Logger logger = LoggerFactory.getLogger(ToolMain.class);
    private static Map<String, ITool> iToolMap = new HashMap<>();

    static {
        try {
            ClassUtil<ToolImpl, ITool> classUtil = new ClassUtil<>();
            //扫描所有有ToolImpl注解的类
            Set<Class<?>> classSet = classUtil.getClassSet("com.cqx.cli.tool.impl", ToolImpl.class);
            for (Class<?> cls : classSet) {
                //构造
                ITool tool = classUtil.generate(cls);
                //加入map
                iToolMap.put(tool.getType(), tool);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static Map<String, ITool> getiToolMap() {
        return iToolMap;
    }

    public static ITool getiToolByName(String name) {
        return iToolMap.get(name);
    }

    public static void main(String[] args) throws Exception {
        // 第一个参数是类型
        // 第二个参数都是配置文件
        // 如果参数超出2个，那么必定成双成对出现，属于可替换配置文件中的参数
        String type = null;
        String conf = null;
        Map<String, Object> addParams = new HashMap<>();
        if (args.length == 1) {
            type = args[0];
        } else if (args.length == 2) {
            type = args[0];
            conf = args[1];
        } else if (args.length > 2 && args.length % 2 == 0) {// 判断是否成双成对
            type = args[0];
            conf = args[1];
            // 解析1，格式：--tab_fields org_id,amount,max_amount --dst_submit 2000
            // 解析2，针对List，格式：--list@key a --list@key b
            Map<String, List<String>> listMap = new HashMap<>();
            for (int i = 1; i < args.length / 2; i++) {
                String key = args[i * 2];
                String value = args[i * 2 + 1];
                if (!key.startsWith("--")) {
                    logger.info("{} is not startsWith '--' or not startsWith '--list@' , Try 'clitool help' for usage.", key);
                    System.exit(-1);
                } else if (key.startsWith("--list@")) {
                    String listKey = key.replace("--list@", "");
                    List<String> list = listMap.get(listKey);
                    if (list == null) {
                        list = new ArrayList<>();
                        listMap.put(listKey, list);
                    }
                    list.add(value);
                } else {
                    addParams.put(key.replace("--", ""), value);
                }
            }
            if (listMap.size() > 0) {
                addParams.putAll(listMap);
            }
            logger.info("addParams is {}", addParams);
        } else if (args.length > 2 && args.length % 2 != 0) {// 参数大于2且不是成双成对
            logger.info("The parameter is greater than 2 and is not paired.");
            for (int i = 0; i < args.length; i++) {
                logger.info("param[{}] is {}", i, args[i]);
            }
            logger.info("Try 'clitool help' for usage.");
            System.exit(-1);
        } else {
            logger.info("Try 'clitool help' for usage.");
            System.exit(-1);
        }
        ITool iTool = iToolMap.get(type);
        //1、type不对
        //2、conf没有的情况下有2种
        //2.1、help
        //2.2、打印帮助
        //3、conf有的情况下有4种
        //3.1、实际上是--help
        //3.2、是conf
        //3.3、第一个是help，第二个是命令
        //3.4、上面都不是
        int code;
        if (iTool == null) {
            code = 1;
        } else if (conf == null) {
            if (type.equals("help")) {
                code = 21;
            } else {
                code = 22;
            }
        } else {
            if (type.equals("help")) {
                iTool = iToolMap.get(conf);
                if (iTool == null) {
                    code = 1;
                } else {
                    code = 33;
                }
            } else if (conf.equals("--help")) {
                code = 31;
            } else if (FileUtil.isExists(conf)) {
                code = 32;
            } else {
                code = 34;
            }
        }
        switch (code) {
            case 1:
                logger.info(String.format("No such clitool tool: %s. See 'clitool help'.", type));
                break;
            case 22:
            case 34:
                logger.info(iTool.getDesc());
                logger.info("Try --help for usage instructions.");
                break;
            case 31:
            case 33:
                logger.info(iTool.getHelp());
                break;
            case 21:
            case 32:
                Map param = null;
                if (conf != null) {
                    param = YamlParser.builder().parserConfToMap(conf);
                }
                try {
                    if (param != null && addParams.size() > 0) {
                        param.putAll(addParams);
                    }
                    // 判断是否有task_id
                    if (param != null) {
                        String task_id = (String) param.get("task_id");
                        if (task_id != null && task_id.length() > 0) {
                            // 解析出时间，并计算时间偏移量，替换参数中所有的时间关键字
                            TimeVarUtils timeVarUtils = new TimeVarUtils();
                            timeVarUtils.parserTaskID(task_id, param);
                        }
                    }
                    iTool.init(param);
                    iTool.exec();
                } finally {
                    iTool.close();
                }
                break;
            default:
                break;
        }
    }
}
