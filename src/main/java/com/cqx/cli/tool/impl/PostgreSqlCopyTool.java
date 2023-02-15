package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.IJDBCUtilCall;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Map;

/**
 * COPY拷贝文件到数据库
 *
 * @author chenqixu
 */
@ToolImpl
public class PostgreSqlCopyTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSqlCopyTool.class);
    protected JDBCUtil srcJdbcUtil;
    private String copy_file_path;
    private String copy_file_key;
    private String copy_file_endwith;
    private String table_name;
    private String delimiter;
    private String quote;

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        srcJdbcUtil = new JDBCUtil(srcBean, false);
        copy_file_path = (String) param.get("copy_file_path");
        copy_file_key = (String) param.get("copy_file_key");
        copy_file_endwith = (String) param.get("copy_file_endwith");
        table_name = (String) param.get("table_name");
        delimiter = ParamUtil.setValDefault(param, "delimiter", ",");
        quote = ParamUtil.setValDefault(param, "quote", "");

        logger.info("【参数打印 | 开始】===================");
        logger.info("【数据库 | srcBean】{}", srcBean);
        logger.info("【表名 | table_name】{}", table_name);
        logger.info("【拷贝路径 | copy_file_path】{}", copy_file_path);
        logger.info("【拷贝文件关键字 | copy_file_key】{}", copy_file_key);
        logger.info("【拷贝文件后缀 | copy_file_endwith】{}", copy_file_endwith);
        logger.info("【拷贝文件内容分隔符 | delimiter】{}", delimiter);
        logger.info("【拷贝文件内容包围符 | quote】{}", quote);
        logger.info("【参数打印 | 完成】===================");
    }

    @Override
    public void exec() throws Exception {
        TimeCostUtil tc = new TimeCostUtil();
        tc.start();
        int file_cnt = 0;
        //扫描
        FileUtil fileUtils = new FileUtil();
        for (File file : fileUtils.listFiles(copy_file_path, copy_file_key, copy_file_endwith)) {
            file_cnt++;
            final String copy_file = file.getPath();
            srcJdbcUtil.getConnection(new IJDBCUtilCall.IConnCallBack() {
                @Override
                public void call(Connection conn) throws Exception {
                    TimeCostUtil exec = new TimeCostUtil();
                    exec.start();
                    FileInputStream fileInputStream = null;
                    try {
                        CopyManager copyManager = new CopyManager((BaseConnection) conn);
                        fileInputStream = new FileInputStream(copy_file);
                        String sql;
                        if (quote.length() > 0) {
                            sql = String.format("COPY %s FROM STDIN DELIMITER '%s' CSV QUOTE '%s' "
                                    , table_name, delimiter, quote);
                        } else {
                            sql = String.format("COPY %s FROM STDIN DELIMITER '%s' NULL '' ", table_name, delimiter);
                        }
                        logger.info("【导入命令】{}", sql);
                        copyManager.copyIn(sql, fileInputStream);
                    } finally {
                        if (fileInputStream != null) {
                            try {
                                fileInputStream.close();
                            } catch (IOException e) {
                                logger.error("文件关闭异常：" + copy_file + "，异常信息：" + e.getMessage(), e);
                            }
                        }
                    }
                    logger.info("文件：{}，写入表：{}，执行耗时：{}毫秒", copy_file, table_name, exec.stopAndGet());
                }
            });
        }
        logger.info("导入文件个数：{}，总耗时：{}毫秒", file_cnt, tc.stopAndGet());
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "postgresql_copy";
    }

    @Override
    public String getDesc() {
        return "File import ADB";
    }

    @Override
    public String getHelp() {
        return null;
    }
}
