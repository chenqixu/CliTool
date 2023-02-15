package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.file.FileCount;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.jdbc.*;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件导入Oracle工具
 *
 * @author chenqixu
 */
@ToolImpl
public class FileToOracleTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(FileToOracleTool.class);
    protected JDBCUtil srcJdbcUtil;
    private String tab_fields;
    private String[] tab_fields_array;
    private String tab_name;
    private String file_path;
    private String file_name_keyword;
    private String file_endwith;
    private String file_split_str;
    private String file_line_end;
    private FileUtil fileUtils;
    private StringBuilder tab_fields_values = new StringBuilder();
    private LinkedHashMap<String, String> dstMetaMap;

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        tab_fields = (String) param.get("tab_fields");
        tab_name = (String) param.get("tab_name");
        file_path = (String) param.get("file_path");
        file_name_keyword = (String) param.get("file_name_keyword");
        file_endwith = (String) param.get("file_endwith");
        file_split_str = ParamUtil.setValDefault(param, "file_split_str", ",");
        file_line_end = ParamUtil.setValDefault(param, "file_line_end", "");
        srcJdbcUtil = new JDBCUtil(srcBean, false);
        fileUtils = FileUtil.builder();

        tab_fields_array = tab_fields.split(",", -1);
        int tab_fields_length = tab_fields_array.length;
        for (int i = 0; i < tab_fields_length; i++) {
            tab_fields_values.append("?");
            if (i < tab_fields_length - 1) {
                tab_fields_values.append(",");
            }
        }
        // 目标表所有字段类型映射关系
        dstMetaMap = srcJdbcUtil.getDstTableMetaData(tab_name, false);

        logger.info("【参数打印 | 开始】===================");
        logger.info("【数据库 | srcBean】{}", srcBean);
        logger.info("【表字段 | tab_fields】{}", tab_fields);
        logger.info("【表名 | tab_name】{}", tab_name);
        logger.info("【文件路径 | file_path】{}", file_path);
        logger.info("【文件关键字 | file_name_keyword】{}", file_name_keyword);
        logger.info("【文件后缀 | file_endwith】{}", file_endwith);
        logger.info("【文件内容分隔符 | file_split_str】{}", file_split_str);
        logger.info("【文件换行符附加内容(为了Clob而设计) | file_line_end】{}", file_line_end);
        logger.info("【参数打印 | 完成】===================");
    }

    @Override
    public boolean execHasRet() throws Exception {
        // 扫描文件，逐个读取，并拼接成sql执行
        for (File file : fileUtils.listFiles(file_path, file_name_keyword, file_endwith)) {
            fileUtils.setReader(file.getAbsolutePath());
            fileUtils.read(new FileCount() {
                StringBuilder sb = new StringBuilder();
                List<List<QueryResult>> allResult = new ArrayList<>();
                List<String> fieldsType;

                @Override
                public void run(String content) throws IOException {
                    if (content.endsWith(file_line_end)) {
                        sb.append(content.replace(file_line_end, ""));
                        String[] content_array = sb.toString().split(file_split_str, -1);
                        QueryResultFactory qrf = QueryResultFactory.getInstance();
                        for (int i = 0; i < tab_fields_array.length; i++) {
                            String fieldType = dstMetaMap.get(tab_fields_array[i]);
                            if ("oracle.jdbc.OracleClob".equals(fieldType)) fieldType = "java.sql.Clob";
                            String value = content_array[i];
                            if (value.length() == 0) value = null;
                            qrf.buildQR(tab_fields_array[i], fieldType, value);
                        }
                        qrf.toList();
                        allResult.addAll(qrf.getData());
                        if (fieldsType == null) {
                            fieldsType = qrf.getDstFieldsType();
                        }
                        sb.delete(0, sb.length());
                    } else {
                        sb.append(content);
                    }
                }

                @Override
                public void tearDown() throws IOException {
                    String sql = String.format("insert into %s(%s) values(%s)"
                            , tab_name, tab_fields, tab_fields_values);
                    if (allResult.size() > 0) {
                        TimeCostUtil tc = new TimeCostUtil();
                        tc.start();
                        try {
                            int ret = srcJdbcUtil.executeBatch(sql, allResult, fieldsType, true);
                            logger.info("数据大小：{}，执行耗时：{} 毫秒，执行结果：{}", allResult.size(), tc.stopAndGet(), ret);
                        } catch (Exception e) {
                            logger.error("执行入库SQL异常+" + e.getMessage(), e);
                        }
                    } else {
                        logger.warn("没有数据，读取异常，请检查数据内容中的【文件换行符】。");
                    }
                }
            });
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        if (fileUtils != null) fileUtils.closeRead();
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "file_to_oracle";
    }

    @Override
    public String getDesc() {
        return "文件导入Oracle";
    }

    @Override
    public String getHelp() {
        return "建设中……";
    }
}
