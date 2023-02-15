package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.jdbc.QueryResult;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * DataToFileTool
 *
 * @author chenqixu
 */
@ToolImpl
public class DataToFileTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(DataToFileTool.class);

    protected JDBCUtil srcJdbcUtil;
    private String src_query_sql;
    private String tab_fields;
    private String tab_name;
    private String src_where;
    private FileUtil fileUtil;

    @Override
    public void init(Map param) throws Exception {
        fileUtil = new FileUtil();

        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        tab_fields = (String) param.get("tab_fields");
        tab_name = (String) param.get("tab_name");
        src_where = (String) param.get("src_where");
        srcJdbcUtil = new JDBCUtil(srcBean);
        src_query_sql = "select " + tab_fields + " from " + tab_name + (src_where == null ? "" : " where " + src_where);
        logger.info("源端查询sql：{}", src_query_sql);

        String file_name = (String) param.get("file_name");
        fileUtil.createFile(file_name, "UTF-8", false);
        logger.info("保存的文件：{}", file_name);
    }

    @Override
    public void exec() throws Exception {
        final long[] write_cnt = {0};
        TimeCostUtil exec = new TimeCostUtil();
        exec.start();
        // 从源端查询数据，写入文件
        logger.info("从源端查询数据，写入文件");
        try {
            srcJdbcUtil.executeQuery(src_query_sql, new JDBCUtil.IQueryResultCallBack() {

                @Override
                public void call(List<QueryResult> queryResults) throws Exception {
                    fileUtil.write(getContent(queryResults));
                    fileUtil.newline();
                    write_cnt[0]++;
                    if (write_cnt[0] % 10000 == 0) {
                        logger.info("已经写入行数：{}", write_cnt[0]);
                    }
                }
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException("从源端查询数据，写入文件发生异常。信息：" + e.getMessage(), e);
        }
        logger.info("从源端查询数据，写入文件完成，耗时：{}毫秒，总行数：{}", exec.stopAndGet(), write_cnt[0]);
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
        if (fileUtil != null) fileUtil.closeWrite();
    }

    @Override
    public String getType() {
        return "data_to_file";
    }

    @Override
    public String getDesc() {
        return "Save data to local file.";
    }

    @Override
    public String getHelp() {
        return "开发示例：\n" +
                "tab_fields: \"org_id,amount,max_amount\"\n" +
                "tab_name: \"op_org_computing_res_test\"\n" +
                "src_where:\n" +
                "file_name: \"D:\\\\Document\\\\Workspaces\\\\Git\\\\FujianBI\\\\etl-jstorm\\\\nl-rt-jstorm-fujianbi-common\\\\target\\\\data.txt\"\n" +
                "dbbeans:\n" +
                "  - name: srcBean\n" +
                "    user_name: \"suyan\"\n" +
                "    pass_word: \"suyan\"\n" +
                "    tns: \"jdbc:mysql://10.1.8.200:3306/suyan_perf?useUnicode=true\"\n" +
                "    dbType: \"MYSQL\"\n";
    }

    /**
     * 内容特殊处理
     *
     * @param queryResults
     * @return
     */
    protected String getContent(List<QueryResult> queryResults) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (QueryResult queryResult : queryResults) {
            Object val = queryResult.getValue();
            if (val != null) {
                int size = val.toString().indexOf("\"");
                if (size >= 0) {
                    val = val.toString().replaceAll("\"", "'");
                }
                sb.append(String.format("\"%s\",", val));
//                sb.append(String.format("%s|", val));
            } else {
                sb.append(",");
            }
        }
        if (sb.length() > 0) sb.delete(sb.length() - 1, sb.length());
        logger.debug("getContent：{}", sb.toString());
        return sb.toString();
    }
}
