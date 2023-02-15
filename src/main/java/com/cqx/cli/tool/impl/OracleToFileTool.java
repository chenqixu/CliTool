package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.bean.Count;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.jdbc.QueryResult;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import com.cqx.common.utils.thread.BaseCallableV1;
import com.cqx.common.utils.thread.ExecutorFactory;
import com.cqx.common.utils.thread.ExecutorFactoryV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OracleToFileTool
 *
 * @author chenqixu
 */
@ToolImpl
public class OracleToFileTool extends DataToFileTool {
    private static final Logger logger = LoggerFactory.getLogger(OracleToFileTool.class);
    private String tab_fields;
    private String tab_name;
    private String src_where;
    private int thread_num;
    private int fetchSize;
    private String file_name;
    private String file_split_str;
    private String file_line_end;
    private ExecutorFactoryV1 srcExecutor;

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        tab_fields = (String) param.get("tab_fields");
        tab_name = (String) param.get("tab_name");
        src_where = (String) param.get("src_where");
        file_name = (String) param.get("file_name");
        file_split_str = ParamUtil.setValDefault(param, "file_split_str", ",");
        file_line_end = ParamUtil.setValDefault(param, "file_line_end", "");
        thread_num = ParamUtil.setNumberValDefault(param, "thread_num", 1);
        fetchSize = ParamUtil.setNumberValDefault(param, "fetchSize", JDBCUtil.DEFAULT_FETCH_SIZE);
        int jdbc_maxactive = thread_num + 1;
        srcJdbcUtil = new JDBCUtil(srcBean, jdbc_maxactive, thread_num, thread_num);
        srcJdbcUtil.setFetchSize(fetchSize);
        srcExecutor = ExecutorFactoryV1.newInstance(thread_num);
    }

    @Override
    public void exec() throws Exception {
        TimeCostUtil exec = new TimeCostUtil();
        exec.start();
        //先查总记录数
        String count_sql = "select count(1) as cnt from " + tab_name + (src_where == null ? "" : " where " + src_where);
        logger.info("总记录数查询sql：{}", count_sql);
        List<Count> countList = srcJdbcUtil.executeQuery(count_sql, Count.class);
        //有记录数
        if (countList != null && countList.size() > 0) {
            int count = countList.get(0).getCnt();
            logger.info("查询总记录数：{}", count);
            //然后按thread_num进行分割处理
            List<SplitNum> splitNums = splitNum(count, thread_num);
            String splitSql = "select " + tab_fields + " from (select " + tab_fields + ",row_number() over(partition by 1 order by rowid) rn from " +
                    tab_name + (src_where == null ? "" : " where " + src_where) + ") t where t.rn between %s and %s";
            for (int i = 0; i < splitNums.size(); i++) {
                String _file_name = file_name + i + ".txt";
                SrcRunable srcRunable = new SrcRunable(splitNums.get(i), splitSql, _file_name);
                // 提交任务
                srcExecutor.submit(srcRunable);
            }
            // 等待
            srcExecutor.join();
        } else {
            logger.warn("查询总记录数为0，不需要同步。");
        }
        logger.info("处理完成，总耗时：{}毫秒", exec.stopAndGet());
    }

    @Override
    public void close() throws Exception {
        if (srcExecutor != null) srcExecutor.stop();
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "oracle_to_file";
    }

    @Override
    public String getDesc() {
        return "Save oracle data to local file of Concurrent.";
    }

    @Override
    public String getHelp() {
        return "开发示例：\n" +
                "tab_fields: \"org_id,amount,max_amount\"\n" +
                "tab_name: \"op_org_computing_res_test\"\n" +
                "src_where:\n" +
                "file_name: \"D:\\\\Document\\\\Workspaces\\\\Git\\\\FujianBI\\\\etl-jstorm\\\\nl-rt-jstorm-fujianbi-common\\\\target\\\\data.txt\"\n" +
                "thread_num: 2\n" +
                "dbbeans:\n" +
                "  - name: srcBean\n" +
                "    user_name: \"suyan\"\n" +
                "    pass_word: \"suyan\"\n" +
                "    tns: \"jdbc:mysql://10.1.8.200:3306/suyan_perf?useUnicode=true\"\n" +
                "    dbType: \"MYSQL\"\n";
    }

    /**
     * 计算每个并发下的起始和结束位置
     *
     * @param count
     * @param thread_num
     * @return
     */
    protected List<SplitNum> splitNum(int count, int thread_num) {
        List<SplitNum> splitNums = new ArrayList<>();
        int split_num = count / thread_num;
        logger.info("count：{}，thread_num：{}，split_num：{}", count, thread_num, split_num);
        int start = 1;
        int end;
        for (int i = 1; i <= thread_num; i++) {
            int last_num = 0;
            if (i == thread_num) {
                last_num = count - i * split_num;
            }
            if (start == 1) {
                end = i * split_num + last_num;
                SplitNum splitNum = new SplitNum(start, end);
                splitNums.add(splitNum);
                logger.info("【{}】splitNum：{}", i, splitNum);
                start = end + 1;
            } else {
                end = i * split_num + last_num;
                SplitNum splitNum = new SplitNum(start, end);
                splitNums.add(splitNum);
                logger.info("【{}】splitNum：{}", i, splitNum);
                start = end + 1;
            }
        }
        return splitNums;
    }

    /**
     * 内容特殊处理
     *
     * @param queryResults 数据
     * @return
     */
    @Override
    protected String getContent(List<QueryResult> queryResults) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (QueryResult queryResult : queryResults) {
            Object val = queryResult.getValue();
            if (val != null) {
                if (val instanceof Clob) {
                    Clob clob = (Clob) val;
                    sb.append(String.format("%s%s", clob.getSubString(1, (int) clob.length()), file_split_str));
                } else {
                    sb.append(String.format("%s%s", val, file_split_str));
                }
            } else {
                sb.append(file_split_str);
            }
        }
        if (sb.length() > file_split_str.length()) sb.delete(sb.length() - file_split_str.length(), sb.length());
        logger.debug("getContent：{}", sb.toString());
        return sb.toString();
    }

    static class SplitNum {
        int start;
        int end;

        public SplitNum(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        @Override
        public String toString() {
            return String.format("start：%s，end：%s", getStart(), getEnd());
        }
    }

    class SrcRunable extends BaseCallableV1 {
        SplitNum splitNum;
        String sqlMode;
        FileUtil fileUtil;
        String fileName;

        public SrcRunable(SplitNum splitNum, String sqlMode, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
            this.splitNum = splitNum;
            this.sqlMode = sqlMode;
            this.fileName = fileName;
            fileUtil = new FileUtil();
            fileUtil.createFile(fileName, "UTF-8", false);
            logger.info("保存的文件：{}，splitNum：{}，sqlMode：{}", fileName, splitNum, sqlMode);
        }

        @Override
        public void exec() throws Exception {
            final long[] write_cnt = {0};
            TimeCostUtil exec = new TimeCostUtil();
            exec.start();
            // 从源端查询数据，写入文件
            logger.info("从源端查询数据，开始写入文件");
            try {
                srcJdbcUtil.executeQuery(String.format(sqlMode, splitNum.getStart(), splitNum.getEnd()),
                        new JDBCUtil.IQueryResultCallBack() {
                            @Override
                            public void call(List<QueryResult> queryResults) throws Exception {
                                fileUtil.write(getContent(queryResults));
                                fileUtil.newline(file_line_end);
                                write_cnt[0]++;
                                if (write_cnt[0] % 10000 == 0) {
                                    logger.info("{} 已经写入行数：{}", fileName, write_cnt[0]);
                                }
                            }
                        });
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException("从源端查询数据，写入文件" + fileName + "发生异常。信息：" + e.getMessage(), e);
            }
            //停止线程
            stop();
            logger.info("从源端查询数据，写入文件{}完成，耗时：{}毫秒，总行数：{}", fileName, exec.stopAndGet(), write_cnt[0]);
        }

        @Override
        public void lastExec() throws Exception {
            if (fileUtil != null) fileUtil.closeWrite();
        }
    }
}
