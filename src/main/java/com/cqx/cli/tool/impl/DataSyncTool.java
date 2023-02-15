package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.bean.MergeBean;
import com.cqx.common.utils.jdbc.*;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.ArrayUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import com.cqx.common.utils.thread.BaseCallableV1;
import com.cqx.common.utils.thread.ExecutorFactoryV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DataSyncTool
 *
 * @author chenqixu
 */
@ToolImpl
public class DataSyncTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(DataSyncTool.class);
    // 默认类型，truncate table，再插入
    private static final String DEFAULT = "DEFAULT";
    // 写入合并，需要指定pk键
    private static final String MERGE_INTO_ONLY = "MERGE_INTO_ONLY";
    private static final String MERGE_INTO_UPDATE = "MERGE_INTO_UPDATE";
    // 先根据条件进行delete操作，然后插入
    private static final String DELETE = "DELETE";

    private MergeEnum mergeEnum;
    private JDBCUtil dstJdbcUtil;
    private JDBCUtil srcJdbcUtil;
    private String src_query_sql;
    private String dst_insert_sql;
    private String clean_sql;
    private String src_tab_name;
    private String dst_tab_name;
    private String tab_fields;
    private String src_where;
    private LinkedBlockingQueue<List<QueryResult>> queue;
    private SrcRunable srcRunable;
    private int dst_submit;
    private int dst_thread_num;
    private List<DstRunable> dstRunables;
    private String sync_type;
    private String pks;
    private List<String> defalut_insert_array = null;
    private String[] fields_array = {};
    private String[] pks_array = {};
    private String[] fields_type = {};
    private String[] pks_type = {};

    private ExecutorFactoryV1 srcExecutor;
    private ExecutorFactoryV1 dstExecutor;
    // 是否查询结束
    private AtomicBoolean queryEnd = new AtomicBoolean(false);

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        DBBean dstBean = paramsParserUtil.getBeanMap().get("dstBean");

        src_tab_name = (String) param.get("src_tab_name");
        dst_tab_name = ParamUtil.setValDefault(param, "dst_tab_name", src_tab_name);
        tab_fields = (String) param.get("tab_fields");
        src_where = (String) param.get("src_where");
        pks = (String) param.get("pks");
        dst_submit = ParamUtil.setNumberValDefault(param, "dst_submit", 2000);
        dst_thread_num = ParamUtil.setNumberValDefault(param, "dst_thread_num", 1);
        int jdbc_maxactive = dst_thread_num + 1;
        // 如果目标数据库是Oracle，需要把字段转大写，防止问题
        if (dstBean.getDbType().equals(DBType.ORACLE)) {
            tab_fields = tab_fields.toUpperCase();
            pks = pks.toUpperCase();
        }

        srcJdbcUtil = new JDBCUtil(srcBean);
        dstJdbcUtil = new JDBCUtil(dstBean, jdbc_maxactive, dst_thread_num, dst_thread_num);
        dstJdbcUtil.setBatchNum(dst_submit);

        src_query_sql = "select " + tab_fields + " from " + src_tab_name + (src_where == null ? "" : " where " + src_where);
        String[] tab_fields_arr = tab_fields.split(",", -1);
        StringBuilder prepare = new StringBuilder();
        for (int i = 0; i < tab_fields_arr.length; i++) {
            prepare.append("?,");
        }
        if (prepare.length() > 0) prepare.delete(prepare.length() - 1, prepare.length());
        dst_insert_sql = "insert into " + dst_tab_name + "(" + tab_fields + ") values(" + prepare + ")";

        queue = new LinkedBlockingQueue<>();
        dstRunables = new ArrayList<>();

        // 目标表所有字段类型映射关系
        LinkedHashMap<String, String> dstMetaMap = dstJdbcUtil.getDstTableMetaData(dst_tab_name);

        // 源端线程
        srcRunable = new SrcRunable();
        srcExecutor = ExecutorFactoryV1.newInstance(1);
        // 目标端线程
        dstExecutor = ExecutorFactoryV1.newInstance(dst_thread_num);
        for (int i = 0; i < dst_thread_num; i++) {
            DstRunable dstRunable = new DstRunable(i);
            dstRunables.add(dstRunable);
        }

        // 同步类型
        sync_type = ParamUtil.setValDefault(param, "sync_type", DEFAULT);
        switch (sync_type) {
            case DELETE:
                clean_sql = "delete from " + dst_tab_name + (src_where == null ? "" : " where " + src_where);
                defalut_insert_array = dstJdbcUtil.getFieldsTypeAsList(dstMetaMap, tab_fields_arr);
                logger.info("默认插入的数据字段类型：{}", defalut_insert_array);
                break;
            case MERGE_INTO_ONLY:
                mergeEnum = MergeEnum.valueOf(MERGE_INTO_ONLY);
            case MERGE_INTO_UPDATE:
                if (mergeEnum == null) {
                    mergeEnum = MergeEnum.valueOf(MERGE_INTO_UPDATE);
                }
                // 写入合并，[pks]主键不能为空
                if (pks == null || pks.trim().length() == 0) {
                    throw new NullPointerException("参数[pks]主键不能为空！");
                }
                // 整理出以下参数
                String[] tab_fields_array = tab_fields.split(",", -1);
                // pks_array
                pks_array = pks.split(",", -1);
                // fields_array
                fields_array = ArrayUtil.arrayRemove(tab_fields_array, pks_array);
                // fields_type
                fields_type = dstJdbcUtil.getFieldsTypeAsArray(dstMetaMap, fields_array);
                // pks_type
                pks_type = dstJdbcUtil.getFieldsTypeAsArray(dstMetaMap, pks_array);

                // 查询sql的顺序需要把pks调整到最后
                String fields = ArrayUtil.arrayToStr(fields_array, ",");
                String query = "select %s,%s from %s";
                src_query_sql = String.format(query, fields, pks, src_tab_name) + (src_where == null ? "" : " where " + src_where);
                dst_insert_sql = null;
                break;
            case DEFAULT:
            default:
                clean_sql = "truncate table " + dst_tab_name;
                defalut_insert_array = dstJdbcUtil.getFieldsTypeAsList(dstMetaMap, tab_fields_arr);
                logger.info("默认插入的数据字段类型：{}", defalut_insert_array);
                break;
        }

        logger.info("源端表名：{}", src_tab_name);
        logger.info("目标表所有字段类型映射关系：{}", dstMetaMap);
        logger.info("目标端表名：{}", dst_tab_name);
        logger.info("同步类型：{}", sync_type);
        logger.info("写入合并类型：{}", mergeEnum);
        logger.info("源端查询sql：{}", src_query_sql);
        logger.info("目标端数据清理sql：{}", clean_sql);
        logger.info("目标端写入sql：{}", dst_insert_sql);
    }

    @Override
    public void exec() throws Exception {
        TimeCostUtil total = new TimeCostUtil();
        total.start();
        // 写入合并不做清理
        if (mergeEnum == null) {
            // 目标端数据清理
            int clean_ret = dstJdbcUtil.executeUpdate(clean_sql);
            logger.info("目标端数据清理结果：{}", clean_ret);
//        if (clean_ret < 0) throw new Exception("清空目标端数据失败");
        }

        // 查询源端
        srcExecutor.submit(srcRunable);
        // 异步插入目标端
        for (DstRunable dst : dstRunables) {
            dstExecutor.submit(dst);
        }

        // 等待查询完成
        srcExecutor.join();
        // 等待异步插入完成
        dstExecutor.join();

        // 看看是否还有剩余队列需要处理
        if (queue.size() > 0) {
            TimeCostUtil exec = new TimeCostUtil();
            exec.start();
            List<List<QueryResult>> list = new ArrayList<>();
            List<QueryResult> queryResults;
            while ((queryResults = queue.poll()) != null) {
                list.add(queryResults);
            }
            try {
                int exec_size = list.size();
                logger.debug("剩余队列需要处理：{}", exec_size);
                int ret = insertInto(list);
                list.clear();
                logger.info("剩余数据：{}，执行结果：{}，耗时：{}", exec_size, ret, exec.stopAndGet());
            } catch (Exception e) {
                throw new RuntimeException("入库异常，异常信息：" + e.getMessage(), e);
            }
        }

        logger.info("数据同步完成，总耗时：{}毫秒", total.stopAndGet());
    }

    @Override
    public void close() throws Exception {
        if (srcExecutor != null) srcExecutor.stop();
        if (dstExecutor != null) dstExecutor.stop();
        if (srcJdbcUtil != null) srcJdbcUtil.close();
        if (dstJdbcUtil != null) dstJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "data_sync";
    }

    @Override
    public String getDesc() {
        return "Data synchronization from one database to another";
    }

    @Override
    public String getHelp() {
        return "参数说明：\n" +
                "tab_fields: \"表字段，逗号分隔\"\n" +
                "src_tab_name: \"源端表名\"\n" +
                "dst_tab_name: \"目标源端表名\"\n" +
                "src_where: \"where条件\"\n" +
                "dst_submit: 单次提交记录数，建议2000\n" +
                "dst_thread_num: 加载入库的并发，数据量小于100万建议1并发\n" +
                "sync_type: \"同步类型[DEFAULT|DELETE|MERGE_INTO_ONLY|MERGE_INTO_UPDATE]\"\n" +
                "pks: \"主键，逗号分隔，只有MERGE_INTO_ONLY|MERGE_INTO_UPDATE才需要\"\n" +
                "dbbeans:\n" +
                "  - name: srcBean[源数据库，名称固定]\n" +
                "    user_name: \"用户名\"\n" +
                "    pass_word: \"密码\"\n" +
                "    tns: \"连接串\"\n" +
                "    dbType: \"数据库类型[ORACLE|MYSQL|POSTGRESQL|OTHER]\"\n" +
                "  - name: dstBean[目标数据库，名称固定]\n" +
                "    user_name: 用户名\n" +
                "    pass_word: 密码\n" +
                "    tns: \"连接串\"\n" +
                "    dbType: \"数据库类型[ORACLE|MYSQL|POSTGRESQL|OTHER]\"";
    }

    /**
     * 数据写入
     *
     * @param list
     * @return
     * @throws Exception
     */
    private int insertInto(List<List<QueryResult>> list) throws Exception {
        if (mergeEnum != null) {
            List<MergeBean> mergeBeans = new ArrayList<>();
            for (List<QueryResult> queryResults : list) {
                mergeBeans.add(new MergeBean(queryResults));
            }
            List<Integer> rets = dstJdbcUtil.executeBatch(mergeBeans, dst_tab_name, fields_array, fields_type
                    , pks_array, pks_type, true, mergeEnum);
            int result = 0;
            for (int ret : rets) result += ret;
            return result;
        } else {
            return dstJdbcUtil.executeBatch(dst_insert_sql, list, defalut_insert_array);
        }
    }

    /**
     * 查询线程
     */
    class SrcRunable extends BaseCallableV1 {

        @Override
        public void exec() throws Exception {
            TimeCostUtil exec = new TimeCostUtil();
            exec.start();
            final long[] cnt = {0L};
            // 从源端查询数据，写入队列
            logger.info("从源端查询数据，写入队列");
            try {
                srcJdbcUtil.executeQuery(src_query_sql, new JDBCUtil.IQueryResultCallBack() {
                    @Override
                    public void call(List<QueryResult> queryResults) throws Exception {
                        queue.put(queryResults);
                        cnt[0]++;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException("从源端查询数据异常，异常信息：" + e.getMessage(), e);
            }
            logger.info("从源端查询数据，写入队列完成，记录数：{}，耗时：{}毫秒", cnt[0], exec.stopAndGet());
            // 停止
            stop();
            // 查询结束
            queryEnd.set(true);
        }
    }

    /**
     * 加载线程
     */
    class DstRunable extends BaseCallableV1 {
        List<List<QueryResult>> list;
        TimeCostUtil exec;
        int num;

        DstRunable(int num) {
            this.num = num;
            list = new ArrayList<>();
            exec = new TimeCostUtil();
            exec.start();
        }

        @Override
        public void exec() throws Exception {
            List<QueryResult> queryResults;
            if ((queryResults = queue.poll()) != null) {
                list.add(queryResults);
            } else {
                // 查询结束，并且队列没有值，就可以stop
                if (queryEnd.get()) {
                    stop();
                }
            }
            if (list.size() >= dst_submit) {
                try {
                    logger.debug("[num-{}] list.size等于{}，执行插入操作", num, dst_submit);
                    int ret = insertInto(list);
                    list.clear();
                    logger.info("[num-{}] 数据大小：{}，执行结果：{}，耗时：{}", num, dst_submit, ret, exec.stopAndGet());
                    exec.start();
                } catch (Exception e) {
                    throw new RuntimeException("入库异常，异常信息：" + e.getMessage(), e);
                }
            }
        }

        @Override
        public void lastExec() throws Exception {
            if (list.size() > 0) {
                try {
                    int exec_size = list.size();
                    logger.debug("[num-{}] 数据大小：{}，执行插入操作", num, exec_size);
                    int ret = insertInto(list);
                    list.clear();
                    logger.info("[num-{}] 数据大小：{}，执行结果：{}，耗时：{}", num, exec_size, ret, exec.stopAndGet());
                    exec.start();
                } catch (Exception e) {
                    throw new RuntimeException("入库异常，异常信息：" + e.getMessage(), e);
                }
            }
        }
    }
}
