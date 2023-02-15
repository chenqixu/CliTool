package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * JDBCTool
 *
 * @author chenqixu
 */
@ToolImpl
public class JDBCTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(JDBCTool.class);
    protected JDBCUtil srcJdbcUtil;
    private List<String> sqls;
    private boolean auto_commit;

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        srcJdbcUtil = new JDBCUtil(srcBean);
        sqls = (List<String>) param.get("sql");
        auto_commit = ParamUtil.setValDefault(param, "auto_commit", true);
        logger.info("srcBean：{}，auto_commit：{}，执行sql：{}", srcBean, auto_commit, sqls);
    }

    @Override
    public boolean execHasRet() throws Exception {
        TimeCostUtil exec = new TimeCostUtil();
        for (String sql : sqls) {
            exec.start();
            int ret;
            if (sql.trim().toLowerCase().startsWith("select ")) {
                ret = srcJdbcUtil.executeQuery(sql).size();
            } else {
                ret = srcJdbcUtil.executeUpdate(sql, auto_commit);
            }
            logger.info("执行sql：{}，执行结果：{}，执行耗时：{}毫秒", sql, ret, exec.stopAndGet());
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public String getDesc() {
        return "SQL execution through JDBC.";
    }

    @Override
    public String getHelp() {
        return "参数说明：\n" +
                "sql:\n" +
                "  - \"执行SQL语句\"\n" +
                "  - \"执行SQL语句……\"\n" +
                "auto_commit: true[自动提交，如果有执行VACUUM analyze，需要设置为true，默认false，也可以不设置这个参数]\n" +
                "dbbeans:\n" +
                "  - name: srcBean[源数据库，名称固定]\n" +
                "    user_name: \"用户名\"\n" +
                "    pass_word: \"密码\"\n" +
                "    tns: \"连接串\"\n" +
                "    dbType: \"数据库类型[ORACLE|MYSQL|POSTGRESQL|OTHER]\"";
    }
}
