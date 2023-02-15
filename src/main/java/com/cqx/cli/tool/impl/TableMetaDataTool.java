package com.cqx.cli.tool.impl;

import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.jdbc.QueryResult;
import com.cqx.cli.tool.ITool;
import com.cqx.cli.tool.ToolMain;
import com.cqx.cli.tool.annotation.ToolImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 表元数据查询
 *
 * @author chenqixu
 */
@ToolImpl
public class TableMetaDataTool implements ITool {
    private static final Logger logger = LoggerFactory.getLogger(TableMetaDataTool.class);

    private JDBCUtil jdbcUtil;
    private String tab_name;

    @Override
    public void init(Map param) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        tab_name = (String) param.get("tab_name");
        jdbcUtil = new JDBCUtil(srcBean);
    }

    @Override
    public void exec() throws Exception {
        //元数据查询
        for (QueryResult queryResult : jdbcUtil.getTableMetaData(tab_name)) {
            logger.info("元数据 {}", queryResult);
        }
    }

    @Override
    public void close() throws Exception {
        if (jdbcUtil != null) jdbcUtil.close();
    }

    @Override
    public String getType() {
        return "table_meta_data";
    }

    @Override
    public String getDesc() {
        return "Query table metadata";
    }

    @Override
    public String getHelp() {
        return "开发示例：\n" +
                "tab_name: \"LOCATE_MART_ROTATE_TAB\"\n" +
                "dbbeans:\n" +
                "  - name: srcBean\n" +
                "    user_name: \"frtbase\"\n" +
                "    pass_word: \"frtbase\"\n" +
                "    tns: \"jdbc:oracle:thin:@10.1.8.204:1521/orapri\"\n" +
                "    dbType: \"ORACLE\"\n";
    }
}
