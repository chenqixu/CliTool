package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.bean.CheckSchemaBean;
import com.cqx.cli.tool.util.GetAvscUtils;
import com.cqx.common.utils.ftp.FtpBean;
import com.cqx.common.utils.ftp.FtpParamCfg;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.IJDBCUtilCall;
import com.cqx.common.utils.jdbc.JDBCUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.common.utils.jdbc.lob.DefaultLobHandler;
import com.cqx.common.utils.jdbc.lob.LobHandler;
import com.cqx.common.utils.kafka.SchemaUtil;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.sftp.SftpConnection;
import com.cqx.common.utils.sftp.SftpUtil;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 检查远程ogg的schema和数据库中是否一致，支持批量
 *
 * @author chenqixu
 */
@ToolImpl
public class CheckRemoteOggSchemaMultipleTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(CheckRemoteOggSchemaMultipleTool.class);
    private JDBCUtil srcJdbcUtil;
    private FtpParamCfg ftpParamCfg;
    private SftpConnection sftpConnection;
    private GetAvscUtils getAvscUtils;
    private List<CheckSchemaBean> checkSchemaBeanList;

    @Override
    public void init(Map param) throws Exception {
        checkSchemaBeanList = new ArrayList<>();
        List<Map<String, String>> check_schemas = (List<Map<String, String>>) param.get("check_schema");
        for (Map<String, String> map : check_schemas) {
            CheckSchemaBean checkSchemaBean = new CheckSchemaBean();
            checkSchemaBean.setOgg_topic_name(map.get("ogg_topic_name"));
            checkSchemaBean.setFile_path(map.get("file_path"));
            checkSchemaBean.setFile_name(map.get("file_name"));
            String cluster_name = ParamUtil.setValDefault(map, "cluster_name", "kafka");
            String group_id = ParamUtil.setValDefault(map, "group_id", "default");
            checkSchemaBean.setCluster_name(cluster_name);
            checkSchemaBean.setGroup_id(group_id);
            checkSchemaBeanList.add(checkSchemaBean);
        }
        // db
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        srcJdbcUtil = new JDBCUtil(srcBean, 2, 1, 1);
        // sftp
        FtpBean ftpBean = paramsParserUtil.getFtpBeanMap().get("oggSftp");
        ftpParamCfg = new FtpParamCfg(ftpBean);
        sftpConnection = SftpUtil.getSftpConnection(ftpParamCfg);
        // avsc util
        getAvscUtils = new GetAvscUtils();

        logger.info("【参数】ftpParamCfg：{}", ftpParamCfg);
        logger.info("【参数】checkSchemaBeanList：{}", checkSchemaBeanList);
    }

    @Override
    public boolean execHasRet() throws Exception {
        final boolean[] ret = {true};
        for (final CheckSchemaBean checkSchemaBean : checkSchemaBeanList) {
            final LobHandler lobHandler = new DefaultLobHandler();
            String sql = "select avsc from nmc_schema where schema_name in(select schema_name from nmc_topic " +
                    "where topic_name='%s' and cluster_name='%s' and group_id='%s')";
            srcJdbcUtil.executeQuery(
                    String.format(sql, checkSchemaBean.getOgg_topic_name(), checkSchemaBean.getCluster_name(), checkSchemaBean.getGroup_id())
                    , new IJDBCUtilCall.ICallBack() {
                        @Override
                        public void call(ResultSet rs) throws SQLException {
                            SchemaUtil schemaUtil = new SchemaUtil(null);
                            String db_avsc = lobHandler.getClobAsString(rs, 1);
                            Schema db_schema = schemaUtil.getSchemaByString(db_avsc);
                            logger.debug("db_schema：{}", db_schema);
                            String sftp_avsc = getAvscUtils.avscFromSftp(sftpConnection
                                    , checkSchemaBean.getFile_path(), checkSchemaBean.getFile_name());
                            if (sftp_avsc != null) {
                                Schema sftp_schema = schemaUtil.getSchemaByString(sftp_avsc);
                                logger.debug("sftp_schema：{}", sftp_schema);
                                logger.info("{} equals {}", checkSchemaBean.getOgg_topic_name(), db_schema.equals(sftp_schema));
                                // 全部为真才为真
                                ret[0] = (ret[0] && db_schema.equals(sftp_schema));
                            }
                        }
                    }
            );
        }
        logger.info("最终校验结果 {}", ret[0]);
        return true;
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
        if (sftpConnection != null) SftpUtil.closeSftpConnection(sftpConnection);
    }

    @Override
    public String getType() {
        return "check_ogg_schema_multiple";
    }

    @Override
    public String getDesc() {
        return "Check whether the schema of remote multiple Oggs is consistent with that in the database.";
    }

    @Override
    public String getHelp() {
        return null;
    }
}
