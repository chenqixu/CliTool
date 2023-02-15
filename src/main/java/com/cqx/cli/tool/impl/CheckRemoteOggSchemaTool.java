package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
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
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * 检查远程ogg的schema和数据库中是否一致
 *
 * @author chenqixu
 */
@ToolImpl
public class CheckRemoteOggSchemaTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(CheckRemoteOggSchemaTool.class);
    private JDBCUtil srcJdbcUtil;
    private String ogg_topic_name;
    private FtpParamCfg ftpParamCfg;
    private String file_path;
    private String file_name;
    private GetAvscUtils getAvscUtils;
    private String cluster_name;
    private String group_id;

    @Override
    public void init(Map param) throws Exception {
        ogg_topic_name = (String) param.get("ogg_topic_name");
        file_path = (String) param.get("file_path");
        file_name = (String) param.get("file_name");
        cluster_name = ParamUtil.setValDefault(param, "cluster_name", "kafka");
        group_id = ParamUtil.setValDefault(param, "group_id", "default");
        // db
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        srcJdbcUtil = new JDBCUtil(srcBean, 2, 1, 1);
        // sftp
        FtpBean ftpBean = paramsParserUtil.getFtpBeanMap().get("oggSftp");
        ftpParamCfg = new FtpParamCfg(ftpBean);
        // avsc util
        getAvscUtils = new GetAvscUtils();

        logger.info("【参数】ogg_topic_name：{}", ogg_topic_name);
        logger.info("【参数】file_path：{}", file_path);
        logger.info("【参数】file_name：{}", file_name);
        logger.info("【参数】cluster_name：{}", cluster_name);
        logger.info("【参数】group_id：{}", group_id);
        logger.info("【参数】ftpParamCfg：{}", ftpParamCfg);
    }

    @Override
    public boolean execHasRet() throws Exception {
        final boolean[] ret = {false};
        final LobHandler lobHandler = new DefaultLobHandler();
        String sql = "select avsc from nmc_schema where schema_name in(select schema_name from nmc_topic " +
                "where topic_name='%s' and cluster_name='%s' and group_id='%s')";
        srcJdbcUtil.executeQuery(
                String.format(sql, ogg_topic_name, cluster_name, group_id)
                , new IJDBCUtilCall.ICallBack() {
                    @Override
                    public void call(ResultSet rs) throws SQLException {
                        SchemaUtil schemaUtil = new SchemaUtil(null);
                        String db_avsc = lobHandler.getClobAsString(rs, 1);
                        Schema db_schema = schemaUtil.getSchemaByString(db_avsc);
                        logger.info("db_schema：{}", db_schema);
                        String sftp_avsc = getAvscUtils.avscFromSftp(ftpParamCfg, file_path, file_name);
                        if (sftp_avsc != null) {
                            Schema sftp_schema = schemaUtil.getSchemaByString(sftp_avsc);
                            logger.info("sftp_schema：{}", sftp_schema);
                            logger.info("equals：{}", db_schema.equals(sftp_schema));
                            ret[0] = db_schema.equals(sftp_schema);
                        }
                    }
                }
        );
        // 只有校验不一致，才需要返回true进行更新操作
        return !ret[0];
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "check_ogg_schema";
    }

    @Override
    public String getDesc() {
        return "Check whether the schema of the remote Ogg is consistent with that in the database.";
    }

    @Override
    public String getHelp() {
        return null;
    }
}
