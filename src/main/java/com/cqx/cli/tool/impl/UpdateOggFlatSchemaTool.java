package com.cqx.cli.tool.impl;

import com.cqx.cli.tool.AbstractTool;
import com.cqx.cli.tool.annotation.ToolImpl;
import com.cqx.cli.tool.util.GetAvscUtils;
import com.cqx.common.bean.kafka.AvroRecord;
import com.cqx.common.utils.ftp.FtpBean;
import com.cqx.common.utils.ftp.FtpParamCfg;
import com.cqx.common.utils.jdbc.*;
import com.cqx.common.utils.kafka.SchemaUtil;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.TimeCostUtil;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 更新ogg和扁平化的schema工具
 *
 * @author chenqixu
 */
@ToolImpl
public class UpdateOggFlatSchemaTool extends AbstractTool {
    private static final Logger logger = LoggerFactory.getLogger(UpdateOggFlatSchemaTool.class);
    private final String FILE = "FILE";
    private final String SFTP = "SFTP";
    private final String STRING = "STRING";
    private JDBCUtil srcJdbcUtil;
    private String file_path;
    private String file_name;
    private String ogg_schema;
    private SchemaUtil schemaUtil;
    private String ogg_topic_name;
    private String flat_topic_name;
    private String avsc_type;
    private FtpParamCfg ftpParamCfg;
    private GetAvscUtils getAvscUtils;
    private String cluster_name;
    private String group_id;

    @Override
    public void init(Map param) throws Exception {
        file_path = (String) param.get("file_path");
        file_name = (String) param.get("file_name");
        avsc_type = ParamUtil.setValDefault(param, "avsc_type", "FILE");
        ogg_schema = ParamUtil.setValDefault(param, "ogg_schema", "");
        ogg_topic_name = (String) param.get("ogg_topic_name");
        flat_topic_name = (String) param.get("flat_topic_name");
        cluster_name = ParamUtil.setValDefault(param, "cluster_name", "kafka");
        group_id = ParamUtil.setValDefault(param, "group_id", "default");
        schemaUtil = new SchemaUtil("");
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(param);
        DBBean srcBean = paramsParserUtil.getBeanMap().get("srcBean");
        srcJdbcUtil = new JDBCUtil(srcBean, 2, 1, 1);

        switch (avsc_type) {
            case SFTP:
                // sftp
                FtpBean ftpBean = paramsParserUtil.getFtpBeanMap().get("oggSftp");
                ftpParamCfg = new FtpParamCfg(ftpBean);
                logger.info("【参数】ftpParamCfg：{}", ftpParamCfg);
                break;
            case STRING:
                break;
            case FILE:
            default:
                break;
        }
        // avsc util
        getAvscUtils = new GetAvscUtils();

        logger.info("【参数】file_path：{}", file_path);
        logger.info("【参数】file_name：{}", file_name);
        logger.info("【参数】avsc_type：{}", avsc_type);
        logger.info("【参数】ogg_topic_name：{}", ogg_topic_name);
        logger.info("【参数】flat_topic_name：{}", flat_topic_name);
        logger.info("【参数】cluster_name：{}", cluster_name);
        logger.info("【参数】group_id：{}", group_id);
        logger.info("【参数】ogg_schema：{}", ogg_schema);
    }

    @Override
    public boolean execHasRet() throws Exception {
        TimeCostUtil total = new TimeCostUtil();
        total.start();
        // 根据topic_name查询出对应的schema_name
        String querySql = "select schema_name from nmc_topic where topic_name='%s'" +
                " and cluster_name='%s' and group_id='%s'";
        List<List<QueryResult>> oggQuery = srcJdbcUtil.executeQuery(String.format(querySql, ogg_topic_name, cluster_name, group_id));
        List<List<QueryResult>> flatQuery = srcJdbcUtil.executeQuery(String.format(querySql, flat_topic_name, cluster_name, group_id));
        String ogg_scheam_name;
        String flat_schema_name;
        if (oggQuery.size() == 1 && flatQuery.size() == 1) {
            ogg_scheam_name = oggQuery.get(0).get(0).getValue().toString();
            flat_schema_name = flatQuery.get(0).get(0).getValue().toString();
            logger.info("查询到的ogg.schema：{}，flat.schema：{}", ogg_scheam_name, flat_schema_name);
        } else {
            logger.error(String.format("没有查询到数据，ogg：%s，flat：%s", ogg_topic_name, flat_topic_name));
            return false;
        }
        String oggSchema;
        switch (avsc_type) {
            case FILE:
                // 从文件读取oggSchema
                oggSchema = getAvscUtils.avscFromFile(file_path, file_name);
                break;
            case SFTP:
                // 从SFTP读取oggSchema
                oggSchema = getAvscUtils.avscFromSftp(ftpParamCfg, file_path, file_name);
                break;
            case STRING:
                oggSchema = ogg_schema;
                break;
            default:
                logger.error("不认识的类型：{}，({} | {} | {})", avsc_type, FILE, SFTP, STRING);
                return false;
        }
        if (oggSchema == null || oggSchema.trim().length() == 0) {
            logger.error("无法获取到oggSchema");
            return false;
        }
        // 更新到ogg的schema
        String sql = "update nmc_schema set avsc=?, modify_date=sysdate where schema_name=?";
        QueryResultFactory oggQRF = QueryResultFactory.getInstance()
                .buildQR("avsc", "java.sql.Clob", oggSchema)
                .buildQR("schema_name", "java.lang.String", ogg_scheam_name)
                .toList();
        int ogg_ret = srcJdbcUtil.executeBatch(sql, oggQRF.getData(), oggQRF.getDstFieldsType(), true);
        // 解析成扁平化需要的schema
        String flatSchemaStr = changeOggToFlat(oggSchema);
        // 更新到扁平化的schema
        QueryResultFactory flatQRF = QueryResultFactory.getInstance()
                .buildQR("avsc", "java.sql.Clob", flatSchemaStr)
                .buildQR("schema_name", "java.lang.String", flat_schema_name)
                .toList();
        int flat_ret = srcJdbcUtil.executeBatch(sql, flatQRF.getData(), flatQRF.getDstFieldsType(), true);
        logger.info("更新ogg和扁平化的schema完成，ogg_ret：{}，flat_ret：{}，总耗时：{}毫秒", ogg_ret, flat_ret, total.stopAndGet());
        return true;
    }

    @Override
    public void close() throws Exception {
        if (srcJdbcUtil != null) srcJdbcUtil.close();
    }

    @Override
    public String getType() {
        return "update_ogg_flat_schema";
    }

    @Override
    public String getDesc() {
        return "Updating Ogg and flattening schema tools";
    }

    @Override
    public String getHelp() {
        return null;
    }

    /**
     * 转换ogg的schema变成扁平化的schema
     *
     * @param oggSchema
     * @return
     */
    private String changeOggToFlat(String oggSchema) {
        StringBuilder sb = new StringBuilder();
        Schema schema = schemaUtil.getSchemaByString(oggSchema);
        AvroRecord avroRecord = schemaUtil.dealSchema(schema, null);
        String schemaName = schema.getName().toLowerCase();
        String schemaNameSpace = schema.getNamespace().toLowerCase();
        String schemaType = schema.getType().getName();
        sb.append("{\"name\":\"").append(schemaName).append("\",\n" +
                "  \"namespace\":\"").append(schemaNameSpace).append("\",\n" +
                "  \"type\":\"").append(schemaType).append("\",\n" +
                "  \"fields\":[");
        if (avroRecord.hasChild()) {
            for (AvroRecord child : avroRecord.getChilds()) {
                String fatherName = child.getName();
                if (child.hasChild()) {
                    for (AvroRecord _child : child.getChilds()) {
                        sb.append("  {\"name\":\"")
                                .append(fatherName + "_" + _child.getName().toLowerCase())
                                .append("\",\"type\":[\"")
                                .append(_child.getType().getName().toLowerCase());
                        sb.append("\"]},\n");
                    }
                } else {
                    sb.append("  {\"name\":\"")
                            .append(fatherName)
                            .append("\",\"type\":[\"")
                            .append("string")
                            .append("\"]},\n");
                }
            }
        }
        sb.deleteCharAt(sb.length() - 2);
        sb.append("]}");
        return sb.toString();
    }

    /**
     * 设置ogg_schema，适用于avsc_type是STRING的情况
     *
     * @param ogg_schema
     */
    public void setOgg_schema(String ogg_schema) {
        this.ogg_schema = ogg_schema;
    }
}
