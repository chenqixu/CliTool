package com.cqx.cli.tool.bean;

import com.cqx.common.utils.jdbc.IJDBCUtilCall;
import com.cqx.common.utils.jdbc.QueryResult;

import java.util.List;

/**
 * MergeBean
 *
 * @author chenqixu
 */
public class MergeBean implements IJDBCUtilCall.IQueryResultBean {
    private String op_type;
    private List<QueryResult> queryResults;

    public MergeBean(List<QueryResult> queryResults) {
        this("i", queryResults);
    }

    public MergeBean(String op_type, List<QueryResult> queryResults) {
        this.op_type = op_type;
        this.queryResults = queryResults;
    }

    @Override
    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    @Override
    public List<QueryResult> getQueryResults() {
        return queryResults;
    }

    public void setQueryResults(List<QueryResult> queryResults) {
        this.queryResults = queryResults;
    }
}
