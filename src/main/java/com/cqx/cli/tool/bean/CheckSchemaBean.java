package com.cqx.cli.tool.bean;

/**
 * CheckSchemaBean
 *
 * @author chenqixu
 */
public class CheckSchemaBean {
    private String ogg_topic_name;
    private String file_path;
    private String file_name;
    private String cluster_name;
    private String group_id;

    @Override
    public String toString() {
        return "ogg_topic_name：" + ogg_topic_name
                + "，file_path：" + file_path
                + "，file_name：" + file_name
                + "，cluster_name：" + cluster_name
                + "，group_id：" + group_id;
    }

    public String getOgg_topic_name() {
        return ogg_topic_name;
    }

    public void setOgg_topic_name(String ogg_topic_name) {
        this.ogg_topic_name = ogg_topic_name;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getCluster_name() {
        return cluster_name;
    }

    public void setCluster_name(String cluster_name) {
        this.cluster_name = cluster_name;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }
}
