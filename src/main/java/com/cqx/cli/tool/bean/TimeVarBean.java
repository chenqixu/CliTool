package com.cqx.cli.tool.bean;

import com.cqx.common.utils.Utils;

/**
 * TimeVarBean
 *
 * @author chenqixu
 */
public class TimeVarBean {
    private long baseTime;
    private String formatStr;
    private boolean isAdd;
    private long offset;
    private long resultLongTime;
    private String resultStrTime;
    private String desc;

    public TimeVarBean(String formatStr, String desc) {
        this(formatStr, false, desc);
    }

    public TimeVarBean(String formatStr, boolean isAdd, String desc) {
        this(formatStr, isAdd, 0L, desc);
    }

    public TimeVarBean(String formatStr, boolean isAdd, long offset, String desc) {
        this(0L, formatStr, isAdd, offset, desc);
    }

    public TimeVarBean(long baseTime, String formatStr, boolean isAdd, long offset, String desc) {
        this.baseTime = baseTime;
        this.formatStr = formatStr;
        this.isAdd = isAdd;
        this.offset = offset;
        this.desc = desc;
    }

    public void calc() {
        if (baseTime <= 0L) {
            throw new NullPointerException("基础时间为空！");
        }
        if (isAdd()) {
            setResultLongTime(getBaseTime() + getOffset());
            setResultStrTime(Utils.formatTime(getResultLongTime(), getFormatStr()));
        } else {
            setResultLongTime(getBaseTime() - getOffset());
            setResultStrTime(Utils.formatTime(getResultLongTime(), getFormatStr()));
        }
    }

    public long getBaseTime() {
        return baseTime;
    }

    public void setBaseTime(long baseTime) {
        this.baseTime = baseTime;
    }

    public String getFormatStr() {
        return formatStr;
    }

    public void setFormatStr(String formatStr) {
        this.formatStr = formatStr;
    }

    public boolean isAdd() {
        return isAdd;
    }

    public void setAdd(boolean add) {
        isAdd = add;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getResultLongTime() {
        return resultLongTime;
    }

    public void setResultLongTime(long resultLongTime) {
        this.resultLongTime = resultLongTime;
    }

    public String getResultStrTime() {
        return resultStrTime;
    }

    public void setResultStrTime(String resultStrTime) {
        this.resultStrTime = resultStrTime;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
