package com.ibeifeng.sparkproject.domain;

/**
 * 页面切片转换率
 */
public class PageSplitConvertRate {
    private long taskid;
    private String converRate;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConverRate() {
        return converRate;
    }

    public void setConverRate(String converRate) {
        this.converRate = converRate;
    }
}
