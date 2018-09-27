package com.ibeifeng.sparkproject.domain;

/**
 * 广告点击趋势
 */
public class AdClickTrend {
    private String date;
    private String minute;
    private long addid;
    private long clickCount;
    private String hour;


    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public long getAddid() {
        return addid;
    }

    public void setAddid(long addid) {
        this.addid = addid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
