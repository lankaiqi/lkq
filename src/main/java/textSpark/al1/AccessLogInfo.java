package textSpark.al1;

import java.io.Serializable;

/**
 * 访问日志信息类 可序列化
 */
public class AccessLogInfo  implements Serializable{
   private static final long serialVersionUID = 5897694456388556355L;
   
    private long timestamp;
    private long upTraffic;
    private long downTraffic;

    public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public AccessLogInfo() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}
