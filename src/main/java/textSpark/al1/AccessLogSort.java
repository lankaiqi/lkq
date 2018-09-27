package textSpark.al1;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/9/21.
 */
public class AccessLogSort implements Serializable, Ordered<AccessLogSort> {
    private static final long serialVersionUID = 5897694900388556355L;

    private long upTraffic;
    private long downTraffic;
    private long timestamp;

    @Override
    public boolean $greater(AccessLogSort that) {
        if(upTraffic-that.getUpTraffic()>0){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()>0){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()==0 && timestamp-that.getTimestamp()>0){
            return true;
        }
        return false;
    }
    @Override
    public boolean $greater$eq(AccessLogSort that) {
        if($greater(that)){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()==0 && timestamp-that.getTimestamp()==0){
            return true;
        }
        return false;
    }


    @Override
    public boolean $less(AccessLogSort that) {
        if(upTraffic-that.getUpTraffic()<0){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()<0){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()==0 && timestamp-that.getTimestamp()<0){
            return true;
        }
        return false;
    }
    @Override
    public boolean $less$eq(AccessLogSort that) {
        if($less(that)){
            return true;
        }else if (upTraffic-that.getUpTraffic()==0 && downTraffic-that.getDownTraffic()==0 && timestamp-that.getTimestamp()==0){
            return true;
        }
        return false;
    }


    @Override
    public int compare(AccessLogSort that) {
        if(upTraffic-that.getUpTraffic() != 0){
            return (int)(upTraffic-that.getUpTraffic());
        }else if(downTraffic-that.getDownTraffic() != 0){
            return (int)(downTraffic-that.getDownTraffic());
        }else if (timestamp-that.getTimestamp() != 0){
            return (int)(timestamp-that.getTimestamp());
        }
        return 0;
    }
    @Override
    public int compareTo(AccessLogSort that) {
        return 0;
    }

    public Long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(Long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public Long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(Long downTraffic) {
        this.downTraffic = downTraffic;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AccessLogSort that = (AccessLogSort) o;

        if (upTraffic != that.upTraffic) return false;
        if (downTraffic != that.downTraffic) return false;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        int result = (int) (upTraffic ^ (upTraffic >>> 32));
        result = 31 * result + (int) (downTraffic ^ (downTraffic >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    public AccessLogSort() {
    }

    public AccessLogSort(Long upTraffic, Long downTraffic, Long timestamp) {
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AccessLogSort{" +
                "upTraffic=" + upTraffic +
                ", downTraffic=" + downTraffic +
                ", timestamp=" + timestamp +
                '}';
    }

}
