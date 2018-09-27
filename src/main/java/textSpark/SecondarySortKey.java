package textSpark;

import scala.Serializable;
import scala.math.Ordered;

/**
 * Created by Administrator on 2018/9/4.
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if(this.first-that.first != 0 ) return this.first-that.first;
        else return this.second-that.second;
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if(this.first<that.first) return true;
        else if(this.first==that.first && this.second<that.second)  return  true;
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this.first>that.first) return true;
        else if(this.first==that.first && this.second>that.second) return true;
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if(this.$less(that)) return  true;
        else if(this.first==that.first && that.second==that.second) return true;
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if(this.$greater(that)) return true;
        else if(this.first==that.first && this.second==that.second) return true;
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if(this.first-that.first != 0 ) return this.first-that.first;
        else return this.second-that.second;
    }


    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }


}
