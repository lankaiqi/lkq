package com.ibeifeng.sparkproject.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 品类二次排序key
 *
 * 封装你要进行排序算法需要的几个字段:点击次数,下单次数,支付次数
 * 实现Ordered接口要求的几个方法
 *
 * 跟其他的key相比,如何判定大于,大于等于,小于,小于等于
 *
 * 依次使用三个次数进行比较,如果某一个相等,那么就比较下一个
 *
 * TODO(自定义的二次排序key,必须要实现Seializable接口,表明是可以序列化的,否则会报错)
 */
public class CategorySortKey implements Ordered<CategorySortKey>,Serializable{

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount=clickCount;
        this.orderCount=orderCount;
        this.payCount=payCount;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount>other.clickCount){
            return true;
        }else if (clickCount == other.clickCount && orderCount>other.orderCount){
            return true;
        }else if (clickCount == other.clickCount && orderCount == other.orderCount && payCount>other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if ($greater(other)){
            return true;
        }else if (clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if (clickCount<other.clickCount){
            return true;
        }else if (clickCount == other.clickCount && orderCount<other.orderCount){
            return true;
        }else if (clickCount == other.clickCount && orderCount == other.orderCount && payCount<other.payCount){
            return true;
        }
        return false;
    }


    @Override
    public boolean $less$eq(CategorySortKey other) {
        if ($less(other)){
            return true;
        }else if (clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount){
            return true;
        }
        return false;
    }


    @Override
    public int compare(CategorySortKey other) {
        if(clickCount - other.clickCount != 0 ){
            return (int) (clickCount - other.clickCount);
        }else if (orderCount - other.orderCount != 0 ){
            return (int) (orderCount - other.orderCount);
        }else if (payCount - other.payCount != 0 ){
            return (int) (payCount - other.payCount);
        }
        return 0;
    }


    @Override
    public int compareTo(CategorySortKey other) {
        if(clickCount - other.clickCount != 0 ){
            return (int) (clickCount - other.clickCount);
        }else if (orderCount - other.orderCount != 0 ){
            return (int) (orderCount - other.orderCount);
        }else if (payCount - other.payCount != 0 ){
            return (int) (payCount - other.payCount);
        }
        return 0;
    }


    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
