package main;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by Administrator on 2018/8/15.
 */
public class Main2 {
    public static int[] ii =new int[3];
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int num = scan.nextInt();
        BigDecimal[][] array = new BigDecimal[num][4];
        String[] result = new String[num];
        for (int n = 0; n < num; n++) {
            array[n][0] = scan.nextBigDecimal();
            array[n][1] = scan.nextBigDecimal();
            array[n][2] = scan.nextBigDecimal();
            array[n][3] = scan.nextBigDecimal();
        }
        BigDecimal x = new BigDecimal(0);
        BigDecimal y = new BigDecimal(0);
        BigDecimal z = new BigDecimal(0);
        for (int n = 0; n < num; n++){
            for (int a = 0; a< 4; a++){
                if(a==0){
                    try {
                        x=array[n][1].add(array[n][2]).add(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        y=array[n][1].subtract(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        z=array[n][1].subtract(array[n][2]).subtract(array[n][3]).subtract(array[n][3]).divide(new BigDecimal(3));
                        if(new BigDecimal(x.intValue()).compareTo(x)==0 && new BigDecimal(y.intValue()).compareTo(y)==0 && new BigDecimal(z.intValue()).compareTo(z)==0 && x.compareTo(BigDecimal.ZERO)>=0&& y.compareTo(BigDecimal.ZERO)>=0&& z.compareTo(BigDecimal.ZERO)>=0){
                            js(x.intValue(),y.intValue(),z.intValue(),array[n][0].intValue()-array[n][1].intValue(),n,result);
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }else if(a==1){
                    if(result[n]!=null) continue;
                    array[n][3]=BigDecimal.ZERO.subtract(array[n][3]);

                    try {
                        x=array[n][1].add(array[n][2]).add(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        y=array[n][1].subtract(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        z=array[n][1].subtract(array[n][2]).subtract(array[n][3]).subtract(array[n][3]).divide(new BigDecimal(3));
                        if(new BigDecimal(x.intValue()).compareTo(x)==0 && new BigDecimal(y.intValue()).compareTo(y)==0 && new BigDecimal(z.intValue()).compareTo(z)==0 && x.compareTo(BigDecimal.ZERO)>=0&& y.compareTo(BigDecimal.ZERO)>=0&& z.compareTo(BigDecimal.ZERO)>=0){
                            js(x.intValue(),y.intValue(),z.intValue(),array[n][0].intValue()-array[n][1].intValue(),n,result);
                        }
                    } catch (Exception e) {
                        continue;
                    }
                    array[n][3]=BigDecimal.ZERO.subtract(array[n][3]);
                }else if(a==2){
                    if(result[n]!=null) continue;
                    array[n][2]=BigDecimal.ZERO.subtract(array[n][2]);

                    try {
                        x=array[n][1].add(array[n][2]).add(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        y=array[n][1].subtract(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        z=array[n][1].subtract(array[n][2]).subtract(array[n][3]).subtract(array[n][3]).divide(new BigDecimal(3));
                        if(new BigDecimal(x.intValue()).compareTo(x)==0 && new BigDecimal(y.intValue()).compareTo(y)==0 && new BigDecimal(z.intValue()).compareTo(z)==0 && x.compareTo(BigDecimal.ZERO)>=0&& y.compareTo(BigDecimal.ZERO)>=0&& z.compareTo(BigDecimal.ZERO)>=0){
                            js(x.intValue(),y.intValue(),z.intValue(),array[n][0].intValue()-array[n][1].intValue(),n,result);
                        }
                    } catch (Exception e) {
                        continue;
                    }

                    array[n][2]=BigDecimal.ZERO.subtract(array[n][2]);
                }else if(a==3){
                    if(result[n]!=null) continue;
                    array[n][3]=BigDecimal.ZERO.subtract(array[n][3]);
                    array[n][2]=BigDecimal.ZERO.subtract(array[n][2]);

                    try {
                        x=array[n][1].add(array[n][2]).add(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        y=array[n][1].subtract(array[n][2]).add(array[n][3]).divide(new BigDecimal(3));
                        z=array[n][1].subtract(array[n][2]).subtract(array[n][3]).subtract(array[n][3]).divide(new BigDecimal(3));
                        if(new BigDecimal(x.intValue()).compareTo(x)==0 && new BigDecimal(y.intValue()).compareTo(y)==0 && new BigDecimal(z.intValue()).compareTo(z)==0 && x.compareTo(BigDecimal.ZERO)>=0&& y.compareTo(BigDecimal.ZERO)>=0&& z.compareTo(BigDecimal.ZERO)>=0){
                            js(x.intValue(),y.intValue(),z.intValue(),array[n][0].intValue()-array[n][1].intValue(),n,result);
                        }
                    } catch (Exception e) {
                        continue;
                    }

                    array[n][3]=BigDecimal.ZERO.subtract(array[n][3]);
                    array[n][2]=BigDecimal.ZERO.subtract(array[n][2]);
                }
            }

        }

        for (String rr : result){
            if(rr==null) rr="no";
            System.out.println(rr);
        }
    }

    /**
     *
     * @param x x分数
     * @param y y分数
     * @param z z分数
     * @param k 剩下场次
     * @param n 第几祖球队
     * @param result 返回结果数组
     */
    public static void js(int x,int y,int z,int k,int n,String[] result){
        ii[0]=x;ii[1]=y;ii[2]=z;
        while(true) {
            Arrays.sort(ii);
            if(ii[0]<ii[1] && k>=1){
                ii[0]=ii[0]+1;k--;
            }else  if(ii[0]==ii[1] && ii[0]<ii[2] && k>=2){
                ii[0]=ii[0]+1;ii[1]=ii[1]+1;k--;k--;
            }else  if(ii[0]==ii[1] && ii[0]==ii[2] && k>=3){
                ii[0]=ii[0]+1;ii[1]=ii[1]+1;ii[2]=ii[2]+1;k--;k--;k--;
            }else break;
        }
        if(ii[0]==ii[1] && ii[0]==ii[2]){
            result[n]="yes";
        }
    }
}
