package main;

import java.util.Scanner;

public class main7 {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int num = sc.nextInt();
        Integer[][] array = new Integer[num][num];
        for(int a =0 ;a<num ; a++){
            for(int b =0 ;b<num ; b++){
                array[a][b] = sc.nextInt();
            }
        }
        int length =0;
        for(int i=0 ;i<num ; i++ )	{
            for(int j=0 ;j<num ; j++ )	{
                if(array[i][j]==0)continue;
                else {
                    length++;
                    array[i][j] = 0;
                    try {
                    dg(array,i,j,num);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        System.out.println(length);
    }
    public static void dg(Integer[][] array, int i, int j,int num) {
        int a = i-1,b=j;
        if(a<0 || b<0 ||  a>=num || b>=num){}
        else if(array[a][b]==1) {
            array[a][b] = 0;
            dg(array,a,b,num);
        }
        a=i;b=j-1;
        if(a<0 || b<0 ||  a>=num || b>=num){}
        else if(array[a][b]==1) {
            array[a][b] = 0;
            dg(array,a,b,num);
        }
        a=i+1;b=j;
        if(a<0 || b<0 ||  a>=num || b>=num){}
        else if(array[a][b]==1) {
            array[a][b] = 0;
            dg(array,a,b,num);
        }
        a=i;b=j+1;
        if(a<0 || b<0 ||  a>=num || b>=num){}
        else if(array[a][b]==1) {
            array[a][b] = 0;
            dg(array,a,b,num);
        }
    }
}
