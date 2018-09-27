package main;

import java.util.Scanner;

public class Main8 {

    public static int slsl =0 ;
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int m = sc.nextInt();
        int sc1 = 0;
        int  sc2 = 0;
        int[][] array = new int[n][n];
        for (int i = 0; i < m; i++) {
            int num1 = sc.nextInt()-1;
            int num2 = sc.nextInt()-1;
            if(num1<=num2){
                array[num1][num2] = 1;
            }else {
                array[num2][num1] = 1;
            }


        }
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (array[i][j] == 1) {
                    array[i][j] = 0;
                    // 判断周围是否有1
                    sc1++;
                    slsl=1;
                    dg(array, i-1, j-1,n,n);
                    if (sc2<slsl){
                        sc2=slsl;
                    }
                }
            }
        }
        System.out.println(sc1);
    }

    // 递归方法
    public static void dg(int[][] array, int i, int j ,int n , int m) {
        for (int a = 0; a <= 2; a++) {
            for (int b = 0; b <= 2; b++) {
                if (i+a>=m) break;
                if (j+b >=n)break;
                if (i+a<0) break;
                if (j+b<0)break;
                if (array[i+a][j+b] == 1){
                    array[i+a][j+b] =0;
                    slsl++;
                    //	System.out.println(Long.valueOf(i+a)+","+Long.valueOf(j+b));
                    dg(array, i+a-1, j+b-1,n,m);
                }
            }
        }
    }
}
