package main;

import java.math.BigDecimal;
import java.util.Scanner;

/**
 * Created by Administrator on 2018/8/15.
 */
public class Main3 {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int num = scan.nextInt();
        int[][] array = new int[num][4];
        String[] result = new String[num];
        for (int n = 0; n < num; n++) {
            if(num==8)  System.out.println( array[n][0]+"==="+ array[n][1]+"==" +array[n][2]+"==="+ array[n][3]);
            array[n][0] = Integer.valueOf(scan.next());
            array[n][1] = Integer.valueOf(scan.next());
            array[n][2] = Integer.valueOf(scan.next());
            array[n][3] = Integer.valueOf(scan.next());
        }

        for (int n = 0; n < num; n++){
            int i = array[n][0] % 3;
            int ii = array[n][0] / 3;
            if(i!=0)   {System.out.println("no"); continue;}
            if(ii< array[n][2] || ii< array[n][3] )  {System.out.println("no");continue;}
            System.out.println("yes");
        }
    }
}
