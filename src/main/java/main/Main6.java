package main;
import java.util.Scanner;

public class Main6 {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int num = scan.nextInt();//房间数
        int zh = scan.nextInt();//最后走入人的房间号
        int[] array = new int[num+1];
        int pop = -1;
        for(int n=1;n<=num;n++){
            int aa =scan.nextInt();
            array[n]=aa;
            if(pop==-1) pop=aa;
            else if(pop>aa) pop=aa;
        }

        int number = pop*num+zh;

        for(int n=1;n<=num;n++){
            System.out.print(array[n]-pop);
        }
    }
}
