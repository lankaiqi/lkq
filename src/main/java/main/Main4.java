package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main4 {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int num = scan.nextInt();
        int fz = scan.nextInt();//翻转个数
        List<Integer> list = new ArrayList<Integer>();
        String start ="c";
        int sl=0;
        String get = scan.next();
        String[] split = get.split("");
        for (int n = 0; n < num; n++) {
            get = split[n];
            if (n==0){
                start=get;
                sl++;
            }
            else if(start.equals(get)){
                sl++;
            }else {
                start=get;
                list.add(sl);
                sl=1;
            }
        }

        int mix =0;
        for (int n = 0; n < list.size(); n++) {
            try {
                if(fz<list.get(n)){
                    if(n<list.size()) {
                        int after=fz+list.get(n+1);
                        if(mix<after) mix=after;
                    }
                }else if(fz>=list.get(n)){
                    int m=n;
                    int after=0;
                    if(fz==list.get(n) && n!=0)  after=list.get(n-1);
                    while (true){
                        after = after+list.get(m)+list.get(m+1);
                        fz=fz - list.get(m);//剩余翻转个数
                        m=m+2;
                        if(fz<list.get(m)){
                            after=after+fz;
                            if(mix<after) mix=after;
                        }
                    }
                }
            } catch (Exception e) {
               // e.printStackTrace();
            }
        }
        System.out.println(mix);
    }
}
