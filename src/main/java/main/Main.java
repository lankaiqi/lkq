package main;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        Scanner scan = new Scanner(System.in);
        int pm = scan.nextInt();//项目经理
        int programmer = scan.nextInt();//程序员
        int idea = scan.nextInt();//主意
        Integer[] programmers = new Integer[programmer];//程序员集合
        Integer[] scjh = new Integer[idea];//输出集合
        for(int m=0;m<programmer;m++){//初始化程序员时间1
            programmers[m]=1;
        }
        List<Model> list = new ArrayList<Model>();//idea集合
        for (int n=0;n<idea;n++){
            Model model = new Model();
            model.num = scan.nextInt();
            model.pftime = scan.nextInt();
            model.priority = scan.nextInt();
            model.rtime = scan.nextInt();
            model.scwz=n;
            list.add(model);
            System.out.println("============================"+n+"==========================");
        }
        Collections.sort(list);
       while(true){
            Arrays.sort(programmers);//程序员时间排序
            //遍历同一时间空闲的程序员
           // int a=1;    //程序员个数
           // int time =0;//时间节点(控制下面循环出来程序员个数)
            int time =programmers[0];
//            for (int pro : programmers){
//                if (time!=0 && time<pro){ time=pro;  break;}
//                if (time==0 && time<pro){ time=pro;}
//                else if(time==pro) a++;
//                else break;
//            }
            //弹出时间最小的model,并执行
           // for (int x=0 ; x<a ;x++){
           while (true) {
               Model remove = list.remove(0);
               if (time >= remove.pftime) {//如果可以运行
                   programmers[0] = programmers[0] + remove.rtime;
                   scjh[remove.scwz] = programmers[0];
                   break;
               } else {
                   time++;
               }
           }
            //}
            if(list.size()==0) break;
       }
       for (int sc:scjh){
           System.out.println(sc);
       }

    }
}

class Model implements Comparable<Model>{
    int num;      //序号
    int pftime;   //提出时间
    int priority; //优先级
    int rtime;    //需要时间
    int scwz;    //输出位置
    @Override
    public int compareTo(Model o) {
        if(this.pftime>o.pftime){
            return 1;
        }else  if(this.pftime<o.pftime){
            return -1;
        }else  if(this.pftime==o.pftime){
            if(this.priority>o.priority){
                return 1;
            }else  if(this.priority<o.priority){
                return -1;
            }else  if(this.priority==o.priority){
                if(this.rtime>o.rtime){
                    return 1;
                }else  if(this.rtime<o.rtime){
                    return -1;
                }else  if(this.rtime==o.rtime){
                    return 1;
                }
            }
        }
        return 1;
    }
}