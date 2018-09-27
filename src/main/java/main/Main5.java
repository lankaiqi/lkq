package main;
import java.util.Scanner;
public class Main5 {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int[] array = new int[24];//每个面的数字
        for(int n=0;n<24;n++){
            array[n]= scan.nextInt();
        }
        int[][] number = new int[4*4*4*4*4][5];//旋转列表
        int n=0;

        //旋转可能封装
        for(int a=0;a<4;a++){
            for(int b=0;b<4;b++){
                for(int c=0;c<4;c++){
                    for(int d=0;d<4;d++){
                        for(int e=0;e<4;e++){
                            int[] intArray = new int[5];
                            intArray[0] = a;
                            intArray[1] = b;
                            intArray[2] = c;
                            intArray[3] = d;
                            intArray[4] = e;
                            number[n]=intArray;
                            n++;
                        }
                    }
                }
            }
        }
        int[] change = new int[24];//中转站array
        int[] jsArray = new int[24];//进行计算的array
        int sum = reckon(array);
        for(int[] aa : number){
            csh(array,jsArray);
            for(int nums :aa){
                changes(nums,jsArray,change);
                int reckon = reckon(jsArray);
                if(reckon>sum)sum=reckon;
            }
        }
        System.out.println(sum);
    }

    public static void csh(int[] array,int[] jsArray){
        jsArray[0]=array[0];
        jsArray[1]=array[1];
        jsArray[2]=array[2];
        jsArray[3]=array[3];
        jsArray[4]=array[4];
        jsArray[5]=array[5];
        jsArray[6]=array[6];
        jsArray[7]=array[7];
        jsArray[8]=array[8];
        jsArray[9]=array[9];
        jsArray[10]=array[10];
        jsArray[11]=array[11];
        jsArray[12]=array[12];
        jsArray[13]=array[13];
        jsArray[14]=array[14];
        jsArray[15]=array[15];
        jsArray[16]=array[16];
        jsArray[17]=array[17];
        jsArray[18]=array[18];
        jsArray[19]=array[19];
        jsArray[20]=array[20];
        jsArray[21]=array[21];
        jsArray[22]=array[22];
        jsArray[23]=array[23];
    }
    /**
     *交换
     * @param nums      //旋转方式
     * @param array     //每个面的数字
     * @param change    //中转数组
     */
    public static void changes(int nums, int[] array,int[] change){
        if(nums==0){
            change[0]=array[0];
            change[1]=array[1];
            change[2]=array[2];
            change[3]=array[3];
            change[4]=array[4];
            change[5]=array[5];
            change[6]=array[6];
            change[7]=array[7];
            change[8]=array[8];
            change[9]=array[9];
            change[22]=array[22];
            change[23]=array[23];

            array[0]=change[2];
            array[1]=change[0];
            array[2]=change[3];
            array[3]=change[1];
            array[4]=change[6];
            array[5]=change[7];
            array[6]=change[8];
            array[7]=change[9];
            array[8]=change[23];
            array[9]=change[22];
            array[22]=change[5];
            array[23]=change[4];
        }else if(nums==1){
            change[0]=array[0];
            change[1]=array[1];
            change[2]=array[2];
            change[3]=array[3];
            change[4]=array[4];
            change[5]=array[5];
            change[6]=array[6];
            change[7]=array[7];
            change[8]=array[8];
            change[9]=array[9];
            change[22]=array[22];
            change[23]=array[23];

            array[0]=change[1];
            array[1]=change[3];
            array[2]=change[0];
            array[3]=change[2];
            array[4]=change[23];
            array[5]=change[22];
            array[6]=change[4];
            array[7]=change[5];
            array[8]=change[6];
            array[9]=change[7];
            array[22]=change[9];
            array[23]=change[8];
        }else if(nums==2){
            change[4]  =array[4];
            change[10] =array[10];
            change[5]  =array[5];
            change[11] =array[11];
            change[0]  =array[0];
            change[2]  =array[2];
            change[6]  =array[6];
            change[12] =array[12];
            change[16] =array[16];
            change[18] =array[18];
            change[20] =array[20];
            change[22] =array[22];

            array[4]  =change[5];
            array[10] =change[4];
            array[5]  =change[11];
            array[11] =change[10];
            array[0]  =change[6];
            array[2]  =change[12];
            array[6]  =change[16];
            array[12] =change[18];
            array[16] =change[20];
            array[18] =change[22];
            array[20] =change[0];
            array[22] =change[2];
        }else if(nums==3){
            change[4]  =array[4];
            change[10] =array[10];
            change[5]  =array[5];
            change[11] =array[11];
            change[0]  =array[0];
            change[2]  =array[2];
            change[6]  =array[6];
            change[12] =array[12];
            change[16] =array[16];
            change[18] =array[18];
            change[20] =array[20];
            change[22] =array[22];

            array[4]  =change[10];
            array[10] =change[11];
            array[5]  =change[4];
            array[11] =change[5];
            array[0]  =change[20];
            array[2]  =change[22];
            array[6]  =change[0];
            array[12] =change[2];
            array[16] =change[6];
            array[18] =change[12];
            array[20] =change[16];
            array[22] =change[18];
        }
    }
    public static int reckon(int[] array){
        return (array[0]*array[1]*array[2]*array[3])+
                (array[4]*array[5]*array[10]*array[11])+
                (array[6]*array[7]*array[12]*array[13])+
                (array[8]*array[9]*array[14]*array[15])+
                (array[16]*array[17]*array[18]*array[19])+
                (array[20]*array[21]*array[22]*array[23]);
    }
}
