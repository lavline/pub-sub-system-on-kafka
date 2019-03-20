package Client;

import Structure.SubscribeVal;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.Scanner;

public class creatSub {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        int num = -1;
        int rate;
        BufferedWriter bw = null;
        try{
            File file = new File("E:\\Kafka\\subData.txt");
            FileWriter fw = new FileWriter(file, true);
            bw = new BufferedWriter(fw);
        }catch (Throwable e){
            System.exit(1);
        }
        System.out.println("input sub num: ");
        num = input.nextInt();
        input.nextLine();
        System.out.println("input rate: ");
        rate = input.nextInt();
        String[] name = { "Client1","Client2","Client3" };
        int attribute_num, stock_id;
        Random r = new Random();
        String s = String.valueOf(num) + " " + String.valueOf(rate);
        try {
            bw.write(s + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(num != 0) {
            //System.out.println("Client Name:" + name);
            for (int i = 0; i < num; i++) {
                stock_id = r.nextInt(10);
                //stock_id = 1;
                attribute_num = r.nextInt(20) + 1;//[1,20]
                SubscribeVal sVal;
                if (r.nextInt(1000) < rate)
                    sVal = new SubscribeVal(attribute_num, name[r.nextInt(3)], stock_id, 2);
                else {
                    sVal = new SubscribeVal(attribute_num, name[r.nextInt(3)], stock_id);
                }
                s =  sVal.SubId + " "
                    + String.valueOf(sVal.StockId) + " "
                    + String.valueOf(sVal.AttributeNum);
                for(int j = 0; j < sVal.AttributeNum; j++){
                    s = s + " " + String.valueOf(sVal.subVals.get(j).attributeId)
                            + " " + String.valueOf(sVal.subVals.get(j).min_val)
                            + " " + String.valueOf(sVal.subVals.get(j).max_val);
                }
                try {
                    bw.write(s + "\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("creat success!");
            input.nextLine();
            System.out.println("input sub num: ");
            num = input.nextInt();
            input.nextLine();
            System.out.println("input rate: ");
            rate = input.nextInt();
        }
        input.close();
        try {
            bw.close();
        }catch (Exception e){
            System.exit(1);
        }
    }
}
