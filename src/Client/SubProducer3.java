package Client;

import MySerdes.ValueSerde;
import Structure.SubscribeVal;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class SubProducer3 {
    //opindex event
    private final static int ATTRIBUITE_NUM = 20;
    private static ArrayList<Pivot>Pivot_Attri = new ArrayList<>();
    private static ArrayList<SubscribeVal> Cache_Sub = new ArrayList<>();

    static public int compare(Pivot o1, Pivot o2) {
        if ( o1.num > o2.num ) {
            return 1;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) {
        for(int i = 0; i < ATTRIBUITE_NUM; i++)Pivot_Attri.add(new Pivot(i));

        Properties Props =  new Properties();
        Props.put("bootstrap.servers", "192.168.101.15:9092,192.168.101.12:9092,192.168.101.28:9092");
        Props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Props.put("value.serializer", ValueSerde.SubValSerde.class.getName());

        KafkaProducer<String, SubscribeVal> producer = new KafkaProducer<>(Props);

        int num = 0;
        int rate = 0;
        Scanner s = null;
        try{
            File file = new File("E:\\Kafka\\subData.txt");
            s = new Scanner(file);
            num = s.nextInt();
            rate = s.nextInt();
        }catch (Throwable e){
            System.exit(1);
        }
        for(int i = 0; i < num; i++) {
            String SubId = s.next();
            int StockId = s.nextInt();
            int AttributeNum = s.nextInt();
            SubscribeVal sVal = new SubscribeVal(AttributeNum);
            sVal.SubId = SubId;
            sVal.StockId = StockId;
            for(int j = 0; j < sVal.AttributeNum; j++){
                sVal.subVals.get(j).attributeId = s.nextInt();
                sVal.subVals.get(j).min_val = s.nextDouble();
                sVal.subVals.get(j).max_val = s.nextDouble();
                Pivot_Attri.get(sVal.subVals.get(j).attributeId).num++;
            }
            Cache_Sub.add(sVal);
        }
        Collections.sort(Pivot_Attri, (o1, o2) -> compare(o1,o2));
        for(int i = 0; i < num; i++) {
            SubscribeVal sVal = Cache_Sub.get(i);
            int max = ATTRIBUITE_NUM;
            for(int j = 0; j < sVal.AttributeNum; j++){
                int tmp = 0;
                int m = 0;
                while(sVal.subVals.get(j).attributeId != Pivot_Attri.get(m++).Attri_id)
                    tmp++;
                max = max > tmp ? tmp : max;
            }
            sVal.Pivot_Attri_Id = Pivot_Attri.get(max).Attri_id;
            //Record
            ProducerRecord<String, SubscribeVal> record = new ProducerRecord<>("NewSub", sVal);
            //send
            try {
                producer.send(record).get();
                System.err.println("Producer Send Success!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
        s.close();
    }
    static class Pivot{
        public int Attri_id;
        public int num;

        public Pivot(int i){
            this.Attri_id = i;
            this.num = 0;
        }
    }
}
