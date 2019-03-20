package Client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import MySerdes.ValueSerde;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import Structure.SubscribeVal;

public class SubProducer {
	//random send
	public static void main(String[] args) {
		Properties Props =  new Properties();
		Props.put("bootstrap.servers", "192.168.101.15:9092,192.168.101.12:9092,192.168.101.28:9092");
		Props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Props.put("value.serializer", ValueSerde.SubValSerde.class.getName());

		KafkaProducer<String, SubscribeVal> producer = new KafkaProducer<>(Props);

		Scanner input = new Scanner(System.in);
		int num = -1;
		int rate = 0;
		BufferedWriter bw = null;
		try{
			File file = new File("/home/ubuntu/Stream-data/rcv-time-1.txt");
			FileWriter fw = new FileWriter(file, true);
			bw = new BufferedWriter(fw);
		}catch (Throwable e){
			System.exit(1);
		}

		while(num != 0) {
			System.out.println("input sub num: ");
			num = input.nextInt();
			System.out.println("input sub num: ");
			rate = input.nextInt();
			String name = "TestClient";
			System.out.println("Client Name:" + name);
			int attribute_num, stock_id;
			Random r = new Random();
			for (int i = 0; i < num; i++) {
				//name = input.next();
				//System.out.println("Stock Id:");
				//stock_id = r.nextInt(10);
				stock_id = 1;
				//System.out.println("Attribute Num:");
				attribute_num = r.nextInt(20) + 1;//[1,20]
				SubscribeVal sVal;
				if (r.nextInt(100) < rate)
					sVal = new SubscribeVal(attribute_num, name, stock_id, 2);
				else {
					sVal = new SubscribeVal(attribute_num, name, stock_id);
				}

				//ProducerRecord<String, SubscribeVal> record = new ProducerRecord<>("NewSub", sVal);
				String s =  sVal.SubId + " "
						+ String.valueOf(sVal.StockId) + " "
						+ String.valueOf(sVal.AttributeNum);
				for(int j = 0; j < sVal.AttributeNum; j++){
					s = s + " " + String.valueOf(sVal.subVals.get(j).attributeId)
							+ " " + String.valueOf(sVal.subVals.get(j).min_val)
							+ " " + String.valueOf(sVal.subVals.get(j).max_val);
				}

				try {
					//producer.send(record).get();
					//System.err.println("Producer Send Success!");
					bw.write(s + "\n");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		producer.close();
		input.close();
		try {
			bw.close();
		}catch (Exception e){
			System.exit(1);
		}
	}
}
