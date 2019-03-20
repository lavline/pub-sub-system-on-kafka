package EventSender;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import MySerdes.ValueSerde;
import Structure.EventVal;

public class EventProducer {

	public static void main(String[] args) {
		Properties Props =  new Properties();
		Props.put("bootstrap.servers", "192.168.101.15:9092,192.168.101.12:9092,192.168.101.28:9092");
		Props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Props.put("value.serializer", ValueSerde.EventValSerde.class.getName());
		
		//create producer
		KafkaProducer<String, EventVal> producer = new KafkaProducer<>(Props);
		
		Scanner input = new Scanner(System.in);
		int num;
		System.out.println("input event num: ");
		num = input.nextInt();
		int stock_id, attribute_num;
		Random r = new Random();
		for(int i = 0; i < num; i++) {

			//System.out.println("input stock id：");
			//stock_id = r.nextInt(10);
			stock_id = r.nextInt(10);
			//input.nextLine();
			//System.out.println("input the attribute num：");
			attribute_num = 20;

			//System.err.println(stock_id + " " + attribute_num);

			EventVal eVal = new EventVal(attribute_num, stock_id);
			eVal.EventProduceTime = System.currentTimeMillis();
			//create Record
			ProducerRecord<String, EventVal> record = new ProducerRecord<>("NewEvent", eVal);
			//zend
			try {
				producer.send(record);
				Thread.sleep(5);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.err.println("Producer Send Success!");
		}
		producer.close();
		input.close();
	}
}
