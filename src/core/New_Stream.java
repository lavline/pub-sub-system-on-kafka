package core;

import MySerdes.ValueSerde;
import Structure.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;

public class New_Stream {

    private final static int TPYE_MATCH = 0;
    private final static int TYPE_SEND = 1;
    private final static int PART = 1000;
    private final static int MAX_VALUE = 1000;
    private final static double GROUP_WIDTH = (double)MAX_VALUE / (double)PART;
    private final static int STOCKNUM = 100;
    private final static int ATTRIBUTE_NUM = 20;
    private static int[] SubNum = new int[STOCKNUM];
    private static int matchNum = 0;

    private static Bucket[][][][] bucketlist = new Bucket[STOCKNUM][ATTRIBUTE_NUM][PART][2];
    private static BitSetVal[][] bitSet = new BitSetVal[STOCKNUM][100000];

    private static double AverSendTime = 0;
    private static double LastSendTime = 0;
    private static double LastSendThread_N = 28;
    private static int SendNum = 0;
    private static int SendThreadNum = 2;
    private static double alpha = 1.0 / 5000000.0;

    private final static int match_thread_num = 2;

    public static void main(String[] args) {

        //initialize bucketlist
        for(int i = 0; i < 2; i++){
            for(int j = 0; j < ATTRIBUTE_NUM; j++){
                for(int r = 0; r < STOCKNUM; r++){
                    for(int w = 0; w < PART; w++)
                        bucketlist[r][j][w][i] = new Bucket();
                }
            }
        }
        //
        for(int i= 0; i < STOCKNUM; i++)
            SubNum[i] = 0;

        ThreadPoolExecutor executorMatch = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
        ThreadPoolExecutor executorSend = new ThreadPoolExecutor(15, 15,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        //set config
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_index");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.101.15:9092,192.168.101.12:9092,192.168.101.28:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Properties ProducerProps =  new Properties();
        ProducerProps.put("bootstrap.servers", "192.168.101.15:9092,192.168.101.12:9092,192.168.101.28:9092");
        ProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ProducerProps.put("value.serializer", ValueSerde.EventValSerde.class.getName());
        KafkaProducer<String, EventVal> producer = new KafkaProducer<>(ProducerProps);

        final StreamsBuilder index_builder = new StreamsBuilder();
        final StreamsBuilder match_builder = new StreamsBuilder();

        KStream<String, SubscribeVal> subscribe = index_builder.stream("NewSub",
                Consumed.with(Serdes.String(), new ValueSerde.SubscribeSerde()));
        KStream<String, EventVal> event = match_builder.stream("NewEvent",
                Consumed.with(Serdes.String(), new ValueSerde.EventSerde()));

        System.out.println("Stream Id: streams-match Max Stock Num: " + STOCKNUM + " Max Attribute Num: " + ATTRIBUTE_NUM +
                "\nPart Num: " + PART + " Max Value: " + MAX_VALUE + " Group Width: " + GROUP_WIDTH);

        //parallel model
        class Parallel implements Runnable{
            private int threadIdx;
            private int type;
            private EventVal v;
            private CountDownLatch latch;
            private KafkaProducer<String, EventVal> Producer;

            private Parallel(int threadIdx, EventVal val, CountDownLatch latch){
                this.threadIdx = threadIdx;
                this.v = val;
                this.latch = latch;
                this.type = TPYE_MATCH;
            }
            private Parallel(int threadIdx, EventVal val, CountDownLatch latch, KafkaProducer<String, EventVal> producer){
                this.threadIdx = threadIdx;
                this.v = val;
                this.latch = latch;
                this.Producer = producer;
                this.type = TYPE_SEND;
            }

            private void Match(){
                int stock_id = this.v.StockId;
                int index = this.threadIdx;
                int stride = match_thread_num;
                int attribute_num = v.AttributeNum;
                for (int i = index; i < attribute_num; i += stride) {
                    int attribute_id = this.v.eventVals[i].attributeId;
                    double val = this.v.eventVals[i].val;
                    int group = (int) (val / GROUP_WIDTH);

                    for (List e : bucketlist[stock_id][attribute_id][group][1].bucket) {
                        if (e.val < val) {
                            bitSet[stock_id][e.Id].b = true;
                        }
                    }
                    for (int j = group - 1; j >= 0; j--) {
                        for (List e : bucketlist[stock_id][attribute_id][j][1].bucket) {
                            bitSet[stock_id][e.Id].b = true;
                        }
                    }
                    for (List e : bucketlist[stock_id][attribute_id][group][0].bucket) {
                        if (e.val > val) {
                            bitSet[stock_id][e.Id].b = true;
                        }
                    }
                    for (int j = group + 1; j < PART; j++) {
                        for (List e : bucketlist[stock_id][attribute_id][j][0].bucket) {
                            bitSet[stock_id][e.Id].b = true;
                        }
                    }
                }
            }
            private void Send(){
                int stock_id = this.v.StockId;
                int index = this.threadIdx;
                int stride = SendThreadNum;
                for (int i = index; i < SubNum[stock_id]; i += stride) {
                    if (bitSet[stock_id][i].state) {
                        if (!bitSet[stock_id][i].b) {
                            matchNum++;
                            int id = i;
                            Thread thread = new Thread(() -> {
                                ProducerRecord<String, EventVal> record = new ProducerRecord<>(bitSet[stock_id][id].SubId, this.v);
                                try {
                                    Producer.send(record);
                                } catch (Exception x) {
                                    x.printStackTrace();
                                }
                            });
                            executorSend.execute(thread);
                        }else {
                            bitSet[stock_id][i].b = false;
                        }
                    }
                }
            }
            public void run(){
                switch (this.type) {
                    case TPYE_MATCH:
                        this.Match();
                        break;
                    case TYPE_SEND:
                        this.Send();
                        break;
                    default:
                        try{
                            throw new NullPointerException();
                        }catch (NullPointerException e) {
                            System.out.println("The type is error!");
                        }
                        break;
                }
                this.latch.countDown();
            }
        }

        //index structure insert
        subscribe.foreach((k,v)->{

            final  String subId = v.SubId;
            final int stock_id = v.StockId;
            final  int sub_num_id = SubNum[stock_id];
            final int attributeNum = v.AttributeNum;

            System.out.println("Client Name: " + subId + " Client Num Id: " + sub_num_id +
                    " Sub Stock Id: " + stock_id + " Attribute Num: " + attributeNum);

            //initialize bitset
            bitSet[stock_id][sub_num_id] = new BitSetVal();
            bitSet[stock_id][sub_num_id].SubId = subId;
            bitSet[stock_id][sub_num_id].state = true;

            //insert sub to bucketlist
            for(int i = 0; i < attributeNum; i++) {

                int attribute_id = v.subVals.get(i).attributeId;
                double min_val = v.subVals.get(i).min_val;
                double max_val = v.subVals.get(i).max_val;

                //System.out.println("Attribute Id: " + attribute_id + " Lower Limit: " + min_val + " Hight Limit: " + max_val);

                int group = (int)(min_val / GROUP_WIDTH);
                bucketlist[stock_id][attribute_id][group][0].bucket.add(new List(sub_num_id, min_val));
                group = (int)(max_val / GROUP_WIDTH);
                bucketlist[stock_id][attribute_id][group][1].bucket.add(new List(sub_num_id, max_val));
            }

            SubNum[stock_id]++;
        });
        File file = new File("resources/sender-time.txt");
        FileWriter fw = null;
        try {
            fw = new FileWriter(file, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        //matcher
        KStream<String, EventVal> matchstream = event.mapValues(v -> {
            //compute event access delay
            long tmpTime = System.currentTimeMillis();
            //EventVal eVal = value;
            v.EventArriveTime = tmpTime - v.EventProduceTime;
            //preprocess
            final  CountDownLatch latch = new CountDownLatch(match_thread_num);
            //System.out.println("Stock Id: " + stock_id + " Attribute Num: " + attributeNum);
            //match
            tmpTime = System.nanoTime();
            for (int i = 0; i < match_thread_num; i++) {
                Parallel s = new Parallel(i, v, latch);
                executorMatch.execute(s);
            }
            try {
                latch.await();
            }catch (Exception e){
                e.printStackTrace();
            }
            String s = String.valueOf((System.nanoTime() - tmpTime)/1000000.0);
            try {
                bw.write(s + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return v;
        });

        //sender
        matchstream.foreach((k,v)->{
            long tmp = System.nanoTime();
            final  CountDownLatch latch = new CountDownLatch(SendThreadNum);
            v.EventStartSendTime = System.currentTimeMillis();
            for (int i = 0; i < SendThreadNum; i++) {
                Parallel s = new Parallel(i, v, latch, producer);
                executorMatch.execute(s);
            }
            try {
                latch.await();
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("send time: " + (System.nanoTime() - tmp)/1000000.0 + " " + SendThreadNum + "matchNum:" + matchNum);
            matchNum = 0;
            //tmp1 = System.nanoTime();
            /*
            if(SendNum == 10) {
                AverSendTime /= 10;
		        System.out.println("\nAver_Time: " + (AverSendTime / 1000000.0));
                double e = alpha * (AverSendTime - LastSendTime) / (SendThreadNum - LastSendThread_N);
                System.out.println(e);
                e = SendThreadNum - 6 * Math.tanh(e);
                e = e > 1 ? Math.round(e) : 1;
                if (e != SendThreadNum) {
                    LastSendThread_N = SendThreadNum;
                    LastSendTime = AverSendTime;
                    SendThreadNum = (int) e;
                }

                AverSendTime = 0;
                SendNum = 0;
                System.out.println("thread adjust time: " + (System.nanoTime() - tmp1) + "\n");
            }
            tmp = System.nanoTime() - tmp;
            System.out.println("sender delay: " + (tmp / 1000000.0));
            SendNum++;
            AverSendTime += tmp;
            try {
                bw.write(String.valueOf(tmp / 1000000.0) + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }*/
        });

        final Topology index_topology = index_builder.build();
        final KafkaStreams stream_index = new KafkaStreams(index_topology, props);

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_matcher_1");
        final Topology matcher_topology = match_builder.build();
        final KafkaStreams stream_matcher = new KafkaStreams(matcher_topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                stream_index.close();
                stream_matcher.close();
                producer.close();
                executorMatch.shutdown();
                executorSend.shutdown();
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            }
        });

        try {
            stream_index.start();
            stream_matcher.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
