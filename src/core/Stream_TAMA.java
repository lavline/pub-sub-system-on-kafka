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

public class Stream_TAMA {
    private final static int LEVEL = 11;
    private final static int MAX_VALUE = 1000;
    private final static double CELL_WIDTH[] = new double[LEVEL];
    private final static int STOCKNUM = 100;
    private final static int ATTRIBUTE_NUM = 20;
    private static int[] SubNum = new int[STOCKNUM];

    private static Bucket[][][][] bucketlist = new Bucket[STOCKNUM][ATTRIBUTE_NUM][LEVEL][];
    private static SubSet[] sets = new SubSet[STOCKNUM];

    private static double AverSendTime = 0;
    private static double LastSendTime = 0;
    private static double LastSendThread_N = 28;
    private static int SendNum = 0;
    private static int SendThreadNum = 2;
    private static double alpha = 1.0 / 5000000.0;

    private final static int match_thread_num = 2;
    private static int matchNum = 0;

    public static void main(String[] args) {

        //initialize bucketlist
        for(int j = 0; j < ATTRIBUTE_NUM; j++){
            for(int r = 0; r < STOCKNUM; r++){
                sets[r] = new SubSet();
                for(int w = 0; w < LEVEL; w++) {
                    int t = (int) Math.pow(2, w);
                    CELL_WIDTH[w] = (double) MAX_VALUE / (double) t;
                    bucketlist[r][j][w] = new Bucket[t];
                    for(int i = 0;i < t;i++)
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
                "\nPart Num: " + LEVEL + " Max Value: " + MAX_VALUE );

        //parallel model
        class Parallel implements Runnable{
            private int threadIdx;
            private EventVal v;
            private CountDownLatch latch;
            private KafkaProducer<String, EventVal> Producer;

            private Parallel(int threadIdx, EventVal val, CountDownLatch latch, KafkaProducer<String, EventVal> producer){
                this.threadIdx = threadIdx;
                this.v = val;
                this.latch = latch;
                this.Producer = producer;
            }

            private void Match(){
                int stock_id = this.v.StockId;
                int index = this.threadIdx;
                int stride = match_thread_num;
                int attribute_num = v.AttributeNum;
                for (int i = index; i < attribute_num; i += stride) {
                    int attribute_id = this.v.eventVals[i].attributeId;
                    double val = this.v.eventVals[i].val;
                    for(int j = 0;j < LEVEL;j++){
                        int t = (int)(val / CELL_WIDTH[j]);
                        for(List e:bucketlist[stock_id][attribute_id][j][t].bucket){
                            try{
                                sets[stock_id].subset.get(e.Id).sem.acquire(2);
                                sets[stock_id].subset.get(e.Id).matched_num++;
                            }catch(InterruptedException r){
                                r.printStackTrace();
                            }finally {
                                sets[stock_id].subset.get(e.Id).sem.release(2);
                            }
                            if (sets[stock_id].subset.get(e.Id).matched_num == sets[stock_id].subset.get(e.Id).attribute_num) {
                                int id = e.Id;
                                matchNum ++;
                                Thread thread = new Thread(() -> {
                                    ProducerRecord<String, EventVal> record = new ProducerRecord<>(sets[stock_id].subset.get(id).SubId, this.v);
                                    try {
                                        Producer.send(record);
                                    } catch (Exception x) {
                                        x.printStackTrace();
                                    }
                                });
                                executorSend.execute(thread);
                            }
                        }
                    }

                }
            }
            public void run(){
                this.Match();
                this.latch.countDown();
            }
        }

        //index structure insert
        subscribe.foreach((k,v)->{
            final  String subId = v.SubId;
            final int stock_id = v.StockId;
            final  int sub_num_id = SubNum[stock_id];
            final int attributeNum = v.AttributeNum;
            sets[stock_id].add(attributeNum,subId);

            System.out.println("Client Name: " + subId + " Client Num Id: " + sub_num_id +
                    " Sub Stock Id: " + stock_id + " Attribute Num: " + attributeNum);

            for(int i = 0;i < attributeNum;i++){
                double low = v.subVals.get(i).min_val;
                double high = v.subVals.get(i).max_val;
                int start = 0;
                for(int j = 0;j < LEVEL ;j++){
                    int left = (int)(low / CELL_WIDTH[j]);
                    int right = (int)(high / CELL_WIDTH[j]);
                    start = j;
                    if(right - left > 1){
                        if(right - left == 2){
                            bucketlist[stock_id][i][j][left + 1].bucket.add(new List(sub_num_id));
                        }else {
                            bucketlist[stock_id][i][j][left + 1].bucket.add(new List(sub_num_id));
                            bucketlist[stock_id][i][j][right - 1].bucket.add(new List(sub_num_id));
                        }
                        break;
                    }
                }
                for(int j = start + 1;j < LEVEL - 1;j++){
                    int left = (int)(low / CELL_WIDTH[j]);
                    int right = (int)(high / CELL_WIDTH[j]);
                    bucketlist[stock_id][i][j][left + 1].bucket.add(new List(sub_num_id));
                    bucketlist[stock_id][i][j][right - 1].bucket.add(new List(sub_num_id));
                }
                int left = (int)(low / CELL_WIDTH[LEVEL - 1]);
                int right = (int)(high / CELL_WIDTH[LEVEL - 1]);
                bucketlist[stock_id][i][LEVEL - 1][left].bucket.add(new List(sub_num_id));
                bucketlist[stock_id][i][LEVEL - 1][right].bucket.add(new List(sub_num_id));
            }
            SubNum[stock_id]++;
        });
        File file = new File("resources/match-time.txt");
        FileWriter fw = null;
        try {
            fw = new FileWriter(file, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        //matcher
        event.foreach((k, v) -> {
            //compute event access delay
            long tmpTime = System.currentTimeMillis();
            v.EventArriveTime = tmpTime - v.EventProduceTime;
            //preprocess
            final  CountDownLatch latch = new CountDownLatch(match_thread_num);
            //System.out.println("Stock Id: " + stock_id + " Attribute Num: " + attributeNum);
            //match
            tmpTime = System.nanoTime();
            for (int i = 0; i < match_thread_num; i++) {
                Parallel s = new Parallel(i, v, latch, producer);
                executorMatch.execute(s);
            }
            try {
                latch.await();
            }catch (Exception e){
                e.printStackTrace();
            }
            for(SubSet.Val s : sets[v.StockId].subset){
                s.matched_num = 0;
            }
            String s = String.valueOf((System.nanoTime() - tmpTime)/1000000.0);
            System.out.println(matchNum);
            matchNum = 0;
            try {
                bw.write(s + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
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
