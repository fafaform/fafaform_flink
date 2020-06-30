package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ReadFromKafka {

    public static String CARD_NUMBER = "CARD_NUMBER";
    public static String TXN_AMT = "TXN_AMT";
    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "mq-sample100";
//    public static String KAFKA_CONSUMER_TOPIC = "flink-from-kafka";
    public static String KAFKA_PRODUCER_TOPIC = "flink-to-kafka";
    //// TEST IN CLUSTER
    public static String BOOTSTRAP_SERVER = "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092";
    //// TEST IN MY LOCAL
//    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static Logger LOG = LoggerFactory.getLogger(ReadFromKafka.class);

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
//        properties.setProperty("group.id", "flink-gid");

        //// READ FROM EARLIEST HERE
//        properties.setProperty("auto.offset.reset", "earliest");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ////////////////////////////////////////////////////////////////
        //// RECEIVE JSON
//        FlinkKafkaConsumer<ObjectNode> JsonSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new JSONKeyValueDeserializationSchema(false), properties);
//        DataStream<Tuple2<String,Double>> messageStream = env.addSource(JsonSource).flatMap(new FlatMapFunction<ObjectNode, Tuple2<String,Double>>() {
//            @Override
//            public void flatMap(ObjectNode s, Collector<Tuple2<String,Double>> collector) throws Exception {
//                collector.collect(new Tuple2<String, Double>(s.get("value").get(CARD_NUMBER).asText(),s.get("value").get(TXN_AMT).asDouble()));
//            }
//        });

        ////////////////////////////////////////////////////////////////
        //// RECEIVE RAW
        FlinkKafkaConsumer<byte[]> kafkaSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);

        // CONVERTING PROCESS
        DataStream<Tuple2<String, Double>> messageStream = env.addSource(kafkaSource).process(new ProcessFunction<byte[], Tuple2<String, Double>>() {
            @Override
            public void processElement(byte[] s, Context context, Collector<Tuple2<String, Double>> collector) throws Exception {

                TransactionObject transactionObject = new TransactionObject();
                transactionObject.setTXN_ID(Arrays.copyOfRange(s, 0, 9));
                transactionObject.setTIMESTAMP(Arrays.copyOfRange(s, 35, 45));
                transactionObject.setCARD_TYPE(Arrays.copyOfRange(s, 25, 26));
                transactionObject.setCARD_STATUS(Arrays.copyOfRange(s, 26, 27));
                transactionObject.setTXN_AMT(Arrays.copyOfRange(s, 27, 35));
                transactionObject.setCARD_NUMBER(Arrays.copyOfRange(s, 9, 25));

                collector.collect(new Tuple2<String, Double>(transactionObject.getCARD_NUMBER(), transactionObject.getTXN_AMT()));
            }
        });
        ////////////////////////////////////////////////////////////////

        //// PRODUCT KAFKA
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(KAFKA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        DataStream<Tuple3<String, Double, Long>> accessCounts = messageStream
                .keyBy(0).process(new CountWithTimeoutFunction());

        DataStreamSink<String> sendingToKafka = accessCounts.process(new ProcessFunction<Tuple3<String, Double, Long>, String>() {
            @Override
            public void processElement(Tuple3<String, Double, Long> stringLongLongTuple3, Context context, Collector<String> collector) throws Exception {
                collector.collect("{\"CARD_NUMBER\":\"" + stringLongLongTuple3.f0 + "\",\"TOTAL_AMOUNT\":" + stringLongLongTuple3.f1 + ",\"COUNT\":" + stringLongLongTuple3.f2 + "}");
            }
        }).addSink(myProducer);

        env.execute("Read from kafka");
    }
}
