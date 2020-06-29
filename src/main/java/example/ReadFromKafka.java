package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

import java.util.Properties;

public class ReadFromKafka {

    public static String CARD_NUMBER = "CARD_NUMBER";
    public static String TXN_AMT = "TXN_AMT";
    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "flink-from-kafka";
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

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //// RECEIVE STRING
//        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer(kafka_consumer_topic, new SimpleStringSchema(), properties);
//        DataStream<Tuple2<String, Integer>> messageStream = env.addSource(kafkaSource)
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception{
//                out.collect(new Tuple2<String, Integer>(line, 1));
//            }
//        });
        //// RECEIVE JSON
        FlinkKafkaConsumer<ObjectNode> JsonSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new JSONKeyValueDeserializationSchema(false), properties);
        DataStream<Tuple2<String,Long>> messageStream = env.addSource(JsonSource).flatMap(new FlatMapFunction<ObjectNode, Tuple2<String,Long>>() {
            @Override
            public void flatMap(ObjectNode s, Collector<Tuple2<String,Long>> collector) throws Exception {
//                LOG.info("Card_number is "+s.get("value").get(CARD_NUMBER));
                collector.collect(new Tuple2<String, Long>(s.get("value").get(CARD_NUMBER).asText(),s.get("value").get(TXN_AMT).asLong()));
            }
        });

        //// PRODUCT KAFKA
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(KAFKA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        /////////////////// EXAMPLE
//        messageStream.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception{
//                return "Kafka and Flink says: " + value;
//            }
//        });
//        messageStream.rebalance().print();
        ///////////////////
        /////////////////// WORKING 1
//        DataStream<Tuple2<String, Integer>> accessCounts = messageStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception{
//                out.collect(new Tuple2<String, Integer>(line, 1));
//            }
//        }).keyBy(0)
//                .sum(1);
//        accessCounts.rebalance().print();
        ///////////////////
        /////////////////// WORKING
        DataStream<Tuple3<String, Long, Long>> accessCounts = messageStream
                .keyBy(0).process(new CountWithTimeoutFunction());
//        accessCounts.rebalance().print();
        ///////////////////
        DataStreamSink<String> sendingToKafka = accessCounts.process(new ProcessFunction<Tuple3<String, Long, Long>, String>() {
            @Override
            public void processElement(Tuple3<String, Long, Long> stringLongLongTuple3, Context context, Collector<String> collector) throws Exception {
                collector.collect("{\"CARD_NUMBER\":\"" + stringLongLongTuple3.f0 + "\",\"TOTAL_AMOUNT\":" + stringLongLongTuple3.f1 + ",\"COUNT\":" + stringLongLongTuple3.f2 + "}");
            }
        }).addSink(myProducer);

        env.execute("Read from kafka");
    }
}
