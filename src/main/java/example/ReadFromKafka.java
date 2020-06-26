package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

public class ReadFromKafka {

//    final static Logger logger = LoggerFactory.getLogger(ReadFromKafka.class);

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        //// VARIABLES
        //// TEST IN MY LOCAL
        String kafka_consumer_topic = "fafaform_kafka";
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //// TEST IN CLUSTER
//        String kafka_consumer_topic = "flink-from-kafka";
//        properties.setProperty("bootstrap.servers", "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092");
        ////
        properties.setProperty("group.id", "flink-gid");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer(kafka_consumer_topic, new SimpleStringSchema(), properties);
        DataStream<String> messageStream = env.addSource(kafkaSource);

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
        /////////////////// TESTING
        DataStream<Tuple2<String, Long>> accessCounts = messageStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception{
                out.collect(new Tuple2<String, Integer>(line, 1));
            }
        })
                .keyBy(0).process(new CountWithTimeoutFunction());
        accessCounts.rebalance().print();
        ///////////////////

        env.execute("Read from kafka");
    }
}
