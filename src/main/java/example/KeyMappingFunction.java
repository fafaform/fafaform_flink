package example;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KeyMappingFunction extends KeyedProcessFunction<Tuple, Tuple3<String, Double, Long>, Tuple4<String, Double, Long, String>> {
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

    private ValueState<List<KeyMappingObject>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("keyState", Types.LIST(Types.GENERIC(KeyMappingObject.class))));
    }

    @Override
    public void processElement(Tuple3<String, Double, Long> value, Context ctx, Collector<Tuple4<String, Double, Long, String>> out) throws Exception {
        List<KeyMappingObject> current = state.value();
        KeyMappingObject keyMappingObject = new KeyMappingObject();
        if(current == null){
            current = new ArrayList<KeyMappingObject>();
            LOG.info("_______________ NULL LIST __________________");
        }
        if(current.size() == 0){
            keyMappingObject.setCARD_NUMBER(value.f0);
            keyMappingObject.setSTART_TIME(value.f2);
            keyMappingObject.setEND_TIME(value.f2+60000);
            current.add(keyMappingObject);
            LOG.info("+++++++ SIZE 0 ADD NEW LIST +++++++");
        }else{
            Boolean containData = Boolean.FALSE;
            int i = 0;
            while(i < current.size()){
                if(value.f2 >= current.get(i).getSTART_TIME() && value.f2 <= current.get(i).getEND_TIME()) {
                    containData = Boolean.TRUE;
//                    LOG.info("____ CONTAIN IN LIST ____");
                    break;
                }
                i++;
            }
            if(containData){
                keyMappingObject = current.get(i);
//                LOG.info("======= CONTAIN IN LIST =======");
            }else {
                keyMappingObject.setCARD_NUMBER(value.f0);
                keyMappingObject.setSTART_TIME(value.f2);
                keyMappingObject.setEND_TIME(value.f2+60000);
                current.add(keyMappingObject);
//                LOG.info("+++++++ ADD NEW LIST +++++++");
            }
        }

        LOG.info(keyMappingObject.getCARD_NUMBER()+" : "+keyMappingObject.getKEY_TIME());
        state.update(current);
        out.collect(new Tuple4<String, Double, Long, String>(value.f0, value.f1, value.f2, keyMappingObject.getKEY_TIME()));
    }

//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Double, Long, String>> out) throws Exception {
//
//        state.clear();
//    }
}
