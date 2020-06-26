package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Long>> {
    /**
     * The state that is maintained by this process function
     */

    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
            LOG.info("==================== NEW CURRENT ======================");
            current.lastModified = ctx.timestamp();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 60000);
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
//        current.lastModified = ctx.timestamp();
        LOG.info("KEY:" + current.key + " COUNT:" + current.count + " lastModified:" + current.lastModified);

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
//        ctx.timerService().registerProcessingTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        LOG.info("==================== CURRENT TIME:" + timestamp + " ASSISNED ONE: " + result.lastModified);
        // check if this is an outdated timer or the latest timer
        if (timestamp >= result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
            state.clear();
        }
        else{
            out.collect(new Tuple2<String, Long>(result.key, result.lastModified));
        }
    }
}
