package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple3<String, Long, Long>> {
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
    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
            LOG.info("==================== NEW CURRENT ======================");
            LOG.info("KEY:" + current.key);
            LOG.info("==================== FINISHED NEW CURRENT ======================");
            // set the state's timestamp to the record's assigned event time timestamp
            current.firstModified = ctx.timestamp();
            // schedule the next timer 60 seconds from the current processing time
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 60000);
        }

        // update the state's count
        current.count++;

        // update the state's count
        current.txn_amt += value.f1;

        LOG.info("KEY:" + current.key + " COUNT:" + current.count + " TXN_AMT:" + current.txn_amt + " lastModified:" + current.firstModified);

        // write the state back
        state.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        LOG.info("==================== TIMEOUT: " + result.key);
        // emit the state on timeout
        out.collect(new Tuple3<String, Long, Long>(result.key, result.txn_amt, result.count));
        state.clear();
    }
}
