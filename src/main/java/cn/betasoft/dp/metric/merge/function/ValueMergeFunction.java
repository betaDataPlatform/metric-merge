package cn.betasoft.dp.metric.merge.function;

import cn.betasoft.dp.metric.merge.domain.MetricValueAccumulator;
import cn.betasoft.dp.metric.merge.domain.MetricValuePOJO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ValueMergeFunction extends KeyedProcessFunction<String, MetricValuePOJO, MetricValueAccumulator> {

    private ValueState<Long> countValue;

    private ValueState<Double> sumValue;

    private ValueState<Double> maxValue;

    private ValueState<Double> minValue;

    private ValueState<MetricValuePOJO> metricValuePOJOValue;

    private transient DateTimeFormatter formatter;

    @Override
    public void open(Configuration conf) throws Exception {
        countValue = getRuntimeContext().getState(new ValueStateDescriptor<>("count value", Long.class));
        sumValue = getRuntimeContext().getState(new ValueStateDescriptor<>("sum value", Double.class));
        maxValue = getRuntimeContext().getState(new ValueStateDescriptor<>("max value", Double.class));
        minValue = getRuntimeContext().getState(new ValueStateDescriptor<>("min value", Double.class));

        metricValuePOJOValue = getRuntimeContext().getState(new ValueStateDescriptor<>("metricValuePOJO value", MetricValuePOJO.class));

        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void processElement(MetricValuePOJO metricValue, Context ctx, Collector<MetricValueAccumulator> out) throws Exception {
        Long count = countValue.value();
        if (count == 0) {
            countValue.update(Long.valueOf(1));
            sumValue.update(Double.valueOf(metricValue.getMetricValue()));
            maxValue.update(Double.valueOf(metricValue.getMetricValue()));
            minValue.update(Double.valueOf(metricValue.getMetricValue()));

            metricValuePOJOValue.update(metricValue);
        } else {
            countValue.update(count + 1);
            sumValue.update(sumValue.value() + metricValue.getMetricValue());
            maxValue.update(Math.max(maxValue.value(), metricValue.getMetricValue()));
            minValue.update(Math.min(minValue.value(), metricValue.getMetricValue()));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricValueAccumulator> out) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String waterMark = sdf.format(ctx.timerService().currentWatermark());
        String firing = sdf.format(timestamp);
        log.info("key:{}, waterMark: {}, firing: {}.", ctx.getCurrentKey(), waterMark, firing);

        MetricValueAccumulator metricStat = new MetricValueAccumulator();
        metricStat.setMetricName(metricValuePOJOValue.value().getMetricName());
        metricStat.setMoType(metricValuePOJOValue.value().getMoType());
        metricStat.setMoId(metricValuePOJOValue.value().getMoId());
        metricStat.setCount(countValue.value());
        metricStat.setSum(sumValue.value());
        metricStat.setMax(maxValue.value());
        metricStat.setMin(minValue.value());
        metricStat.setAvg(metricStat.getSum() / metricStat.getCount());

        out.collect(metricStat);
    }
}
