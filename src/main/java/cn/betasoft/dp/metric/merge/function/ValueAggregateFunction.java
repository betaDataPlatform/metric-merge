package cn.betasoft.dp.metric.merge.function;

import cn.betasoft.dp.metric.merge.domain.MetricValueAccumulator;
import cn.betasoft.dp.metric.merge.domain.MetricValuePOJO;
import org.apache.flink.api.common.functions.AggregateFunction;

public class ValueAggregateFunction implements AggregateFunction<MetricValuePOJO, MetricValueAccumulator, MetricValueAccumulator> {

    @Override
    public MetricValueAccumulator createAccumulator() {
        MetricValueAccumulator metricValueAccumulator = new MetricValueAccumulator();
        metricValueAccumulator.setCount(0l);
        metricValueAccumulator.setSum(0.0);
        metricValueAccumulator.setMax(Double.MIN_VALUE);
        metricValueAccumulator.setMin(Double.MAX_VALUE);
        return metricValueAccumulator;
    }

    @Override
    public MetricValueAccumulator add(MetricValuePOJO metricValuePOJO, MetricValueAccumulator accumulator) {
        accumulator.setMetricName(metricValuePOJO.getMetricName());
        accumulator.setMoType(metricValuePOJO.getMoType());
        accumulator.setMoId(metricValuePOJO.getMoId());
        accumulator.setCount(accumulator.getCount() + 1);
        accumulator.setSum(accumulator.getSum() + metricValuePOJO.getMetricValue());
        accumulator.setMin(Math.min(accumulator.getMin(), metricValuePOJO.getMetricValue()));
        accumulator.setMax(Math.max(accumulator.getMax(), metricValuePOJO.getMetricValue()));
        return accumulator;
    }

    @Override
    public MetricValueAccumulator getResult(MetricValueAccumulator accumulator) {
        MetricValueAccumulator metricValueAccumulator = new MetricValueAccumulator();
        metricValueAccumulator.setMetricName(accumulator.getMetricName());
        metricValueAccumulator.setMoType(accumulator.getMoType());
        metricValueAccumulator.setMoId(accumulator.getMoId());
        metricValueAccumulator.setCount(accumulator.getCount());
        metricValueAccumulator.setSum(accumulator.getSum());
        metricValueAccumulator.setMax(accumulator.getMax());
        metricValueAccumulator.setMin(accumulator.getMin());
        metricValueAccumulator.setAvg(accumulator.getSum() / accumulator.getCount());
        return metricValueAccumulator;
    }

    @Override
    public MetricValueAccumulator merge(MetricValueAccumulator a, MetricValueAccumulator b) {
        a.setCount(a.getCount() + b.getCount());
        a.setSum(a.getSum() + b.getSum());
        a.setMax(Math.max(a.getMax(), b.getMax()));
        a.setMin(Math.min(a.getMin(),b.getMin()));
        return a;
    }
}
