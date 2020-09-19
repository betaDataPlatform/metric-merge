package cn.betasoft.dp.metric.merge.function;

import cn.betasoft.dp.metric.merge.domain.MetricValueAccumulator;
import cn.betasoft.dp.metric.merge.domain.WindowMetricValueStat;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ValueProcessWindowFunction extends
        ProcessWindowFunction<MetricValueAccumulator, WindowMetricValueStat, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<MetricValueAccumulator> elements, Collector<WindowMetricValueStat> out) throws Exception {
        MetricValueAccumulator metricValueAccumulator = elements.iterator().next();
        WindowMetricValueStat windowMetricValueStat = new WindowMetricValueStat();
        windowMetricValueStat.setMetricValueAccumulator(metricValueAccumulator);
        windowMetricValueStat.setStartTime(context.window().getStart());
        windowMetricValueStat.setEndTime(context.window().getEnd());
        out.collect(windowMetricValueStat);
    }
}
