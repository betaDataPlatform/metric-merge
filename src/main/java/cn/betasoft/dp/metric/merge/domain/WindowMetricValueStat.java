package cn.betasoft.dp.metric.merge.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.text.SimpleDateFormat;

@Getter
@Setter
@NoArgsConstructor
public class WindowMetricValueStat {

    private MetricValueAccumulator metricValueAccumulator;

    private long startTime;

    private long endTime;

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return "WindowMetricValueStat{" +
                "startTime=" + sdf.format(startTime) +
                ", endTime=" + sdf.format(endTime) +
                ", accumulator=" + metricValueAccumulator +
                '}';
    }
}
