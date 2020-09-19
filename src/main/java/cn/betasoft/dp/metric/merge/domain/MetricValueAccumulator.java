package cn.betasoft.dp.metric.merge.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class MetricValueAccumulator {

    private String moType;

    private String metricName;

    private String moId;

    private long count;

    private double sum;

    private double max;

    private double min;

    private double avg;
}
