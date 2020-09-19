package cn.betasoft.dp.metric.merge.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class MetricValuePOJO {

    private String moType;

    private String metricName;

    private String moId;

    private Double metricValue;

    private long sampleTime;

    private Map<String, String> metricTags = new HashMap<>();
}
