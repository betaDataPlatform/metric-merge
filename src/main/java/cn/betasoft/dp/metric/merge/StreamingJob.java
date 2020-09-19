/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.betasoft.dp.metric.merge;

import cn.betasoft.dp.metric.merge.domain.MetricValuePOJO;
import cn.betasoft.dp.metric.merge.domain.WindowMetricValueStat;
import cn.betasoft.dp.metric.merge.function.ValueAggregateFunction;
import cn.betasoft.dp.metric.merge.function.ValueProcessWindowFunction;
import cn.betasoft.dp.metric.merge.protobuf.MetricValue;
import cn.betasoft.dp.metric.merge.schema.MetricValueDeSerializer;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // flink内部数据写使用kryo,这里需要设置转换条件
        env.getConfig().registerTypeWithKryoSerializer(MetricValue.class, ProtobufSerializer.class);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.39:9095,192.168.31.39:9096");
        properties.setProperty("group.id", "metric-merge");

        FlinkKafkaConsumer<MetricValue> kafkaConsumer = new FlinkKafkaConsumer<>("metric_value", new MetricValueDeSerializer(), properties);
        kafkaConsumer.setStartFromEarliest();

        DataStream<MetricValue> metricValueStream = env.addSource(kafkaConsumer);

        DataStream<WindowMetricValueStat> result = metricValueStream
                .map(metricValue -> {
                    MetricValuePOJO metricValuePOJO = new MetricValuePOJO();
                    metricValuePOJO.setMetricName(metricValue.getMetricName());
                    metricValuePOJO.setMoType(metricValue.getMoType());
                    metricValuePOJO.setMoId(metricValue.getMoId());
                    metricValuePOJO.setMetricValue(metricValue.getMetricValue());
                    metricValuePOJO.setSampleTime(metricValue.getSampleTime());
                    metricValue.getMetricTagsMap().forEach((key, value) -> metricValuePOJO.getMetricTags().put(key, value));
                    return metricValuePOJO;
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MetricValuePOJO>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((event, timestamp) -> event.getSampleTime())
                        .withIdleness(Duration.ofSeconds(20)))
                .keyBy(metricValue -> String.join("|", metricValue.getMetricName(), metricValue.getMoId()))
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .aggregate(new ValueAggregateFunction(),new ValueProcessWindowFunction());
                //.apply(new WindowTest()).name("watermark print");


        result.print().setParallelism(1);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class WindowTest implements WindowFunction<MetricValuePOJO, String, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<MetricValuePOJO> input, Collector<String> out)
                throws Exception {
            TreeSet<Long> set = new TreeSet<>();
            //          元素个数
            int size = Iterables.size(input);
            Iterator<MetricValuePOJO> eles = input.iterator();
            while (eles.hasNext()) {
                set.add(eles.next().getSampleTime());
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            //（code，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
            String first = sdf.format(set.first());
            String last = sdf.format(set.last());
            String start = sdf.format(window.getStart());
            String end = sdf.format(window.getEnd());
            // 调试使用
            out.collect("event.key:" + key + ",window中元素个数：" + size + ",window第一个元素时间戳：" + first + ",window最后一个元素时间戳："
                    + last + ",window开始时间戳：" + start + ",window结束时间戳：" + end + ",窗口内所有的时间戳：" + set.toString());

        }
    }
}
