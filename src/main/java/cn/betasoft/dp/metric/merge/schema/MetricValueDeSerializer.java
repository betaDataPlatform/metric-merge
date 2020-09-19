package cn.betasoft.dp.metric.merge.schema;

import cn.betasoft.dp.metric.merge.protobuf.MetricValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

@Slf4j
public class MetricValueDeSerializer implements DeserializationSchema<MetricValue> {

    @Override
    public MetricValue deserialize(byte[] bytes) throws IOException {
        try {
            return MetricValue.parseFrom(bytes);
        } catch (Exception ex) {
            log.error("Received unparseable message", ex);
            throw new RuntimeException("Received unparseable message " + ex.getMessage(), ex);
        }
    }

    @Override
    public boolean isEndOfStream(MetricValue metricValue) {
        return false;
    }

    @Override
    public TypeInformation<MetricValue> getProducedType() {
        return TypeInformation.of(MetricValue.class);
    }
}
