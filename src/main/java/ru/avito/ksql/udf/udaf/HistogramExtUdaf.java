package ru.avito.ksql.udf.udaf;

import com.google.common.collect.Maps;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.common.protocol.types.Field;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

@UdafDescription(name = "histogram_ext", description = "Returns a map of each distinct value")
public final class HistogramExtUdaf {

    private static final int LIMIT = 10000;

    private HistogramExtUdaf() {}

    @UdafFactory(
            description = "Build a value-to-count histogram of input strings"
    )
    public static TableUdaf<String, Map<String, Long>, Map<String, Long>> histogram_ext() {

        return new TableUdaf<String, Map<String, Long>, Map<String, Long>>() {

            @Override
            public Map<String, Long> initialize() {
                //return Maps.newHashMap();
                return Maps.newHashMapWithExpectedSize(512);
            }

            @Override
            public Map<String, Long> aggregate(final String key, final Map<String, Long> aggregate) {
                if (aggregate.size() < LIMIT || aggregate.containsKey(key)) {
                    aggregate.merge(key, 1L, Long::sum);
                }
                return aggregate;
            }

            @Override
            public Map<String, Long> merge(final Map<String, Long> agg1, final Map<String, Long> agg2) {
                agg2.forEach((k, v) -> {
                    if (agg1.size() < LIMIT || agg1.containsKey(k)) {
                        agg1.merge(k, v, Long::sum);
                    }
                });
                return agg1;
            }

            @Override
            public Map<String, Long> undo(final String key, final Map<String, Long> aggregate) {
                aggregate.compute(key, (k, v) -> (--v < 1) ? null : v);
                return aggregate;
            }

            @Override
            public Map<String, Long> map(final Map<String, Long> aggregate) {
                return aggregate;
            }

        };
    }
}