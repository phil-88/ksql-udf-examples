package ru.avito.ksql.udf.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "count_10min", description = "Returns a map of each distinct value")
public final class MovingCount10minUdaf extends MovingCount {

    private MovingCount10minUdaf() {}

    @UdafFactory(
            description = "Compute moving count",
            aggregateSchema = "STRUCT<TIME bigint, BUFFER bigint>")
    public static TableUdaf<Long, Struct, Long> moving_count() {
        return moving_count(600000);
    }

}