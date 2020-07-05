package ru.avito.ksql.udf.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class MovingCount {

    public static final String BUFFER = "BUFFER";
    public static final String TIME = "TIME";

    public static final Schema bufferSchema = SchemaBuilder.struct().optional()
            .field(TIME, Schema.OPTIONAL_INT64_SCHEMA)
            .field(BUFFER, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    public static TableUdaf<Long, Struct, Long> moving_count(final long SIZE) {

        final int STEP = (int)(SIZE / 4);
        final int STEP_BYTES = 2;
        final int STEP_COUNT = 4;

        return new TableUdaf<Long, Struct, Long>() {

            long currentTimeMillis = 0L;

            @Override
            public Struct initialize() {
                return new Struct(bufferSchema).put(TIME, 0L).put(BUFFER, 0L);
            }

            @Override
            public Struct aggregate(final Long current, final Struct aggregate) {

                if (current == null) {
                    return aggregate;
                }

                long bufTime = aggregate.getInt64(TIME);
                long buf = aggregate.getInt64(BUFFER);

                long valTime = current / STEP * STEP;

                long nowTime = bufTime;
                if (valTime > nowTime) {
                    nowTime = valTime;
                    aggregate.put(TIME, nowTime);
                }

                currentTimeMillis = Math.max(currentTimeMillis, valTime);

                int d = (int)((nowTime - bufTime) / STEP);
                buf = d < STEP_COUNT ? buf << (d * 8 * STEP_BYTES) : 0L;

                d = (int)((nowTime - valTime) / STEP);
                buf += d < STEP_COUNT ? 1L << (d * 8 * STEP_BYTES) : 0L;

                aggregate.put(BUFFER, buf);
                return aggregate;
            }

            @Override
            public Struct merge(final Struct agg1, final Struct agg2) {
                long buf1 = agg1.getInt64(BUFFER);
                long bufTime1 = agg1.getInt64(TIME);

                long buf2 = agg2.getInt64(BUFFER);
                long bufTime2 = agg2.getInt64(TIME);

                long nowTime = Math.max(bufTime1, bufTime2);

                int d = (int)((nowTime - bufTime1) / STEP);
                buf1 = d < STEP_COUNT ? buf1 << (d * 8 * STEP_BYTES) : 0L;

                d = (int)((nowTime - bufTime2) / STEP);
                buf2 = d < STEP_COUNT ? buf2 << (d * 8 * STEP_BYTES) : 0L;

                return new Struct(bufferSchema)
                        .put(TIME, nowTime)
                        .put(BUFFER, buf1 + buf2);
            }

            @Override
            public Struct undo(final Long valueToUndo, final Struct aggregate) {

                if (valueToUndo == null) {
                    return aggregate;
                }

                long buf = aggregate.getInt64(BUFFER);
                long bufTime = aggregate.getInt64(TIME);

                long valTime = valueToUndo / STEP * STEP;

                int d = (int)((bufTime - valTime) / STEP);
                if (d < 0 || d >= STEP_COUNT) {
                    return aggregate;
                }

                if (((buf >> (d * 8 * STEP_BYTES)) & ((1 << (8 * STEP_BYTES)) - 1)) != 0) {
                    buf -= 1L << (d * 8 * STEP_BYTES);
                }

                return new Struct(bufferSchema)
                        .put(TIME, bufTime)
                        .put(BUFFER, buf);
            }

            @Override
            public Long map(final Struct aggregate) {
                long buf = aggregate.getInt64(BUFFER);
                long bufTime = aggregate.getInt64(TIME);

                long nowTime = currentTimeMillis;

                int d = (int)((nowTime - bufTime) / STEP);
                if (d > 0 && d < STEP_COUNT) {
                    buf = buf << (d * 8 * STEP_BYTES);
                } else if (d < 0 && d > -STEP_COUNT) {
                    buf = buf >> (-d * 8 * STEP_BYTES);
                } else if (d != 0) {
                    return 0L;
                }

                // BYTE SPECIFIC
                return  (0xffff & buf) +
                        (0xffff & (buf >> 16)) +
                        (0xffff & (buf >> 32)) +
                        (0xffff & (buf >> 48));
            }

        };
    }

}