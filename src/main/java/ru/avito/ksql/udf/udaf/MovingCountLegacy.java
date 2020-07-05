package ru.avito.ksql.udf.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MovingCountLegacy {

    public static final String BUFFER = "BUFFER";
    public static final String OFFSET_TIME = "OFFSET_TIME";
    public static final String OFFSET_POS = "OFFSET_POS";

    public static final Schema bufferSchema = SchemaBuilder.struct().optional()
            .field(OFFSET_TIME, Schema.OPTIONAL_INT64_SCHEMA)
            .field(OFFSET_POS, Schema.OPTIONAL_INT32_SCHEMA)
            .field(BUFFER, SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build())
            .build();

    public static TableUdaf<Long, Struct, Long> moving_count(
            final long STEP,
            final long SIZE
    ) {

        final int STEP_COUNT = (int)(SIZE / STEP);

        return new TableUdaf<Long, Struct, Long>() {

            @Override
            public Struct initialize() {
                return new Struct(bufferSchema)
                        .put(OFFSET_TIME, (System.currentTimeMillis() - SIZE) / STEP * STEP)
                        .put(OFFSET_POS, 0)
                        .put(BUFFER, new ArrayList<Integer>(Collections.nCopies(STEP_COUNT, 0)));
            }

            @Override
            public Struct aggregate(final Long current, final Struct aggregate) {

                if (current == null) {
                    return aggregate;
                }

                List<Integer> buf = aggregate.getArray(BUFFER);
                int bufOffset = aggregate.getInt32(OFFSET_POS);
                long bufTime = aggregate.getInt64(OFFSET_TIME);

                long nowTime = System.currentTimeMillis() / STEP * STEP;
                long valTime = current / STEP * STEP;

                // adjust buffer
                while (nowTime >= bufTime + SIZE) {
                    buf.set(bufOffset, 0);
                    bufOffset = (bufOffset + 1) % STEP_COUNT;
                    bufTime += STEP;
                }
                aggregate.put(OFFSET_POS, bufOffset);
                aggregate.put(OFFSET_TIME, bufTime);

                // seek for position
                int pos = (int)((valTime - bufTime) / STEP);
                if (pos < 0 || pos >= STEP_COUNT) {
                    return aggregate; // out of buffer time bins
                }

                // update counter
                pos = (bufOffset + pos) % STEP_COUNT;
                Integer val = buf.get(pos);
                buf.set(pos, val + 1);

                return aggregate;
            }

            @Override
            public Struct merge(final Struct agg1, final Struct agg2) {
                List<Integer> buf1 = agg1.getArray(BUFFER);
                int bufOffset1 = agg1.getInt32(OFFSET_POS);
                long bufTime1 = agg1.getInt64(OFFSET_TIME);

                List<Integer> buf2 = agg2.getArray(BUFFER);
                int bufOffset2 = agg2.getInt32(OFFSET_POS);
                long bufTime2 = agg2.getInt64(OFFSET_TIME);

                long bufTime = Math.max(bufTime1, bufTime2);
                ArrayList<Integer> buf = new ArrayList<>(STEP_COUNT);
                for (int i = 0; i < STEP_COUNT; ++i) {
                    long t = bufTime + i * STEP;
                    int val = 0;

                    int pos1 = (int)((t - bufOffset1) / STEP);
                    if (pos1 >= 0 && pos1 < STEP_COUNT) {
                        val += buf1.get((bufOffset1 + pos1) % STEP_COUNT);
                    }
                    int pos2 = (int)((t - bufOffset2) / STEP);
                    if (pos2 >= 0 && pos2 < STEP_COUNT) {
                        val += buf2.get((bufOffset2 + pos2) % STEP_COUNT);
                    }
                    buf.add(val);
                }
                return new Struct(bufferSchema)
                        .put(OFFSET_TIME, bufTime)
                        .put(OFFSET_POS, 0)
                        .put(BUFFER, buf);
            }

            @Override
            public Struct undo(final Long valueToUndo, final Struct aggregate) {

                if (valueToUndo == null) {
                    return aggregate;
                }

                long valTime = valueToUndo / STEP * STEP;

                List<Integer> buf = aggregate.getArray(BUFFER);
                int bufOffset = aggregate.getInt32(OFFSET_POS);
                long bufTime = aggregate.getInt64(OFFSET_TIME);

                // seek for position
                int pos = (int)((valTime - bufTime) / STEP);
                if (pos < 0 || pos >= STEP_COUNT) {
                    return aggregate;
                }

                // update counter
                pos = (bufOffset + pos) % STEP_COUNT;
                Integer val = buf.get(pos);
                buf.set(pos, val > 1L ? val - 1 : 0);

                return aggregate;
            }

            @Override
            public Long map(final Struct aggregate) {
                long endTime = System.currentTimeMillis() / STEP * STEP;
                long startTime = endTime - SIZE;

                List<Integer> buf = aggregate.getArray(BUFFER);
                int bufOffset = aggregate.getInt32(OFFSET_POS);
                long bufTime = aggregate.getInt64(OFFSET_TIME);

                Long sum = 0L;
                for (int i = 0; i < buf.size(); ++i) {
                    long t = bufTime + i * STEP;
                    if (t >= startTime && t <= endTime) {
                        int pos = (bufOffset + i) % buf.size();
                        sum += buf.get(pos);
                    }
                }
                return sum;
            }

        };
    }

}