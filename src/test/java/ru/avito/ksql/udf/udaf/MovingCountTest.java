package ru.avito.ksql.udf.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class MovingCountTest {

    @Test
    public void shouldGetCorrectCount() throws InterruptedException {
        final int stepCount = 4;

        //TableUdaf<Long, Struct, Long> udaf = MovingCountLegacy.moving_count(60000, 600000);
        TableUdaf<Long, Struct, Long> udaf = MovingCount.moving_count(600000);
        Struct a = udaf.initialize();

        long t = System.currentTimeMillis();

        final int valueCount = 5432;//100;
        ArrayList<Long> values = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; ++i) {
            values.add((t + (i - valueCount / 2) * 10000L));
        }

        Long count = 0L;
        for (final Long i: values) {
            a = udaf.aggregate(i, a);
            count = udaf.map(a);
        }

        assertTrue(count >= 60L * (stepCount - 1) / stepCount && count <= 60L);

        /*Thread.sleep(60000L);
        a = udaf.aggregate(System.currentTimeMillis(), a);
        Long count2 = udaf.map(a);

        assertThat(55L, equalTo(count2));*/
    }
}
