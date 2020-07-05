package ru.avito.ksql.udf.udf;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DecodeBase36Test {

    @Test
    public void testDecode() {
        DecodeBase36 d = new DecodeBase36();
        String decimal = d.decode("2asdo4hv");

        assertThat(decimal).isEqualTo("180212165059");
    }
}
