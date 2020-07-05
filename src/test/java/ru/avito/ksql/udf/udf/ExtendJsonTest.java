package ru.avito.ksql.udf.udf;

import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ExtendJsonTest {

    @Test
    public void shouldExtend() {
        HashMap<String, Long> map = new HashMap<>();
        map.put("a", 1L);
        map.put("b", null);

        ExtendJson e = new ExtendJson();
        String jj = e.extend_json(map, "{\"c\": 3}");

        assertThat(jj).isEqualTo("{\"a\":1,\"c\": 3}");
    }
}
