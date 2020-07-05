package ru.avito.ksql.udf.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Map;


@UdfDescription(name = "decode_base36", description = "converts base36 string to decimal string")
public class DecodeBase36 {

    @Udf(description = "extends json string with map fields")
    public String decode(@UdfParameter("base36") final String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.toString(Long.parseLong(s, 36), 10);
        } catch (NumberFormatException ingored) {
            return null;
        }
    }
}
