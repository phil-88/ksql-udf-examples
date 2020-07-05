package ru.avito.ksql.udf.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Map;


@UdfDescription(name = "extend_json", description = "converts map to json string")
public class ExtendJson {

    @Udf(description = "extends json string with map fields")
    public String extend_json(@UdfParameter("map") final Map<String, Long> map, @UdfParameter("json") String json) {
        StringBuilder res = new StringBuilder();
        res.append("{");
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            res.append("\"");
            res.append(entry.getKey());
            res.append("\":");
            res.append(entry.getValue());
            res.append(",");
        }
        res.append(json.substring(1));
        return res.toString();
    }
}
