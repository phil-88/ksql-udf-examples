package ru.avito.ksql.udf.udf;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;


@UdfDescription(name = "microcat_node", description = "translates item params to microcat node")
public class MicrocatNode extends InfomodelNode {

    public MicrocatNode() {
        super("microcategories", "master");
    }

    @Override
    public String parsePayload(final JsonNode jn) {
        return Integer.toString(jn.get("id").asInt());
    }

    @Udf(description = "match single infomodel node")
    public String infomodelNode(
            @UdfParameter("cid") final Integer categoryId,
            @UdfParameter("params") final String jsonParams) {
        return super.infomodelNode(categoryId, jsonParams);
    }
}
