package ru.avito.ksql.udf.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.common.Configurable;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class InfomodelNode implements Configurable {

    private final String treeName;
    private final String treeVersion;
    private TreeNode root = null;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public InfomodelNode(String treeName, String treeVersion) {
        this.treeName = treeName;
        this.treeVersion = treeVersion;
    }

    public interface Condition {
        boolean match(Object v);
    }

    public class ConditionExact implements Condition {
        public Integer etalon;

        public ConditionExact(Integer v) {this.etalon = v;}

        @Override
        public boolean match(Object v) {
            return etalon.equals(v);
        }
    }

    public class ConditionList implements Condition {
        public ArrayList<Integer> etalons;

        public ConditionList(ArrayList<Integer> v) { this.etalons = v; }

        @Override
        public boolean match(Object v) {
            return etalons.contains(v);
        }
    }

    public class TreeNode {
        public List<TreeNode> children = new LinkedList<TreeNode>();
        public TreeNode parent = null;

        public Integer id;
        public Integer parentId;
        public String payload;
        public HashMap<String, Condition> conditions;
    }

    @Override
    public void configure(Map<String, ?> map) {

        String TREE_URL = "http://tree.definition.url";

        JSONObject route = new JSONObject();
        route.put("tree", treeName);
        route.put("version", treeVersion);
        JSONObject payload = new JSONObject();
        payload.put("conditions", true);
        payload.put("route", route);

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        try {
            HttpPost request = new HttpPost(TREE_URL);
            StringEntity params = new StringEntity(payload.toString());
            request.addHeader("content-type", "application/json");
            request.addHeader("X-Source", "XXX");
            request.setEntity(params);
            CloseableHttpResponse response = httpClient.execute(request);

            String j = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            root = parseTree(j);
            httpClient.close();

        } catch (Exception ex) {
            // handle exception here
        }
    }

    public boolean isConfigured() {
        return root != null;
    }

    static public ArrayList<Integer> jsonArrayToList(JsonNode arr) {
        ArrayList<Integer> lst = new ArrayList<>();
        for (JsonNode n : arr){
            lst.add(n.asInt());
        }
        return lst;
    }

    public String parsePayload(final JsonNode jn) {
        try {
            Map<String,String> payloadMap = new HashMap<>();
            payloadMap.put("id", Integer.toString(jn.get("id").asInt()));
            payloadMap.put("payload", objectMapper.writeValueAsString(jn.get("payload")));
            return objectMapper.writeValueAsString(payloadMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public TreeNode parseTree(String j) {
        TreeNode root = null;
        HashMap<Integer, TreeNode> nodes = new HashMap<>();

        try {
            JsonNode jsonNode = objectMapper.readTree(j);
            for (JsonNode jn : jsonNode.get("result")) {

                TreeNode n = new TreeNode();
                n.id = jn.get("id").asInt();
                n.parentId = jn.get("parentId").asInt(-1);
                n.payload = parsePayload(jn);

                n.conditions = new HashMap<>();

                JsonNode cond = jn.get("conditions");
                if (cond.has("category")) {
                    JsonNode cat = cond.get("category");
                    if (cat.isInt()) {
                        n.conditions.put("category", new ConditionExact(cat.asInt()));
                    } else if (cat.isArray()) {
                        n.conditions.put("category", new ConditionList(jsonArrayToList(cat)));
                    }
                }

                if (cond.has("attr")) {
                    for (Iterator<Map.Entry<String, JsonNode>> it = cond.get("attr").fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> e = it.next();
                        String name = e.getKey();
                        JsonNode a = e.getValue();
                        if (a.isInt()) {
                            n.conditions.put(name, new ConditionExact(a.asInt()));
                        } else if (a.isArray()) {
                            n.conditions.put(name, new ConditionList(jsonArrayToList(a)));
                        }
                    }
                }

                nodes.put(n.id, n);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (TreeNode n : nodes.values()) {
            TreeNode p = nodes.get(n.parentId);
            if (p != null) {
                p.children.add(n);
                n.parent = p;
            } else if (n.parentId == -1) {
                root = n;
            }
        }

        return root;
    }

    public Map<String, Object> parseJsonParams(final String jsonParams) {
        try {
            return objectMapper.readValue(jsonParams, new TypeReference<Map<String,Object>>(){});
        } catch (IOException e) {
            return null;
        }
    }

    public String infomodelNode(final Integer categoryId, final String jsonParams)
    {
        Map<String, Object> params = (jsonParams != null ? parseJsonParams(jsonParams) : null);
        if (params == null && (categoryId == null || jsonParams != null)) {
            return null;
        }

        String payload = null;

        LinkedList<TreeNode> front = new LinkedList<>(root.children);
        while (!front.isEmpty()) {
            TreeNode n = front.pop();

            for (Map.Entry<String, Condition> e : n.conditions.entrySet()) {
                Object val = e.getKey().equals("category") ? categoryId : (params != null ? params.get(e.getKey()) : null);
                if (val != null && e.getValue().match(val)) {
                    payload = n.payload;
                    front.clear();
                    front.addAll(n.children);
                }
            }
        }

        return payload;
    }
}
