package io.airbyte.integrations.destination.intempt.client.collection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.destination.intempt.client.Service;
import org.apache.avro.Schema;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionService extends Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);

    private static final String PATH = "/collections/";

    public HttpResponse<String> create(String orgName, String body, String apiKey) throws Exception {
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, body);
    }

    public HttpResponse<String> getByNameAndSourceId(String orgName, String apiKey, String sourceId, String name)
            throws Exception{
        final URI uriGetByName = createUriGetByName(orgName, sourceId, name);
        return makeGetRequest(apiKey, uriGetByName);
    }

    public HttpResponse<String> update(String orgName, String apiKey, String collId, String body) throws Exception {
        final URI uriPut = createUriPut(orgName, collId);
        return makePutRequest(apiKey, uriPut, body);
    }

    public boolean isEmpty(String body) throws JsonProcessingException {
        JsonNode collectionList = objectMapper.readTree(body).get("_embedded").get("collections");
        return collectionList.isEmpty();
    }

    public JsonNode extractCollectionList(String body) throws JsonProcessingException {
        return objectMapper.readTree(body).get("_embedded").get("collections").get(0);
    }

    public JsonNode extractCollection(String body) throws JsonProcessingException {
        return objectMapper.readTree(body);
    }

    /**
     * Updates schema of a collection. Adds 'display: true' property to all fields.
     * These fields will be visible in the event editor.
     */
    public void setDisplay(String orgName, String apiKey, String sourceId, String collectionName) throws Exception {
        LOGGER.info("Setting display property for collection {}", collectionName);
        final HttpResponse<String> customersResponse = getByNameAndSourceId(orgName, apiKey, sourceId, collectionName);
        final JsonNode collection = extractCollectionList(customersResponse.body());
        final JsonNode schema = collection.get("schema");

        ArrayNode arrayNode = objectMapper.createArrayNode();
        List<JsonNode> list = new ArrayList<>();
        schema.get("fields").forEach(field -> {
            ((ObjectNode) field).set("display", BooleanNode.getTrue());
            list.add(field);
        });
        arrayNode.addAll(list);
        ((ObjectNode) schema).set("fields", arrayNode);

        String collectionString = convertToString(collectionName, schema, sourceId);
        update(orgName, apiKey, collection.get("id").asText(), collectionString);
    }

    public String convertToString(String name, Schema schema, String sourceId) throws JsonProcessingException {
        return convertToString(name, objectMapper.readTree(schema.toString()), sourceId);
    }

    public String convertToString(String name, JsonNode schema, String sourceId) throws JsonProcessingException {
        final Map<Object, Object> map = new HashMap<>();
        map.put("name", name);
        map.put("title", name);
        map.put("schema", schema);
        map.put("sourceId", sourceId);

        return objectMapper.writeValueAsString(map);
    }

    protected URI createUriGetByName(String orgName, String sourceId, String name) throws URISyntaxException {
        return new URIBuilder(HOST + orgName + PATH)
                .addParameter("sourceId", sourceId)
                .addParameter("name", name)
                .build();
    }

    protected URI createUriPut(String orgName, String collId) throws URISyntaxException {
        return new URI(HOST + orgName + PATH + collId);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + PATH);
    }
}
