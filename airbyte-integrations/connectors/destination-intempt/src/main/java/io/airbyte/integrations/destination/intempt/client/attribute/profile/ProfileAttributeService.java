package io.airbyte.integrations.destination.intempt.client.attribute.profile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.destination.intempt.client.Service;
import io.airbyte.integrations.destination.intempt.client.identifier.IdentifierType;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class ProfileAttributeService extends Service {
    private static final String PATH = "/profile-attributes";

    public HttpResponse<String> create(String orgName, String apiKey, JsonNode collection, String fieldName)
            throws Exception{
        final String profileAttribute = createProfileAttribute(collection, fieldName);
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, profileAttribute);
    }

    public HttpResponse<String> getByCollId(String orgName, String apiKey, String collId) throws Exception {
        final URI uriGetByCollId = createUriGetByCollId(orgName, collId);
        return makeGetRequest(apiKey, uriGetByCollId);
    }

    public boolean contains(String body, String name) throws JsonProcessingException {
        final JsonNode attributeList = objectMapper.readTree(body).get("_embedded").get("profileAttributes");

        for (JsonNode attribute: attributeList) {
            if (attribute.get("name").asText().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private String createProfileAttribute(JsonNode coll, String fieldName) throws JsonProcessingException {
        Map<Object, Object> identifier = new HashMap<>();
        identifier.put("orgId", coll.get("orgId"));
        identifier.put("sourceId", coll.get("sourceId"));
        identifier.put("collId", coll.get("id"));
        identifier.put("name", fieldName);
        identifier.put("schema", coll.get("schema"));

        return objectMapper.writeValueAsString(identifier);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + PATH);
    }

    private URI createUriGetByCollId(String orgName, String collId) throws URISyntaxException {
        return new URIBuilder(HOST + orgName + PATH)
                .addParameter("collId", collId)
                .build();
    }
}
