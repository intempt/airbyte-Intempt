package io.airbyte.integrations.destination.intempt.init;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.destination.intempt.client.relation.RelationType;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class StripeInitializer extends Initializer {

    private static final String NAMESPACE = "com.intempt.data.stripe";

    private static final String CUSTOMERS = "customers";

    private static final Logger LOGGER = LoggerFactory.getLogger(StripeInitializer.class);


    @Override
    protected Map<String, JsonNode> createCollection(String orgName, String apiKey, String sourceId,
                                                     ConfiguredAirbyteCatalog catalog) {
        final Map<String, JsonNode> collectionMap = new HashMap<>();
        catalog.getStreams().forEach(airbyteStream -> {
            try {
                final AirbyteStream stream = airbyteStream.getStream();
                final HttpResponse<String> byNameAndSourceId = collectionService.getByNameAndSourceId(
                        orgName, apiKey, sourceId, stream.getName());

                if (collectionService.isEmpty(byNameAndSourceId.body())) {
                    LOGGER.info("Converting schema for {}, with namespace: {}", stream.getName(), NAMESPACE);
                    final Schema schema = schemaConverter.getAvroSchema(
                            stream.getJsonSchema(), stream.getName(), NAMESPACE);

                    final String collection = collectionService.convertToString(
                            stream.getName(), schema, sourceId);
                    final HttpResponse<String> postResponse = collectionService.create(
                            orgName, collection, apiKey);

                    final JsonNode collectionJson = collectionService.extractCollection(postResponse.body());
                    collectionMap.put(stream.getName(), collectionJson);
                    return;
                }
                final JsonNode collectionJson = collectionService.extractCollectionList(byNameAndSourceId.body());
                collectionMap.put(stream.getName(), collectionJson);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        setDisplay(orgName, apiKey, sourceId, CUSTOMERS);
        return collectionMap;
    }

    @Override
    protected Map<String, JsonNode> createPrimaryId(String orgName, String apiKey, String sourceId,
                                                    Map<String, JsonNode> collectionMap) {
        final Map<String, JsonNode> identifierMap = new HashMap<>();
        collectionMap.forEach((collectionName, collection) -> {
            try {
                final HttpResponse<String> id = identifierService.getByCollId(
                        orgName, apiKey, collection.get("id").asText());
                final JsonNode primaryOrNull = identifierService.getPrimaryOrNull(id.body());

                if (primaryOrNull.isNull()) {
                    final String name = primaryIdName(collectionName);
                    final Schema schema = primaryIdSchema(name);
                    final HttpResponse<String> primaryId =
                            identifierService.createPrimaryId(orgName, apiKey, collection, name, schema);
                    final JsonNode primaryIdentifier = objectMapper.readTree(primaryId.body());
                    identifierMap.put(collectionName, primaryIdentifier);
                }
                identifierMap.put(collectionName, primaryOrNull);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return identifierMap;
    }

    @Override
    protected void createForeignIdAndRelations(String orgName, String apiKey, String sourceId,
                                                        Map<String, JsonNode> primaryIdMap,
                                                        Map<String, JsonNode> collectionMap,
                                                        ConfiguredAirbyteCatalog catalog) {
        catalog.getStreams().stream().filter(stream ->
                stream.getStream().getJsonSchema().get("properties").has(CUSTOMERS))
                .forEach(airbyteStream -> {
                    try {
                        final String name = airbyteStream.getStream().getName();
                        final JsonNode collection = collectionMap.get(name);
                        final String primaryId = primaryIdMap.get(CUSTOMERS).get("id").asText();
                        final HttpResponse<String> getByCollId = identifierService.getByCollId(
                                orgName, apiKey, collection.get("id").asText());
                        final JsonNode foreignOrNull = identifierService.getForeignOrNull(getByCollId.body());

                        if (foreignOrNull.isNull()) {
                            final HttpResponse<String> foreignId = identifierService.createForeignId(
                                    orgName, apiKey, collection, foreignIdSchema(), name, primaryId);
                            final JsonNode idNode = objectMapper.readTree(foreignId.body());
                            relationService.create(
                                    orgName, apiKey, name, idNode.get("id").asText(), RelationType.MANY_TO_ONE);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    protected void createProfileId(String orgName, String apiKey, String sourceId,
                                   Map<String, JsonNode> collectionMap) {
        try {
            final JsonNode customers = collectionMap.get(CUSTOMERS);
            final HttpResponse<String> byCollId = identifierService.getByCollId(orgName, apiKey, customers.get("id").asText());
            final JsonNode profileOrNull = identifierService.getProfileOrNull(byCollId.body());
            if (profileOrNull.isNull()) {
                identifierService.createProfileId(
                        orgName, apiKey, customers, "CustomerId", primaryIdSchema("CustomerId"));
            }
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Override
    protected void createProfileAttribute(String orgName, String apiKey, String sourceId,
                                          Map<String, JsonNode> collectionMap) {
        try {
            final JsonNode collection = collectionMap.get(CUSTOMERS);
            final HttpResponse<String> byCollId = profileAttributeService.getByCollId(
                    orgName, apiKey, collection.get("id").asText());
            if (!profileAttributeService.contains(byCollId.body(), "email")) {
                profileAttributeService.create(orgName, apiKey, collection, "email");
            }
            if (!profileAttributeService.contains(byCollId.body(), "phone")) {
                profileAttributeService.create(orgName, apiKey, collection, "phone");
            }
            if (!profileAttributeService.contains(byCollId.body(), "name")) {
                profileAttributeService.create(orgName, apiKey, collection, "name");
            }
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    private Schema primaryIdSchema(String name) {
        return SchemaBuilder.record(name)
                .namespace(NAMESPACE)
                .fields()
                .optionalString("id")
                .endRecord();
    }

    private Schema foreignIdSchema() {
        return SchemaBuilder.record("customers_id")
                .namespace(NAMESPACE)
                .fields()
                .optionalString("customer")
                .endRecord();
    }
}
