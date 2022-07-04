package io.airbyte.integrations.destination.intempt.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.integrations.destination.intempt.client.attribute.profile.ProfileAttributeService;
import io.airbyte.integrations.destination.intempt.client.collection.CollectionService;
import io.airbyte.integrations.destination.intempt.client.identifier.IdentifierService;
import io.airbyte.integrations.destination.intempt.client.relation.RelationService;
import io.airbyte.integrations.destination.intempt.client.relation.RelationType;
import io.airbyte.integrations.destination.s3.avro.JsonToAvroSchemaConverter;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(StripeInitializer.class);

    private final IdentifierService identifierService = new IdentifierService();

    private final RelationService relationService = new RelationService();

    private final CollectionService collectionService = new CollectionService();

    private final ProfileAttributeService profileAttributeService = new ProfileAttributeService();

    private final JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();

    private final ObjectMapper objectMapper = new ObjectMapper();

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
                    final Schema schema = schemaConverter.getAvroSchema(
                            stream.getJsonSchema(), stream.getName(), NAMESPACE);

                    final String collection = collectionService.convertToString(
                            stream.getName(), schema, sourceId);
                    final HttpResponse<String> postResponse = collectionService.create(
                            orgName, collection, apiKey);

                    final JsonNode collectionJson = collectionService.extractCollection(postResponse.body());
                    collectionMap.put(stream.getName(), collectionJson);
                }
                final JsonNode collectionJson = collectionService.extractCollectionList(byNameAndSourceId.body());
                collectionMap.put(stream.getName(), collectionJson);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
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
                    identifierMap.put(primaryIdentifier.get("name").asText(), primaryIdentifier);
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
                stream.getStream().getJsonSchema().get("properties").has("customer"))
                .forEach(airbyteStream -> {
                    try {
                        final String name = airbyteStream.getStream().getName();
                        final JsonNode collection = collectionMap.get(name);
                        final String primaryId = primaryIdMap.get("customers").get("id").asText();
                        final HttpResponse<String> getByCollId = identifierService.getByCollId(
                                orgName, apiKey, collection.get("id").asText());
                        final JsonNode foreignOrNull = identifierService.getForeignOrNull(getByCollId.body());

                        if (foreignOrNull.isNull()) {
                            final HttpResponse<String> foreignId = identifierService.createForeignId(
                                    orgName, apiKey, collection, foreignIdSchema(), name, primaryId);
                            final JsonNode idNode = objectMapper.readTree(foreignId.body());
                            final HttpResponse<String> response = relationService.create(
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
            collectionService.setDisplay(orgName, apiKey, sourceId);
            final JsonNode customers = collectionMap.get("customers");
            identifierService.createProfileId(
                    orgName, apiKey, customers, "CustomerId", primaryIdSchema("CustomerId"));
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Override
    protected void createProfileAttribute(String orgName, String apiKey, String sourceId,
                                          Map<String, JsonNode> collectionMap) {
        try {
            final JsonNode collection = collectionMap.get("customers");
            profileAttributeService.create(orgName, apiKey, collection, "email");
            profileAttributeService.create(orgName, apiKey, collection, "phone");
            profileAttributeService.create(orgName, apiKey, collection, "name");
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
