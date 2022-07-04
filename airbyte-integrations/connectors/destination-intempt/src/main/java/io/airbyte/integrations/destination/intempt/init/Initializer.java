package io.airbyte.integrations.destination.intempt.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.CaseFormat;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class Initializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Initializer.class);

    public Map<String, String> init(String orgName, String apiKey, String sourceId,
                             ConfiguredAirbyteCatalog catalog) {
        final Map<String, JsonNode> collectionMap = createCollection(orgName, apiKey, sourceId, catalog);
        LOGGER.info("Initializing primary identifiers");
        final Map<String, JsonNode> primaryIdMap = createPrimaryId(orgName, apiKey, sourceId, collectionMap);
        LOGGER.info("Initializing Foreign Key identifiers & Relations");
        createForeignIdAndRelations(orgName, apiKey, sourceId, primaryIdMap, collectionMap, catalog);
        LOGGER.info("Initializing Profile identifiers");
        createProfileId(orgName, apiKey, sourceId, collectionMap);
        LOGGER.info("Initializing Profile attributes");
        createProfileAttribute(orgName, apiKey, sourceId, collectionMap);
        return getCollectionId(collectionMap);
    }

    /**
    * Checks if collection exists for each stream. If not, creates it.
    * @return Map of collection name as key, collection object as value.
     */
    protected abstract Map<String, JsonNode> createCollection(
            String orgName, String apiKey, String sourceId, ConfiguredAirbyteCatalog catalog);

    /**
    * @param collectionMap  is map returned by #createCollection method.
    * Checks if primary Identifier exists for each stream. If not, creates it.
    * @return  Map of collection name as key, identifier object as value.
    */
    protected abstract Map<String, JsonNode> createPrimaryId(
            String orgName, String apiKey, String sourceId, Map<String, JsonNode> collectionMap);

    /**
    * @param primaryIdMap is map returned by #createPrimaryId method.
     * @param collectionMap is map returned by #createCollection method.
    * Checks if foreign Identifiers and Relations exists for each stream. If not, creates it.
    */
    protected abstract void createForeignIdAndRelations(String orgName, String apiKey, String sourceId,
                                                        Map<String, JsonNode> primaryIdMap,
                                                        Map<String, JsonNode> collectionMap,
                                                        ConfiguredAirbyteCatalog catalog);

    /**
     * @param collectionMap  is map returned by #createCollection method.
     * Checks if primary Identifier exists. If not, creates it.
     */
    protected abstract void createProfileId(
            String orgName, String apiKey, String sourceId, Map<String, JsonNode> collectionMap);

    /**
     * @param collectionMap  is map returned by #createCollection method.
     * Checks if profile Attribute exists. If not, creates it.
     */
    protected abstract void createProfileAttribute(
            String orgName, String apiKey, String sourceId, Map<String, JsonNode> collectionMap);

    public Map<String, String> getCollectionId(Map<String, JsonNode> collectionMap) {
        return collectionMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().get("id").asText()));
    }


    public String primaryIdName(String collectionName) {
        return CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, collectionName).concat("Id");
    }
}
