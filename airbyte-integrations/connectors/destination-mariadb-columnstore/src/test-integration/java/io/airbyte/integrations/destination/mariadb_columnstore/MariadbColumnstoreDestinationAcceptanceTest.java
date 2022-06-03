///*
// * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
// */
//
//package io.airbyte.integrations.destination.mariadb_columnstore;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.google.common.collect.ImmutableMap;
//import io.airbyte.commons.json.Jsons;
//import io.airbyte.db.factory.DataSourceFactory;
//import io.airbyte.db.factory.DatabaseDriver;
//import io.airbyte.db.jdbc.DefaultJdbcDatabase;
//import io.airbyte.db.jdbc.JdbcDatabase;
//import io.airbyte.integrations.base.JavaBaseConstants;
//import io.airbyte.integrations.destination.ExtendedNameTransformer;
//import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
//import io.airbyte.integrations.standardtest.destination.comparator.TestDataComparator;
//import java.sql.SQLException;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.stream.Collectors;
//import org.testcontainers.containers.MariaDBContainer;
//import org.testcontainers.utility.DockerImageName;
//
//public class MariadbColumnstoreDestinationAcceptanceTest extends DestinationAcceptanceTest {
//
//  private final ExtendedNameTransformer namingResolver = new MariadbColumnstoreNameTransformer();
//  private ExecutorService executorService;
//
//  private MariaDBContainer db;
//
//  @Override
//  protected boolean implementsNamespaces() {
//    return true;
//  }
//
//  @Override
//  protected String getImageName() {
//    return "airbyte/destination-mariadb-columnstore:dev";
//  }
//
//  @Override
//  protected JsonNode getConfig() {
//    return Jsons.jsonNode(ImmutableMap.builder()
//        .put("host", db.getHost())
//        .put("port", db.getFirstMappedPort())
//        .put("database", db.getDatabaseName())
//        .put("username", db.getUsername())
//        .put("password", db.getPassword())
//        .build());
//  }
//
//  @Override
//  protected JsonNode getFailCheckConfig() {
//    final JsonNode clone = Jsons.clone(getConfig());
//    ((ObjectNode) clone).put("password", "wrong password");
//    return clone;
//  }
//
//  @Override
//  protected String getDefaultSchema(final JsonNode config) {
//    if (config.get("database") == null) {
//      return null;
//    }
//    return config.get("database").asText();
//  }
//
//  @Override
//  protected TestDataComparator getTestDataComparator() {
//    return new MariaDbTestDataComparator();
//  }
//
//  @Override
//  protected boolean supportBasicDataTypeTest() {
//    return true;
//  }
//
//  @Override
//  protected boolean supportArrayDataTypeTest() {
//    return true;
//  }
//
//  @Override
//  protected boolean supportObjectDataTypeTest() {
//    return true;
//  }
//
//  @Override
//  protected List<JsonNode> retrieveRecords(final TestDestinationEnv testEnv,
//                                           final String streamName,
//                                           final String namespace,
//                                           final JsonNode streamSchema)
//      throws Exception {
//    return retrieveRecordsFromTable(namingResolver.getRawTableName(streamName), namespace)
//        .stream()
//        .map(r -> Jsons.deserialize(r.get(JavaBaseConstants.COLUMN_NAME_DATA).asText()))
//        .collect(Collectors.toList());
//  }
//
//  private List<JsonNode> retrieveRecordsFromTable(final String tableName, final String schemaName) throws SQLException {
//    final JdbcDatabase database = getDatabase(getConfig());
//    final String query = String.format("SELECT * FROM %s.%s ORDER BY %s ASC;", schemaName, tableName, JavaBaseConstants.COLUMN_NAME_EMITTED_AT);
//    return database.queryJsons(query);
//  }
//
//  private static JdbcDatabase getDatabase(final JsonNode config) {
//    return new DefaultJdbcDatabase(
//        DataSourceFactory.create(
//            config.get("username").asText(),
//            config.has("password") ? config.get("password").asText() : null,
//            MariadbColumnstoreDestination.DRIVER_CLASS,
//            String.format(DatabaseDriver.MARIADB.getUrlFormatString(),
//                config.get("host").asText(),
//                config.get("port").asInt(),
//                config.get("database").asText())));
//  }
//
//  @Override
//  protected void setup(final TestDestinationEnv testEnv) throws Exception {
//    executorService = Executors.newFixedThreadPool(1);
//    executorService.submit(() -> {
//      while (true) {
//        System.out.println(" \t Free Memory  \t \t  Total Memory  \t \t  Max Memory");
//        System.out.println("\t " + Runtime.getRuntime().freeMemory() +
//            " \t \t " + Runtime.getRuntime().totalMemory() +
//            " \t \t " + Runtime.getRuntime().maxMemory());
//        Thread.sleep(10000);
//      }
//    });
//
//    final DockerImageName mcsImage = DockerImageName.parse("fengdi/columnstore:1.5.2").asCompatibleSubstituteFor("mariadb");
//    db = new MariaDBContainer(mcsImage);
//    db.start();
//
//    final String createUser = String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", db.getUsername(), db.getPassword());
//    final String grantAll = String.format("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' IDENTIFIED BY '%s';", db.getUsername(), db.getPassword());
//    final String createDb = String.format("CREATE DATABASE %s DEFAULT CHARSET = utf8;", db.getDatabaseName());
//    db.execInContainer("mariadb", "-e", createUser + grantAll + createDb);
//  }
//
//  @Override
//  protected void tearDown(final TestDestinationEnv testEnv) {
//    db.stop();
//    db.close();
//    executorService.shutdownNow();
//  }
//
//}
