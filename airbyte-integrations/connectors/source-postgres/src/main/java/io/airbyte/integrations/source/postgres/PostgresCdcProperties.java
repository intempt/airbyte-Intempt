/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres;

import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CLIENT_KEY_STORE_PASS;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CLIENT_KEY_STORE_URL;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.SSL_MODE;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.TRUST_KEY_STORE_PASS;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.TRUST_KEY_STORE_URL;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.debezium.internals.PostgresConverter;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource.SslMode;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcProperties {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcProperties.class);
  static Properties getDebeziumDefaultProperties(final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();
    final JsonNode dbConfig = database.getDatabaseConfig();
    final Properties props = commonProperties();
    props.setProperty("plugin.name", PostgresUtils.getPluginValue(sourceConfig.get("replication_method")));
    if (sourceConfig.has("snapshot_mode")) {
      // The parameter `snapshot_mode` is passed in test to simulate reading the WAL Logs directly and
      // skip initial snapshot
      props.setProperty("snapshot.mode", sourceConfig.get("snapshot_mode").asText());
    } else {
      props.setProperty("snapshot.mode", "initial");
    }

    props.setProperty("slot.name", sourceConfig.get("replication_method").get("replication_slot").asText());
    props.setProperty("publication.name", sourceConfig.get("replication_method").get("publication").asText());

    props.setProperty("publication.autocreate.mode", "disabled");

    // Check params for SSL connection in config and add properties for CDC SSL connection
    // https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-database-sslmode
    if (!sourceConfig.has(JdbcUtils.SSL_KEY) || sourceConfig.get(JdbcUtils.SSL_KEY).asBoolean()) {
      if (sourceConfig.has(JdbcUtils.SSL_MODE_KEY) && sourceConfig.get(JdbcUtils.SSL_MODE_KEY).has(JdbcUtils.MODE_KEY)) {
        LOGGER.info("dbConfig: {}", dbConfig);

        if (dbConfig.has(SSL_MODE) && !dbConfig.get(SSL_MODE).asText().isEmpty()) {
          LOGGER.debug("sslMode: {}", dbConfig.get(SSL_MODE).asText());
          props.setProperty("database.sslmode", PostgresSource.toSslJdbcParamInternal(SslMode.valueOf(dbConfig.get(SSL_MODE).asText())));
          props.setProperty("database.history.producer.security.protocol", "SSL");
          props.setProperty("database.history.consumer.security.protocol", "SSL");
        }

        if (dbConfig.has(PostgresSource.CA_CERTIFICATE_PATH) && !dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText().isEmpty()) {
          props.setProperty("database.sslrootcert", dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText());
          props.setProperty("database.history.producer.ssl.truststore.location",
              dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText());
          props.setProperty("database.history.consumer.ssl.truststore.location",
              dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText());
          props.setProperty("database.history.producer.ssl.truststore.type", "PKCS12");
          props.setProperty("database.history.consumer.ssl.truststore.type", "PKCS12");

        }
        if (dbConfig.has(TRUST_KEY_STORE_PASS) && !dbConfig.get(TRUST_KEY_STORE_PASS).asText().isEmpty()) {
          props.setProperty("database.ssl.truststore.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());
          props.setProperty("database.history.producer.ssl.truststore.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());
          props.setProperty("database.history.consumer.ssl.truststore.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());
          props.setProperty("database.history.producer.ssl.key.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());
          props.setProperty("database.history.consumer.ssl.key.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());

        }
        if (dbConfig.has(CLIENT_KEY_STORE_URL) && !dbConfig.get(CLIENT_KEY_STORE_URL).asText().isEmpty()) {
          props.setProperty("database.sslkey", Path.of(URI.create(dbConfig.get(CLIENT_KEY_STORE_URL).asText())).toString());
          props.setProperty("database.history.producer.ssl.keystore.location",
              Path.of(URI.create(dbConfig.get(CLIENT_KEY_STORE_URL).asText())).toString());
          props.setProperty("database.history.consumer.ssl.keystore.location",
              Path.of(URI.create(dbConfig.get(CLIENT_KEY_STORE_URL).asText())).toString());
          props.setProperty("database.history.producer.ssl.keystore.type", "PKCS12");
          props.setProperty("database.history.consumer.ssl.keystore.type", "PKCS12");

        }
        if (dbConfig.has(CLIENT_KEY_STORE_PASS) && !dbConfig.get(CLIENT_KEY_STORE_PASS).asText().isEmpty()) {
          props.setProperty("database.sslpassword", dbConfig.get(CLIENT_KEY_STORE_PASS).asText());
          props.setProperty("database.history.producer.ssl.keystore.password", dbConfig.get(CLIENT_KEY_STORE_PASS).asText());
          props.setProperty("database.history.consumer.ssl.keystore.password", dbConfig.get(CLIENT_KEY_STORE_PASS).asText());
        }
      } else {
        props.setProperty("database.ssl.mode", "required");
      }
    }
    return props;
  }

  private static Properties commonProperties() {
    final Properties props = new Properties();
    props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

    props.setProperty("converters", "datetime");
    props.setProperty("datetime.type", PostgresConverter.class.getName());
    props.setProperty("include.unknown.datatypes", "true");
    return props;
  }

  static Properties getSnapshotProperties() {
    final Properties props = commonProperties();
    props.setProperty("snapshot.mode", "initial_only");
    return props;
  }

}
