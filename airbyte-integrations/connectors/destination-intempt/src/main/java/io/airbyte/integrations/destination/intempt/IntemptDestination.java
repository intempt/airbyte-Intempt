/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.intempt;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.intempt.client.source.SourceService;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntemptDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntemptDestination.class);

  private final SourceService sourceService = new SourceService();

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new IntemptDestination()).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      final String apiKey = config.get("api_key").asText();
      final String orgName = config.get("org_name").asText();
      final String sourceId = config.get("source_id").asText();

      return sourceService.checkById(orgName, apiKey, sourceId);
    } catch (Exception e) {
      return new AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.FAILED)
              .withMessage(e.getMessage());
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog configuredCatalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {
    // TODO
    return null;
  }

}
