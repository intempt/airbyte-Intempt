package io.airbyte.integrations.destination.intempt.client.push;

import io.airbyte.integrations.destination.intempt.client.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;

public class PushService extends Service {

    public HttpResponse<String> pushData(String orgName, String body, String collectionId, String apiKey)
            throws Exception{
        final URI uri = createUri(orgName, collectionId);
        return makePostRequest(apiKey, uri, body);
    }

    protected URI createUri(String orgName, String collId) throws URISyntaxException {
        return new URI(HOST + orgName + "/collections/" + collId + "/data");
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return null;
    }
}
