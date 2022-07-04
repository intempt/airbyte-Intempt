package io.airbyte.integrations.destination.intempt.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpException;
import software.amazon.awssdk.http.HttpStatusCode;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public abstract class Service {

    public static final String HOST =
            "https://api.staging.intempt.com/v1/";

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final HttpClient client = HttpClient.newHttpClient();


    public HttpResponse<String> isOK(HttpResponse<String> response) throws HttpException {
        if (response.statusCode() != HttpStatusCode.OK) {
            throw new HttpException(response.body() + "\nstatus-code: " + response.statusCode());
        }
        return response;
    }

    protected HttpResponse<String> makePostRequest(String apiKey, URI uri, String body) throws Exception {
        final HttpRequest postRequest = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .build();

        final HttpResponse<String> response = client.send(postRequest, HttpResponse.BodyHandlers.ofString());
        return isOK(response);
    }

    protected HttpResponse<String> makeGetRequest(String apiKey, URI uri) throws Exception{
        final HttpRequest getRequest = HttpRequest.newBuilder(uri)
                .GET()
                .header("Authorization", "Bearer " + apiKey)
                .build();

        final HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
        return isOK(response);
    }

    protected HttpResponse<String> makePutRequest(String apiKey, URI uri, String body) throws Exception {
        final HttpRequest putRequest = HttpRequest.newBuilder(uri)
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .build();

        final HttpResponse<String> response = client.send(putRequest, HttpResponse.BodyHandlers.ofString());
        return isOK(response);
    }

    protected abstract URI createUri(String orgName) throws URISyntaxException;
}
