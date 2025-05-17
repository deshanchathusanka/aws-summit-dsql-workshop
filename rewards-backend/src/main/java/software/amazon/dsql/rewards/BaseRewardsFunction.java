/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.dsql.rewards;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.postgresql.jdbc.SslMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public abstract class BaseRewardsFunction {
    private static final Logger logger = LoggerFactory.getLogger(BaseRewardsFunction.class);
    private static final long maxConnectionDurationMillis = 55 * 60 * 1000L;  // 5 minutes less than DSQL's connection timeout

    private static final double JITTER_BASE = 20d;
    private static final double JITTER_MAX = 1000 * 5d;
    protected static final int MAX_DB_RETRIES = 5;

    private final ClusterConfig clusterConfig;
    private final Region currentRegion;
    private String sessionId = "";
    private Connection connection;
    private long connectionStartTime;


    BaseRewardsFunction() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load PostgreSQL driver", e);
        }

        String localClusterEndpoint = System.getenv("CLUSTER_ENDPOINT");
        String dbUsername = System.getenv("DB_USERNAME");
        String dbName = System.getenv("DB_NAME");

        currentRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();

        DsqlClient api = DsqlClient.builder()
                .region(currentRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        clusterConfig = new ClusterConfig(localClusterEndpoint, currentRegion, api, dbName, dbUsername);
    }

    protected Connection getConnection(boolean forceReconnect) throws SQLException {
        if (!forceReconnect && System.currentTimeMillis() < connectionStartTime + maxConnectionDurationMillis) {
            return connection;
        }

        if (connection != null) {
            try {connection.close();} catch (SQLException ignored) {}
            connection = null;
        }

        Properties props = new Properties();

        // Set user for the connection
        props.setProperty("user", clusterConfig.getDatabaseUsername());

        // Set the IAM auth token as the password
        props.setProperty("password", DsqlLib.getPasswordToken(clusterConfig));

        // Note that verify-full does not work, only allow, prefer, require and verify-ca are supported.
        props.setProperty("sslmode", SslMode.REQUIRE.name());

        int attempt = 0;
        while (attempt++ < MAX_DB_RETRIES) {
            if (attempt > 1)
                backoff(attempt);

            try {
                connection = DriverManager.getConnection(clusterConfig.getJdbcUrl(), props);
                connectionStartTime = System.currentTimeMillis();

                sessionId = DsqlLib.getSessionId(connection);
                connectionSetup(connection);

                attempt = MAX_DB_RETRIES + 1;
            } catch (SQLException e) {
                if (attempt == MAX_DB_RETRIES || !DsqlLib.isConcurrencyConflict(e)) {
                    logger.error("Failing at attempt: " + attempt + " with SQL State " + e.getSQLState(), e);
                    throw e;
                } else {
                    logger.warn("Concurrency collision on attempt " + attempt);
                }
            }
        }

        return connection;
    }

    protected String getSessionId() {
        return sessionId;
    }

    protected void connectionSetup(Connection connection) throws SQLException {
    }

    protected void close() {
        DatabaseUtil.closeQuietly(connection);
        try {clusterConfig.getApiClient().close();} catch (Exception ignored) {}
    }

    protected void backoff(int attempt) {
        long duration = (long) (Math.min(JITTER_MAX, JITTER_BASE * Math.pow(2.0d, attempt)) * Math.random());
        try {Thread.sleep(duration);} catch (InterruptedException ignored) {}
    }

    protected ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    protected Region getCurrentRegion() {
        return currentRegion;
    }

    protected String getUsername(APIGatewayProxyRequestEvent event) {
        if (event.getHeaders() == null) {
            return null;
        }

        Map<String, String> headers = event.getHeaders();
        if (!headers.containsKey("authorization")) {
            return null;
        }

        String token = headers.get("authorization");
        if (!token.startsWith("Bearer ")) {
            return null;
        }

        token = token.substring(7);
        String[] chunks = token.split("\\.");
        Base64.Decoder decoder = Base64.getUrlDecoder();
        String payload = new String(decoder.decode(chunks[1]));

        try {
            String username = null;
            JsonObject jsonObject = JsonParser.parseString(payload).getAsJsonObject();
            if (jsonObject.has("username"))
                username = jsonObject.get("username").getAsString();
            else if (jsonObject.has("cognito:username"))
                username = jsonObject.get("cognito:username").getAsString();
            else
                logger.warn("Username not found in payload: " + payload);

            return username;
        } catch (JsonSyntaxException e) {
            logger.error("Error parsing Authorization payload: " + payload, e);
            throw new RuntimeException("Error parsing Authorization payload: " + payload, e);
        }
    }

    protected String makeErrorJson(String message) {
        Gson gson = new Gson();
        JsonObject data = new JsonObject();
        data.addProperty("error", message);
        return gson.toJson(data);
    }

    protected void setCorsHeaders(APIGatewayProxyResponseEvent event) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Credentials", "true");
        headers.put("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type,Authorization,*");
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Expose-Headers", "Date, x-api-id, *");
        event.setHeaders(headers);
    }
}
