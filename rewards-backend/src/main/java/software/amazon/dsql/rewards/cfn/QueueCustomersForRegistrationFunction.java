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
package software.amazon.dsql.rewards.cfn;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.postgresql.jdbc.SslMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.dsql.rewards.ClusterConfig;
import software.amazon.dsql.rewards.DatabaseUtil;
import software.amazon.dsql.rewards.DsqlLib;
import software.amazon.lambda.powertools.cloudformation.AbstractCustomResourceHandler;
import software.amazon.lambda.powertools.cloudformation.Response;

import java.sql.*;
import java.util.*;


public class QueueCustomersForRegistrationFunction extends AbstractCustomResourceHandler {
    private static final Logger logger = LoggerFactory.getLogger(QueueCustomersForRegistrationFunction.class);

    private static final double JITTER_BASE = 20d;
    private static final double JITTER_MAX = 1000 * 10d;
    private static final int MAX_ATTEMPTS = 5;


    public QueueCustomersForRegistrationFunction() {
        super();

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load PostgreSQL driver", e);
        }
    }

    @Override
    protected Response create(CloudFormationCustomResourceEvent event, Context context) {
        String physicalResourceId = "cognito-add-rewards-users-" + UUID.randomUUID();
        try {
            Map<String, String> responseAttrs = Map.of("Password", processUsers());
            return Response.builder()
                    .value(responseAttrs)
                    .status(Response.Status.SUCCESS)
                    .physicalResourceId(physicalResourceId)
                    .build();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return Response.failed(physicalResourceId);
        }
    }

    @Override
    protected Response update(CloudFormationCustomResourceEvent event, Context context) {
        return create(event, context);
    }

    @Override
    protected Response delete(CloudFormationCustomResourceEvent event, Context context) {
        String physicalResourceId = "cognito-add-rewards-users-" + UUID.randomUUID();
        return Response.success(physicalResourceId);
    }

    private String processUsers() {
        String password = makePassword();
        String localClusterEndpoint = System.getenv("CLUSTER_ENDPOINT");
        String dbUsername = System.getenv("DB_USERNAME");
        String dbName = System.getenv("DB_NAME");
        String queueUrl = System.getenv("QUEUE_URL");

        // what is my region
        Region myRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();

        DsqlClient api = DsqlClient.builder()
                .region(myRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        ClusterConfig clusterConfig = new ClusterConfig(localClusterEndpoint, myRegion, api, dbName, dbUsername);

        SqsClient sqs = SqsClient.builder().build();
        Gson gson = new Gson();

        Properties props = new Properties();

        // Set user for the connection
        props.setProperty("user", clusterConfig.getDatabaseUsername());

        // Set the IAM auth token as the password
        props.setProperty("password", DsqlLib.getPasswordToken(clusterConfig, 30L, true));

        // Note that verify-full does not work, only allow, prefer, require and verify-ca are supported.
        props.setProperty("sslmode", SslMode.REQUIRE.name());

        Connection conn = null;

        try {
            int attempt = 0;
            while (attempt++ < MAX_ATTEMPTS) {
                if (attempt > 1)
                    backoff(attempt);

                try {
                    conn = DriverManager.getConnection(clusterConfig.getJdbcUrl(), props);
                    attempt = MAX_ATTEMPTS + 1;
                } catch (SQLException e) {
                    if (attempt == MAX_ATTEMPTS || !DsqlLib.isConcurrencyConflict(e)) {
                        logger.error("Failing at attempt: " + attempt + " with SQL State " + e.getSQLState(), e);
                        throw new RuntimeException(e);
                    } else {
                        logger.warn("Concurrency collision on attempt " + attempt);
                    }
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("select username, email from xpoints.customers")) {

                List<SendMessageBatchRequestEntry> messages = new ArrayList<>();

                int count = 0;
                while (rs.next()) {
                    JsonObject data = new JsonObject();
                    data.addProperty("username", rs.getString("username"));
                    data.addProperty("email", rs.getString("email"));
                    data.addProperty("password", password);

                    messages.add(SendMessageBatchRequestEntry.builder()
                            .messageBody(gson.toJson(data))
                            .id(Integer.toString(++count))
                            .build());

                    if (messages.size() == 10) {
                        SendMessageBatchRequest batch = SendMessageBatchRequest.builder()
                                .entries(messages)
                                .queueUrl(queueUrl)
                                .build();

                        sqs.sendMessageBatch(batch);
                        messages.clear();
                    }
                }

                if (messages.size() > 0) {
                    SendMessageBatchRequest batch = SendMessageBatchRequest.builder()
                            .entries(messages)
                            .queueUrl(queueUrl)
                            .build();

                    sqs.sendMessageBatch(batch);
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        } finally {
            DatabaseUtil.closeQuietly(conn);
        }


        return password;
    }

    private String makePassword() {
        String acctNum = StsClient.create().getCallerIdentity().account();
        return acctNum.substring(0,4) + "-dSqL-" + acctNum.substring(acctNum.length() - 4);
    }

    private static void backoff(int attempt) {
        long duration = (long) (Math.min(JITTER_MAX, JITTER_BASE * Math.pow(2.0d, attempt)) * Math.random());
        try {Thread.sleep(duration);} catch (InterruptedException ignored) {}
    }
}
