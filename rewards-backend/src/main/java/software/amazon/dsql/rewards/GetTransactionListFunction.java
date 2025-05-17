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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.dsql.rewards.model.Transaction;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class GetTransactionListFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetTransactionListFunction.class);

    private PreparedStatement statement;

    public GetTransactionListFunction() {
        super();
        try {
            getConnection(false);
        } catch (SQLException e) {
            logger.error("Error initializing database connection.", e);
            throw new RuntimeException("Error intializing database connection", e);
        }
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        setCorsHeaders(responseEvent);

        String username = getUsername(event);
        if (username == null) {
            logger.error("Unable to determine username from request");
            responseEvent.setBody(makeErrorJson("Unable to determine username from request"));
            responseEvent.setStatusCode(401);
        }

        try {
            Instant toInstant = Instant.now();
            Instant fromInstant = toInstant.minus(30, ChronoUnit.DAYS);

            if (event.getQueryStringParameters() != null) {
                if (event.getQueryStringParameters().containsKey("from")) {
                    fromInstant = Instant.ofEpochMilli(Long.parseLong(event.getQueryStringParameters().get("from")));
                }

                if (event.getQueryStringParameters().containsKey("to")) {
                    toInstant = Instant.ofEpochMilli(Long.parseLong(event.getQueryStringParameters().get("to")));
                }
            }

            List<Transaction> transactions = process(username, fromInstant, toInstant);

            Gson gson = new Gson();
            JsonObject data = new JsonObject();
            data.add("transactions", gson.toJsonTree(transactions));
            responseEvent.setBody(gson.toJson(data));
            responseEvent.setStatusCode(200);
        } catch (IllegalArgumentException e) {
            responseEvent.setBody(makeErrorJson(e.getMessage()));
            responseEvent.setStatusCode(404);
        } catch (JsonSyntaxException e) {
            logger.error("Unable to parse JSON", e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private List<Transaction> process(String username, Instant fromInstant, Instant toInstant) {
        List<Transaction> transactions = new ArrayList<>();

        try {
            getConnection(false);

            statement.setString(1, username);
            statement.setTimestamp(2, Timestamp.from(fromInstant));
            statement.setTimestamp(3, Timestamp.from(toInstant));

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Transaction transaction = new Transaction();
                    transactions.add(transaction);

                    transaction.setId((UUID) rs.getObject("id"));
                    transaction.setCustomerId((UUID) rs.getObject("customer_id"));
                    transaction.setType(rs.getString("tx_type"));
                    transaction.setDescription(rs.getString("tx_description"));
                    transaction.setPoints(rs.getLong("points"));
                    if (rs.getTimestamp("tx_dt") != null)
                        transaction.setTimestamp(rs.getTimestamp("tx_dt"));
                }
            }
        } catch (SQLException e) {
            logger.error("SESSION ID:  {}", getSessionId());
            logger.error("ERROR CODE:  {}", e.getErrorCode());
            logger.error("SQL STATE:   {}", e.getSQLState());
            logger.error("Database error", e);
            throw new RuntimeException(e);
        }

        return transactions;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        statement = connection.prepareStatement("select * from xpoints.transactions tx inner join xpoints.customers c on tx.customer_id = c.id where username = ? and tx_dt >= ? and tx_dt <= ? order by tx_dt desc");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(statement);
        super.close();
    }
}
