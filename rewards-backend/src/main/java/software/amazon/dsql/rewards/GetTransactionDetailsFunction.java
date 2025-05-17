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
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.dsql.rewards.model.OrderItem;
import software.amazon.dsql.rewards.model.TransactionDetails;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;


public class GetTransactionDetailsFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetTransactionDetailsFunction.class);

    private PreparedStatement getTxStmt;
    private PreparedStatement getCustStmt;

    public GetTransactionDetailsFunction() {
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
            if (event.getPathParameters() != null && event.getPathParameters().containsKey("tx_id")) {
                UUID txId = UUID.fromString(event.getPathParameters().get("tx_id"));
                TransactionDetails tx = process(txId, username);

                if (tx == null) {
                    responseEvent.setStatusCode(404);
                } else {
                    Gson gson = new Gson();
                    responseEvent.setBody(gson.toJson(tx));
                    responseEvent.setStatusCode(200);
                }
            } else {
                responseEvent.setStatusCode(404);
            }
        } catch (IllegalArgumentException e) { // Bad UUID string
            responseEvent.setBody(makeErrorJson(e.getMessage()));
            responseEvent.setStatusCode(404);
        } catch (JsonSyntaxException e) {
            logger.error("Error parsing JSON", e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private TransactionDetails process(UUID txId, String username) {
        TransactionDetails tx = null;

        try {
            getConnection(false);

            UUID customerId;
            getCustStmt.setString(1, username);
            try (ResultSet rs = getCustStmt.executeQuery();) {
                if (rs.next()) {
                    customerId = (UUID) rs.getObject(1);
                } else {
                    throw new IllegalArgumentException("Customer " + username + " not found");
                }
            }

            getTxStmt.setObject(1, txId);
            getTxStmt.setObject(2, customerId);

            try (ResultSet rs = getTxStmt.executeQuery()) {
                while (rs.next()) {
                    if (tx == null) {
                        tx = new TransactionDetails();
                        tx.setId((UUID) rs.getObject("id"));
                        tx.setCustomerId((UUID) rs.getObject("customer_id"));
                        tx.setType(rs.getString("tx_type"));
                        tx.setDescription(rs.getString("tx_description"));
                        tx.setPoints(rs.getLong("points"));
                        if (rs.getTimestamp("tx_dt") != null)
                            tx.setTimestamp(rs.getTimestamp("tx_dt"));
                    }

                    if (rs.getObject("cat_item_id") != null) {
                        OrderItem item = new OrderItem();
                        tx.addOrderItem(item);
                        item.setCatalogItemId((UUID) rs.getObject("cat_item_id"));
                        item.setPointsPrice(rs.getLong("unit_points_price"));
                        item.setQuantity(rs.getInt("unit_cnt"));
                        item.setName(rs.getString("item_name"));
                        item.setDescription(rs.getString("item_description"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SESSION ID:  {}", getSessionId());
            logger.error("ERROR CODE:  {}", e.getErrorCode());
            logger.error("SQL STATE:   {}", e.getSQLState());
            logger.error("Database error", e);
            throw new RuntimeException(e);
        }

        return tx;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        getCustStmt = connection.prepareStatement("select id from xpoints.customers where username = ?");
        getTxStmt = connection.prepareStatement("select t.*, oi.cat_item_id, oi.unit_cnt, oi.unit_points_price, oi.item_name, oi.item_description from xpoints.transactions t left outer join (select o.tx_id, o.cat_item_id, o.unit_cnt, o.unit_points_price, ci.name item_name, ci.description item_description from xpoints.order_items o inner join xpoints.catalog_items ci on ci.id = o.cat_item_id order by ci.name) oi on t.id = oi.tx_id where t.id = ? and t.customer_id = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(getCustStmt);
        DatabaseUtil.closeQuietly(getTxStmt);
        super.close();
    }
}
