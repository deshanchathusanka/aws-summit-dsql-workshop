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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class ShoppingCartCheckoutFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(ShoppingCartCheckoutFunction.class);

    private PreparedStatement getCustStmt;
    private PreparedStatement getCartItemsStmt;
    private PreparedStatement getBalanceStmt;
    private PreparedStatement updateBalanceStmt;
    private PreparedStatement insertTxStmt;
    private PreparedStatement insertOrderItemStmt;
    private PreparedStatement deleteCartItemStmt;

    public ShoppingCartCheckoutFunction() {
        super();
        try {
            getConnection(false);
        } catch (SQLException e) {
            logger.error("Error initializing database connection.", e);
            throw new RuntimeException("Error intializing database connection", e);
        }
    }

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
            UUID txId = process(username);

            Gson gson = new Gson();
            JsonObject data = new JsonObject();

            if (txId == null) {
                data.addProperty("message", "Empty cart. Nothing to do.");
            } else {
                data.addProperty("txId", txId.toString());
            }
            responseEvent.setBody(gson.toJson(data));
            responseEvent.setStatusCode(200);
        } catch (JsonSyntaxException e) {
            logger.error("Unable to parse JSON", e);
            responseEvent.setStatusCode(400);
        } catch (IllegalArgumentException e) {
            responseEvent.setBody(makeErrorJson(e.getMessage()));
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private UUID process(String username) {
        int attempt = 0;
        boolean retry = true;
        boolean forceReconnect = false;

        UUID transactionId = null;

        while (retry && ++attempt <= MAX_DB_RETRIES) {
            ResultSet rs1 = null;
            ResultSet rs2 = null;
            Connection conn = null;

            try {
                conn = getConnection(forceReconnect);

                UUID customerId;
                getCustStmt.setString(1, username);
                rs1 = getCustStmt.executeQuery();
                if (rs1.next()) {
                    customerId = (UUID) rs1.getObject(1);
                } else {
                    throw new IllegalArgumentException("Customer " + username + " not found");
                }

                getCartItemsStmt.setObject(1, customerId);
                rs1 = getCartItemsStmt.executeQuery();

                long pointsTotal = 0L;
                int cnt = 0;
                List<CartItemRow> cartItems = new ArrayList<>();

                while (rs1.next()) {
                    CartItemRow ci = new CartItemRow((UUID) rs1.getObject("customer_id"),
                            (UUID) rs1.getObject("item_id"), rs1.getInt("quantity"), rs1.getInt("points_price"));

                    cartItems.add(ci);
                    pointsTotal += (long) ci.pointsPrice * ci.quantity;
                    cnt++;
                }

                // Nothing to do with an empty cart
                if (cnt == 0) {
                    conn.commit();
                    return null;
                }

                long pointsBalance = 0L;
                getBalanceStmt.setObject(1, customerId);
                rs2 = getBalanceStmt.executeQuery();
                if (rs2.next()) {
                    pointsBalance = rs2.getLong("points_balance");
                }

                if (pointsTotal > pointsBalance) {
                    conn.commit();
                    throw new IllegalArgumentException("Insufficient points to complete order");
                }

                transactionId = UUID.randomUUID();
                for (CartItemRow ci : cartItems) {
                    insertOrderItemStmt.setObject(1, transactionId);
                    insertOrderItemStmt.setObject(2, ci.catalogItemId);
                    insertOrderItemStmt.setInt(3, ci.quantity);
                    insertOrderItemStmt.setInt(4, ci.pointsPrice);
                    insertOrderItemStmt.executeUpdate();

                    deleteCartItemStmt.setObject(1, customerId);
                    deleteCartItemStmt.setObject(2, ci.catalogItemId);
                    deleteCartItemStmt.executeUpdate();
                }

                insertTxStmt.setObject(1, transactionId);
                insertTxStmt.setObject(2, customerId);
                insertTxStmt.setString(3, "SPEND");
                insertTxStmt.setLong(4, pointsTotal * -1);
                insertTxStmt.executeUpdate();

                updateBalanceStmt.setLong(1, pointsTotal);
                updateBalanceStmt.setObject(2, customerId);
                updateBalanceStmt.executeUpdate();

                conn.commit();
                retry = false;
            } catch (SQLException e) {
                DatabaseUtil.rollbackQuietly(conn);
                if (DsqlLib.isConcurrencyConflict(e)) {
                    logger.warn("Concurrency conflict: {}", e.getMessage());
                } else {
                    logger.error("SESSION ID:  {}", getSessionId());
                    logger.error("ERROR CODE:  {}", e.getErrorCode());
                    logger.error("SQL STATE:   {}", e.getSQLState());
                    logger.error("Database error", e);

                    if (DsqlLib.isConnectionError(e) && attempt < MAX_DB_RETRIES) {
                        forceReconnect = true;
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                DatabaseUtil.closeQuietly(rs1);
                DatabaseUtil.closeQuietly(rs2);
            }

            if (retry) {
                backoff(attempt);
            }
        }

        return transactionId;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        connection.setAutoCommit(false);

        getCustStmt = connection.prepareStatement("select id from xpoints.customers where username = ?");
        getCartItemsStmt = connection.prepareStatement("select cart.*, cat.points_price from xpoints.shopping_cart_items cart inner join xpoints.catalog_items cat on cart.item_id = cat.id where cart.customer_id = ?");
        getBalanceStmt = connection.prepareStatement("select points_balance from xpoints.points_balances where customer_id = ?");
        insertOrderItemStmt = connection.prepareStatement("insert into xpoints.order_items (tx_id, cat_item_id, unit_cnt, unit_points_price) values(?, ?, ?, ?)");
        insertTxStmt = connection.prepareStatement("insert into xpoints.transactions (id, customer_id, tx_type, points) values (?, ?, ?, ?)");
        updateBalanceStmt = connection.prepareStatement("update xpoints.points_balances set points_balance = points_balance - ? where customer_id = ?");
        deleteCartItemStmt = connection.prepareStatement("delete from xpoints.shopping_cart_items where customer_id = ? and item_id = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(getCustStmt);
        DatabaseUtil.closeQuietly(getCartItemsStmt);
        DatabaseUtil.closeQuietly(getBalanceStmt);
        DatabaseUtil.closeQuietly(updateBalanceStmt);
        DatabaseUtil.closeQuietly(insertTxStmt);
        DatabaseUtil.closeQuietly(insertOrderItemStmt);
        DatabaseUtil.closeQuietly(deleteCartItemStmt);

        super.close();
    }

    record CartItemRow(UUID customerId, UUID catalogItemId, int quantity, int pointsPrice) {}
}
