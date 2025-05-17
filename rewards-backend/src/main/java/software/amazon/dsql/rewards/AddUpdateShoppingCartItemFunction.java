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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;


public class AddUpdateShoppingCartItemFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Logger logger = LoggerFactory.getLogger(AddUpdateShoppingCartItemFunction.class);
    private PreparedStatement getCustStmt;
    private PreparedStatement getCartItemStmt;
    private PreparedStatement insertStmt;
    private PreparedStatement getCatalogItemStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement deleteStmt;

    public AddUpdateShoppingCartItemFunction() {
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

        if (event.getBody() == null) {
            responseEvent.setStatusCode(400);
            return responseEvent;
        }

        String username = getUsername(event);
        if (username == null) {
            logger.error("Unable to determine username from request");
            responseEvent.setBody(makeErrorJson("Unable to determine username from request"));
            responseEvent.setStatusCode(401);
        }

        try {
            Gson gson = new Gson();
            FunctionArguments arguments = gson.fromJson(event.getBody(), FunctionArguments.class);

            if (arguments.getItemId() == null) {
                responseEvent.setBody(makeErrorJson("Catalog item ID required"));
                responseEvent.setStatusCode(400);
                return responseEvent;
            }

            if (arguments.getQuantity() == 0) {
                responseEvent.setStatusCode(200);
                return responseEvent;
            }

            process(arguments, username);
            responseEvent.setStatusCode(200);
        } catch (JsonSyntaxException e) {
            logger.error("Poorly formatted JSON input", e);
            responseEvent.setBody(makeErrorJson("Poorly formatted item ID"));
            responseEvent.setStatusCode(400);
        } catch (IllegalArgumentException e) {
            responseEvent.setBody(makeErrorJson(e.getMessage()));
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private void process(FunctionArguments arguments, String username) {
        int attempt = 0;
        boolean retry = true;
        boolean forceReconnect = false;

        while (retry && ++attempt <= MAX_DB_RETRIES) {
            ResultSet rs1 = null;
            ResultSet rs2 = null;
            ResultSet rs3 = null;
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

                getCartItemStmt.setObject(1, customerId);
                getCartItemStmt.setObject(2, arguments.getItemId());
                rs2 = getCartItemStmt.executeQuery();
                if (rs2.next()) {
                    int netQty = arguments.getQuantity() + rs2.getInt("quantity");
                    if (netQty < 1) {
                        deleteStmt.setObject(1, customerId);
                        deleteStmt.setObject(2, arguments.getItemId());
                        deleteStmt.executeUpdate();
                    } else {
                        getCatalogItemStmt.setObject(1, arguments.getItemId());
                        rs3 = getCatalogItemStmt.executeQuery();
                        if (rs3.next()) {
                            updateStmt.setInt(1, netQty);
                            updateStmt.setObject(2, customerId);
                            updateStmt.setObject(3, arguments.getItemId());
                            updateStmt.executeUpdate();
                        } else {
                            throw new IllegalArgumentException("Catalog item " + arguments.getItemId() + " not found");
                        }
                    }
                } else {
                    if (arguments.getQuantity() > 0) {
                        getCatalogItemStmt.setObject(1, arguments.getItemId());
                        rs3 = getCatalogItemStmt.executeQuery();
                        if (rs3.next()) {
                            insertStmt.setObject(1, customerId);
                            insertStmt.setObject(2, arguments.getItemId());
                            insertStmt.setInt(3, arguments.getQuantity());
                            insertStmt.executeUpdate();
                        } else {
                            throw new IllegalArgumentException("Catalog item " + arguments.getItemId() + " not found");
                        }
                    }
                }

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
                DatabaseUtil.closeQuietly(rs3);
            }

            if (retry) {
                backoff(attempt);
            }
        }
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        connection.setAutoCommit(false);
        getCustStmt = connection.prepareStatement("select id from xpoints.customers where username = ?");
        getCartItemStmt = connection.prepareStatement("select * from xpoints.shopping_cart_items where customer_id = ? and item_id = ?");
        getCatalogItemStmt = connection.prepareStatement("select 1 from xpoints.catalog_items where id = ?");
        insertStmt = connection.prepareStatement("insert into xpoints.shopping_cart_items (customer_id, item_id, quantity) values (?, ?, ?)");
        deleteStmt = connection.prepareStatement("delete from xpoints.shopping_cart_items where customer_id = ? and item_id = ?");
        updateStmt = connection.prepareStatement("update xpoints.shopping_cart_items set quantity = ? where customer_id = ? and item_id = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(getCustStmt);
        DatabaseUtil.closeQuietly(getCartItemStmt);
        DatabaseUtil.closeQuietly(insertStmt);
        DatabaseUtil.closeQuietly(getCatalogItemStmt);
        DatabaseUtil.closeQuietly(updateStmt);
        DatabaseUtil.closeQuietly(deleteStmt);
        super.close();
    }

    static class FunctionArguments {
        private UUID itemId;
        private int quantity = 1;

        public UUID getItemId() {
            return itemId;
        }

        public void setItemId(UUID itemId) {
            this.itemId = itemId;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }
}

