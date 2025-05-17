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
import software.amazon.dsql.rewards.model.CatalogItem;
import software.amazon.dsql.rewards.model.ShoppingCartItem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class GetShoppingCartItemsFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetShoppingCartItemsFunction.class);

    private PreparedStatement statement;

    public GetShoppingCartItemsFunction() {
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
            List<ShoppingCartItem> items = process(username);

            Gson gson = new Gson();
            JsonObject data = new JsonObject();
            data.add("cart", gson.toJsonTree(items));
            responseEvent.setBody(gson.toJson(data));
            responseEvent.setStatusCode(200);
        } catch (JsonSyntaxException e) {
            logger.error("Unable to parse JSON", e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private List<ShoppingCartItem> process(String username) {
        List<ShoppingCartItem> items = new ArrayList<>();

        try {
            getConnection(false);

            statement.setString(1, getCurrentRegion().id());
            statement.setString(2, username);

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    ShoppingCartItem item = new ShoppingCartItem();
                    items.add(item);

                    item.setCustomerId((UUID) rs.getObject("customer_id"));
                    item.setQuantity(rs.getInt("quantity"));

                    CatalogItem catalogItem = new CatalogItem();
                    item.setCatalogItem(catalogItem);

                    catalogItem.setId((UUID) rs.getObject("id"));
                    catalogItem.setName(rs.getString("name"));
                    catalogItem.setDescription(rs.getString("description"));
                    catalogItem.setCategory(rs.getString("category"));
                    catalogItem.setUsdPrice(rs.getBigDecimal("usd_price"));
                    catalogItem.setPointsPrice(rs.getInt("points_price"));
                    catalogItem.setRating(rs.getFloat("rating"));
                    catalogItem.setSku(rs.getString("sku"));
                    catalogItem.setWeight(rs.getFloat("weight"));
                    catalogItem.setWidth(rs.getFloat("width"));
                    catalogItem.setHeight(rs.getFloat("height"));
                    catalogItem.setDepth(rs.getFloat("depth"));
                    catalogItem.setThumbnailUrl(rs.getString("thumbnail_url"));
                }
            }
        } catch (SQLException e) {
            logger.error("SESSION ID:  {}", getSessionId());
            logger.error("ERROR CODE:  {}", e.getErrorCode());
            logger.error("SQL STATE:   {}", e.getSQLState());
            logger.error("Database error", e);
            throw new RuntimeException(e);
        }

        return items;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        statement = connection.prepareStatement("select cart.customer_id, cart.quantity, item.* from xpoints.shopping_cart_items cart inner join xpoints.customers cust on cust.id = cart.customer_id inner join (select ci.*, img.presigned_url thumbnail_url from xpoints.catalog_items ci left outer join xpoints.image_urls img on ci.thumbnail_id = img.image_id and img.region = ?) item on cart.item_id = item.id where cust.username = ? order by item.name");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(statement);
        super.close();
    }
}
