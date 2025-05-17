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
import software.amazon.dsql.rewards.model.CatalogImage;
import software.amazon.dsql.rewards.model.CatalogItem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;


public class GetCatalogItemFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetCatalogItemFunction.class);

    private PreparedStatement statement;

    public GetCatalogItemFunction() {
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

        try {
            if (event.getPathParameters() != null && event.getPathParameters().containsKey("item_id")) {
                UUID userId = UUID.fromString(event.getPathParameters().get("item_id"));
                CatalogItem item = process(userId);

                if (item == null) {
                    responseEvent.setStatusCode(404);
                } else {
                    Gson gson = new Gson();
                    responseEvent.setBody(gson.toJson(item));
                    responseEvent.setStatusCode(200);
                }
            } else {
                responseEvent.setStatusCode(404);
            }
        } catch (IllegalArgumentException e) { // Bad UUID string
            logger.error(e.getMessage(), e);
            responseEvent.setStatusCode(404);
        } catch (JsonSyntaxException e) {
            logger.error(e.getMessage(), e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private CatalogItem process(UUID itemId) {
        CatalogItem catalogItem = null;

        try {
            getConnection(false);

            statement.setString(1, getCurrentRegion().id());
            statement.setString(2, getCurrentRegion().id());
            statement.setObject(3, itemId);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    catalogItem = new CatalogItem();
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

                    do {
                        if (rs.getString("image_url") != null) {
                            CatalogImage image = new CatalogImage();

                            image.setId((UUID) rs.getObject("image_id"));
                            image.setImageUrl(rs.getString("image_url"));

                            catalogItem.addImage(image);
                        }
                    } while (rs.next());
                }
            }
        } catch (SQLException e) {
            logger.error("SESSION ID:  {}", getSessionId());
            logger.error("ERROR CODE:  {}", e.getErrorCode());
            logger.error("SQL STATE:   {}", e.getSQLState());
            logger.error("Database error", e);
            throw new RuntimeException(e);
        }

        return catalogItem;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        statement = connection.prepareStatement("select ci.*, img.presigned_url thumbnail_url, ermerg.image_id, ermerg.presigned_url image_url from xpoints.catalog_items ci left outer join xpoints.image_urls img on ci.thumbnail_id = img.image_id and img.region = ? left outer join (select ci.item_id, ci.image_id, img.presigned_url from xpoints.catalog_images ci inner join xpoints.image_urls img on img.image_id = ci.image_id and img.region = ?) ermerg on ci.id = ermerg.item_id where ci.id = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(statement);
        super.close();
    }
}
