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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;


public class GetCatalogItemListFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetCatalogItemListFunction.class);

    private final List<String> sortFields = Arrays.asList("name", "usd_price", "points_price", "rating");
    private final List<String> sortOrders = Arrays.asList("asc", "desc");
    private final List<String> categories = Arrays.asList("Books", "Electronics", "Clothing", "Home", "Toys", "Sports");


    public GetCatalogItemListFunction() {
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
            Gson gson = new Gson();
            JsonObject data = new JsonObject();

            String sortField = "name";
            String sortOrder = "asc";
            String category = null;

            if (event.getQueryStringParameters() != null) {
                sortField = getValue(event.getQueryStringParameters().get("sortBy"), sortFields, sortFields.getFirst());
                sortOrder = getValue(event.getQueryStringParameters().get("sortOrder"), sortOrders, sortOrders.getFirst());

                if (event.getQueryStringParameters().containsKey("category")) {
                    category = event.getQueryStringParameters().get("category");
                    if (!categories.contains(category)) {
                        List<CatalogItem> items = new ArrayList<>();
                        data.add("products", gson.toJsonTree(items));
                        responseEvent.setBody(gson.toJson(data));
                        responseEvent.setStatusCode(200);
                        return responseEvent;
                    }
                }
            }

            if (event.getPathParameters() != null && event.getPathParameters().containsKey("category")) {
                category = event.getPathParameters().get("category");
                if (!categories.contains(category)) {
                    List<CatalogItem> items = new ArrayList<>();
                    data.add("products", gson.toJsonTree(items));
                    responseEvent.setBody(gson.toJson(data));
                    responseEvent.setStatusCode(200);
                    return responseEvent;
                }
            }

            List<CatalogItem> items = process(sortField, sortOrder, category);
            data.add("products", gson.toJsonTree(items));
            responseEvent.setBody(gson.toJson(data));
            responseEvent.setStatusCode(200);
        } catch (JsonSyntaxException e) {
            logger.error("Invalid JSON", e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }

    private List<CatalogItem> process(String sortField, String sortOrder, String category) {
        List<CatalogItem> catalogItems = new ArrayList<>();
        String regionStr = getCurrentRegion().toString();

        String categoryClause = "";
        if (category != null) {
            categoryClause = "where category = '" + category + "'";
        }

        String query = String.format("select ci.*, img.presigned_url thumbnail_url " +
                "from xpoints.catalog_items ci left outer join xpoints.image_urls img on ci.thumbnail_id = img.image_id" +
                " and img.region = '%s' %s order by %s %s", regionStr, categoryClause, sortField, sortOrder);

        try {
            Connection conn = getConnection(false);

            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    CatalogItem catalogItem = new CatalogItem();
                    catalogItems.add(catalogItem);

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
            throw new RuntimeException(e);
        }

        return catalogItems;
    }

    private String getValue(String str, List<String> allowed, String defaultValue) {
        if (str == null)
            return defaultValue;

        str = str.toLowerCase();
        if (!allowed.contains(str))
            return defaultValue;

        return str;
    }
}
