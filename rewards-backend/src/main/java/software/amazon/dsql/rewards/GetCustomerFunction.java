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
import software.amazon.dsql.rewards.model.Address;
import software.amazon.dsql.rewards.model.Customer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class GetCustomerFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(GetCustomerFunction.class);

    private PreparedStatement statement;

    public GetCustomerFunction() {
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
            Gson gson = new Gson();

            Customer customer = process(username);
            if (customer == null) {
                responseEvent.setStatusCode(404);
            } else {
                responseEvent.setBody(gson.toJson(customer));
                responseEvent.setStatusCode(200);
            }
        } catch (JsonSyntaxException e) {
            logger.error(e.getMessage(), e);
            responseEvent.setStatusCode(400);
        }

        return responseEvent;
    }


    private Customer process(String username) {
        Customer customer = null;

        try {
            getConnection(false);
            statement.setString(1, username);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    customer = new Customer();
                    customer.setId((UUID) rs.getObject("id"));
                    customer.setUsername(rs.getString("username"));
                    customer.setFirstName(rs.getString("first_name"));
                    customer.setLastName(rs.getString("last_name"));
                    customer.setMaidenName(rs.getString("maiden_name"));
                    customer.setEmail(rs.getString("email"));
                    customer.setPhone(rs.getString("phone_num"));

                    Address address = new Address();
                    address.setAddress(rs.getString("address"));
                    address.setCity(rs.getString("city"));
                    address.setState(rs.getString("state"));
                    address.setStateCode(rs.getString("state_code"));
                    address.setPostalCode(rs.getString("postal_code"));

                    customer.setAddress(address);
                }
            }
        } catch (SQLException e) {
            logger.error("SESSION ID:  {}", getSessionId());
            logger.error("ERROR CODE:  {}", e.getErrorCode());
            logger.error("SQL STATE:   {}", e.getSQLState());
            throw new RuntimeException(e);
        }

        return customer;
    }

    @Override
    protected void connectionSetup(Connection connection) throws SQLException {
        super.connectionSetup(connection);
        statement = connection.prepareStatement("select * from xpoints.customers where username = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(statement);
        super.close();
    }
}
