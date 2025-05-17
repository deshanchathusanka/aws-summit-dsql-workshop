package software.amazon.dsql.rewards;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;


public class DeleteShoppingCartItemFunction extends BaseRewardsFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(DeleteShoppingCartItemFunction.class);

    private PreparedStatement getCustStmt;
    private PreparedStatement deleteOneStmt;
    private PreparedStatement deleteAllStmt;

    public DeleteShoppingCartItemFunction() {
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
        //setCorsHeaders(responseEvent);

        String username = getUsername(event);
        if (username == null) {
            logger.error("Unable to determine username from request");
            responseEvent.setBody(makeErrorJson("Unable to determine username from request"));
            responseEvent.setStatusCode(401);
        }

        try {
            UUID itemId = null;
            if (event.getPathParameters() != null && event.getPathParameters().containsKey("item_id")) {
                String param = event.getPathParameters().get("item_id");
                if (param != null && !param.isEmpty()) {
                    itemId = UUID.fromString(event.getPathParameters().get("item_id"));
                }
            }

            process(itemId, username);
        } catch (IllegalArgumentException e) {
            responseEvent.setBody(makeErrorJson(e.getMessage()));
            responseEvent.setStatusCode(404);
        }

        return responseEvent;
    }

    private void process(UUID itemId, String username) {
        int attempt = 0;
        boolean retry = true;
        boolean forceReconnect = false;

        while (retry && ++attempt <= MAX_DB_RETRIES) {
            Connection conn = null;

            ResultSet rs = null;

            try {
                conn = getConnection(forceReconnect);

                UUID customerId;
                getCustStmt.setString(1, username);
                rs = getCustStmt.executeQuery();
                if (rs.next()) {
                    customerId = (UUID) rs.getObject(1);
                } else {
                    throw new IllegalArgumentException("Customer " + username + " not found");
                }

                if (itemId == null) {
                    deleteAllStmt.setObject(1, customerId);
                    deleteAllStmt.executeUpdate();
                } else {
                    deleteOneStmt.setObject(1, itemId);
                    deleteOneStmt.setObject(2, customerId);
                    deleteOneStmt.executeUpdate();
                }

                conn.commit();
                retry = false;
            } catch (SQLException e) {
                DatabaseUtil.rollbackQuietly(conn);
                if (DsqlLib.isConcurrencyConflict(e)) {
                    logger.warn("Concurrency conflict: {}", e.getMessage());
                } else {
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
                DatabaseUtil.closeQuietly(rs);
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
        deleteOneStmt = connection.prepareStatement("delete from xpoints.shopping_cart_items where item_id = ? and customer_id = ?");
        deleteAllStmt = connection.prepareStatement("delete from xpoints.shopping_cart_items where customer_id = ?");
    }

    @Override
    protected void close() {
        DatabaseUtil.closeQuietly(getCustStmt);
        DatabaseUtil.closeQuietly(deleteOneStmt);
        DatabaseUtil.closeQuietly(deleteAllStmt);
        super.close();
    }
}
