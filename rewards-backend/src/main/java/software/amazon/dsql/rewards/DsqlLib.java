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

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public abstract class DsqlLib {
    private final static String DSQL_ADMIN_USER = "admin";


    /**
     * Tries to generate a password token for given cluster endpoint and region with default expiration time.
     *
     * @param config Cluster configuration
     * @return optional password token
     */
    public static String getPasswordToken(ClusterConfig config) {
        return getPasswordToken(config, 30L, false);
    }

    /**
     * Tries to generate a password token for given cluster endpoint and region
     * @param config Cluster configuration
     * @param expiresInSecs The duration of the token before expiration
     * @return optional password token
     */
    public static String getPasswordToken(ClusterConfig config, Long expiresInSecs, boolean isAdmin) {
        DsqlUtilities utilities = DsqlUtilities.builder()
                .region(config.getRegion())
                .credentialsProvider(DefaultCredentialsProvider.create()).build();

        if (isAdmin || DSQL_ADMIN_USER.equals(config.getDatabaseUsername())) {
            return utilities.generateDbConnectAdminAuthToken(builder -> builder.hostname(config.getEndpoint()));
        }

        return utilities.generateDbConnectAuthToken(builder -> builder.hostname(config.getEndpoint()));
    }


    /**
     * Returns the ID of the current DSQL session.
     *
     * @param conn  Open database connection
     * @return  The connection's session ID
     * @throws SQLException For database errors getting the current session
     */
    public static String getSessionId(Connection conn) throws SQLException {
        String sessionId = null;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select sys.current_session_id()")) {
            while (rs.next()) {
                sessionId = rs.getString(1);
            }
        }

        return sessionId;
    }

    public static boolean isConcurrencyConflict(SQLException e) {
        if (e == null || e.getSQLState() == null)
            return false;

        return "40001".equals(e.getSQLState());
    }

    public static boolean isConnectionError(SQLException e) {
        if (e == null || e.getSQLState() == null)
            return false;

        return e.getSQLState().startsWith("08");
    }
}
