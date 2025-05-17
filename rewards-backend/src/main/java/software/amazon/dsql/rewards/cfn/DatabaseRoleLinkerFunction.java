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
package software.amazon.dsql.rewards.cfn;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.postgresql.jdbc.SslMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.dsql.rewards.DsqlLib;
import software.amazon.dsql.rewards.ClusterConfig;
import software.amazon.dsql.rewards.DatabaseUtil;
import software.amazon.lambda.powertools.cloudformation.AbstractCustomResourceHandler;
import software.amazon.lambda.powertools.cloudformation.Response;

import java.sql.*;
import java.util.*;


public class DatabaseRoleLinkerFunction extends AbstractCustomResourceHandler {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseRoleLinkerFunction.class);

    private static final double JITTER_BASE = 20d;
    private static final double JITTER_MAX = 1000 * 10d;
    private static final int MAX_ATTEMPTS = 5;

    public DatabaseRoleLinkerFunction() {
        super();

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load PostgreSQL driver", e);
        }
    }

    @Override
    protected Response create(CloudFormationCustomResourceEvent event, Context context) {
        String physicalResourceId = "link-db-role" + UUID.randomUUID();

        try {
            process(event.getResourceProperties(), null);
            return Response.success(physicalResourceId);
        } catch (Exception e) {
            logger.error("Error processing create", e);
            return Response.failed(physicalResourceId);
        }
    }

    @Override
    protected Response update(CloudFormationCustomResourceEvent event, Context context) {
        try {
            process(event.getResourceProperties(), event.getOldResourceProperties());
            return Response.success(event.getPhysicalResourceId());
        } catch (Exception e) {
            logger.error("Error processing update", e);
            return Response.failed(event.getPhysicalResourceId());
        }
    }

    @Override
    protected Response delete(CloudFormationCustomResourceEvent event, Context context) {
        try {
            process(null, event.getResourceProperties());
            return Response.success(event.getPhysicalResourceId());
        } catch (Exception e) {
            logger.error("Error processing delete", e);
            return Response.failed(event.getPhysicalResourceId());
        }
    }

    private void process(Map<String, Object> newMap, Map<String, Object> oldMap) throws SQLException {
        List<RolePair> newPairs = makePairs(newMap);
        List<RolePair> oldPairs = makePairs(oldMap);

        List<RolePair> adds = new ArrayList<>();
        List<RolePair> deletes = new ArrayList<>();

        for (RolePair pair : newPairs) {
            if (!oldPairs.contains(pair)) {
                adds.add(pair);
            }
        }

        for (RolePair pair : oldPairs) {
            if (!newPairs.contains(pair)) {
                deletes.add(pair);
            }
        }

        if (adds.isEmpty() && deletes.isEmpty()) {
            return;
        }

        Region myRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();

        DsqlClient api = DsqlClient.builder()
                .region(myRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        ClusterConfig clusterConfig = new ClusterConfig(System.getenv("CLUSTER_ENDPOINT"), myRegion, api, "postgres", "admin");

        Properties props = new Properties();

        // Set user for the connection
        props.setProperty("user", clusterConfig.getDatabaseUsername());

        // Set the IAM auth token as the password
        props.setProperty("password", DsqlLib.getPasswordToken(clusterConfig, 30L, true));

        // Note that verify-full does not work, only allow, prefer, require and verify-ca are supported.
        props.setProperty("sslmode", SslMode.REQUIRE.name());

        Connection conn = null;

        try {
            int attempt = 0;
            while (attempt++ < MAX_ATTEMPTS) {
                if (attempt > 1)
                    backoff(attempt);

                try {
                    conn = DriverManager.getConnection(clusterConfig.getJdbcUrl(), props);
                    attempt = MAX_ATTEMPTS + 1;
                } catch (SQLException e) {
                    if (attempt == MAX_ATTEMPTS || !DsqlLib.isConcurrencyConflict(e)) {
                        logger.error("Failing at attempt: " + attempt + " with SQL State " + e.getSQLState(), e);
                        throw e;
                    } else {
                        logger.warn("Concurrency collision on attempt " + attempt);
                    }
                }
            }

            processRoles(adds, true, conn);
            processRoles(deletes, false, conn);
        } finally {
            DatabaseUtil.closeQuietly(conn);
        }
    }

    private void processRoles(List<RolePair> pairs, boolean isGrant, Connection conn) throws SQLException {
        PreparedStatement ps = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement("select 1 from sys.iam_pg_role_mappings where pg_role_name = ? and arn = ?");
            stmt = conn.createStatement();

            for (RolePair pair : pairs) {
                int attempt = 0;
                while (attempt++ < MAX_ATTEMPTS) {
                    if (attempt > 1)
                        backoff(attempt);

                    try {
                        if (isGrant) {
                            stmt.executeUpdate(String.format("aws iam grant %s to '%s'", pair.dbRole, pair.iamRole));
                        } else {
                            ps.setString(1, pair.dbRole);
                            ps.setString(2, pair.iamRole);
                            rs = ps.executeQuery();
                            if (rs.next()) {
                                stmt.executeUpdate(String.format("aws iam revoke %s from '%s'", pair.dbRole, pair.iamRole));
                            }
                            rs.close();
                        }

                        attempt = MAX_ATTEMPTS + 1;
                    } catch (SQLException e) {
                        if (attempt == MAX_ATTEMPTS || !DsqlLib.isConcurrencyConflict(e)) {
                            logger.error("Failing at attempt: " + attempt + " with SQL State " + e.getSQLState(), e);
                            throw e;
                        } else {
                            logger.warn("Concurrency collision on attempt " + attempt);
                        }
                    }
                }
            }
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(ps);
            DatabaseUtil.closeQuietly(stmt);
        }
    }

    private List<RolePair> makePairs(Map parameters) {
        List<RolePair> pairs = new ArrayList<>();
        if (parameters == null || !parameters.containsKey("RoleMaps"))
            return pairs;

        ArrayList roleMaps = (ArrayList) parameters.get("RoleMaps");
        for (Object obj : roleMaps) {
            RolePair pair = RolePair.fromMap((Map) obj);
            if (pair.isGood()) {
                pairs.add(pair);
            }
        }

        return pairs;
    }

    private static void backoff(int attempt) {
        long duration = (long) (Math.min(JITTER_MAX, JITTER_BASE * Math.pow(2.0d, attempt)) * Math.random());
        try {Thread.sleep(duration);} catch (InterruptedException ignored) {}
    }
}

class RolePair {
    public String dbRole;
    public String iamRole;

    @Override
    public boolean equals(Object obj) {
        RolePair other = (RolePair) obj;
        return (StringUtils.equals(this.dbRole, other.dbRole) && StringUtils.equals(this.iamRole, other.iamRole));
    }

    public String toString() {
        return "DbRole: " + dbRole + ", IamRole: " + iamRole;
    }

    public boolean isGood() {
        return !(dbRole == null && iamRole == null);
    }

    public static RolePair fromMap(Map map) {
        RolePair pair = new RolePair();

        if (map.containsKey("DatabaseRole"))
            pair.dbRole = (String) map.get("DatabaseRole");
        if (map.containsKey("IamRole"))
            pair.iamRole = (String) map.get("IamRole");

        return pair;
    }
}