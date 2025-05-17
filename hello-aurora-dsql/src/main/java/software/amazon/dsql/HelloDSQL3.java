package software.amazon.dsql;

import org.postgresql.jdbc.SslMode;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;


public class HelloDSQL3 {
    private static final double JITTER_BASE = 20d;
    private static final double JITTER_MAX = 1000 * 5d;


    private static void backoff(int attempt) {
        long duration = (long) (Math.min(JITTER_MAX, JITTER_BASE * Math.pow(2.0d, attempt)) * Math.random());
        try {Thread.sleep(duration);} catch (InterruptedException ignored) {}
    }


    public static void main(String[] args) {

        if (args.length == 0) {
            System.err.println("Cluster endpoint URL is required");
            System.exit(-1);
        }

        String dbEndpoint = args[0];

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load PostgreSQL driver", e);
        }

        DsqlUtilities utilities = DsqlUtilities.builder()
                .region(DefaultAwsRegionProviderChain.builder().build().getRegion())
                .credentialsProvider(DefaultCredentialsProvider.create()).build();

        String password = utilities.generateDbConnectAdminAuthToken(builder -> builder.hostname(dbEndpoint));

        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", password);
        props.setProperty("sslmode", SslMode.REQUIRE.name());

        String jdbcUrl = String.format("jdbc:postgresql://%s:5432/postgres", dbEndpoint);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             PreparedStatement stmt = conn.prepareStatement("update xpoints.customers set age = ? where id = ?");
             AutoCloseable cleanup = conn::rollback) {

            conn.setAutoCommit(false);

            int attempt = 0;
            while (attempt++ < 5) {
                if (attempt > 1)
                    backoff(attempt);

                try {
                    System.out.println("Attempt #" + attempt);
                    stmt.setInt(1, 40);
                    stmt.setObject(2, UUID.fromString("02bd4416-0759-4763-b256-2d97dccf37aa"));
                    stmt.executeUpdate();

                    //
                    // Do all sorts of interesting business logic here!
                    //

                    System.out.println("Sleeping for 15 seconds before committing...");
                    try {Thread.sleep(15000);} catch (InterruptedException ignored) {}

                    conn.commit();
                    System.out.println("Successful commit!");
                    attempt = 5 + 1; // Force attempts above the max
                } catch (SQLException e) {
                    try {conn.rollback();} catch (SQLException ignored) {}

                    if ("40001".equals(e.getSQLState())) {
                        System.err.println("Concurrency collision!");
                        System.err.println();
                    }

                    if (attempt == 5 || !"40001".equals(e.getSQLState())) {
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("MESSAGE:    " + e.getMessage());
            System.err.println("ERROR CODE: " + e.getErrorCode());
            System.err.println("SQL STATE:  " + e.getSQLState());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
