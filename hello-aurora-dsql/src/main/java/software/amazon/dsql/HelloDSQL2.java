package software.amazon.dsql;

import org.postgresql.jdbc.SslMode;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

import java.sql.*;
import java.util.Properties;


public class HelloDSQL2 {
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

        //
        // Our "password" is an IAM authentication token.  We generate a token using the Aurora DSQL SDK.
        //
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
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select username, first_name, last_name from xpoints.customers limit 5;")) {
            while (rs.next()) {
                System.out.println(rs.getString("username") + ": " + rs.getString("first_name") + " " + rs.getString("last_name"));
            }
        } catch (SQLException e) {
            System.err.println("MESSAGE:    " + e.getMessage());
            System.err.println("ERROR CODE: " + e.getErrorCode());
            System.err.println("SQL STATE:  " + e.getSQLState());
        }
    }
}
