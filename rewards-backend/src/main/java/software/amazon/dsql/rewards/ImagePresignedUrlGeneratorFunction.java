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
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class ImagePresignedUrlGeneratorFunction extends BaseRewardsFunction implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger logger = LoggerFactory.getLogger(ImagePresignedUrlGeneratorFunction.class);

    private final List<IdImageName> images = new ArrayList<>();

    private final String BUCKET = System.getenv("IMAGE_BUCKET");
    private String IMAGE_BASE_KEY = System.getenv("IMAGE_BASE_KEY");
    private int URL_DURATION_MINUTES = 60 * 12;

    public ImagePresignedUrlGeneratorFunction() {
        if (BUCKET == null)
            throw new IllegalArgumentException("IMAGE_BUCKET must be set");

        if (IMAGE_BASE_KEY == null)
            IMAGE_BASE_KEY = "/";
        else if (!IMAGE_BASE_KEY.endsWith("/"))
            IMAGE_BASE_KEY += "/";

        String duration = System.getenv("PRESIGNED_URL_DURATION_MINUTES");
        if (duration != null) {
            try {
                int minutes = Integer.parseInt(duration);
                if (minutes > 0)
                    URL_DURATION_MINUTES = minutes;
            } catch (NumberFormatException ignored) {} // Just take the default value
        }
    }

    @Override
    public Void handleRequest(ScheduledEvent scheduledEvent, Context context) {
        process();
        return null;
    }

    private void process() {
        try (Connection conn = getConnection(false)) {
            loadImages(conn);
            presign();

            conn.setAutoCommit(false);
            saveImageUrls(conn);
        } catch (SQLException e) {
            logger.error("Error connecting to database", e);
            throw new RuntimeException("Error connecting to database", e);
        }
    }

    private void loadImages(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select id, filename FROM xpoints.images")) {
            while (rs.next()) {
                UUID id = (UUID) rs.getObject("id");
                images.add(new IdImageName(id, rs.getString("filename")));
            }
        } catch (SQLException e) {
            logger.error("Unable to read image records", e);
            throw new RuntimeException("Unable to read image records", e);
        }
    }

    private void presign() {
        try (S3Presigner presigner = S3Presigner.create()) {
            for (IdImageName image : images) {
                image.url = fetchPresignedUrl(presigner, image.imageName);
            }
        }
    }

    private String fetchPresignedUrl(S3Presigner presigner, String imageName) {
        GetObjectRequest getObjectRequest =
                GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(IMAGE_BASE_KEY + imageName)
                        .build();

        // Create a GetObjectPresignRequest to specify the signature duration
        GetObjectPresignRequest getObjectPresignRequest =
                GetObjectPresignRequest.builder()
                        .signatureDuration(Duration.ofMinutes(URL_DURATION_MINUTES))
                        .getObjectRequest(getObjectRequest)
                        .build();

        // Generate the presigned request
        PresignedGetObjectRequest presignedGetObjectRequest =
                presigner.presignGetObject(getObjectPresignRequest);

        return presignedGetObjectRequest.url().toString();
    }

    private void saveImageUrls(Connection conn) throws SQLException {
        int attempt = 0;

        String region = getCurrentRegion().id();

        try (PreparedStatement select = conn.prepareStatement("select 1 from xpoints.image_urls where image_id = ? and region = ?");
             PreparedStatement insert = conn.prepareStatement("insert into xpoints.image_urls (image_id, region, presigned_url) values (?, ?, ?)");
             PreparedStatement update = conn.prepareStatement("update xpoints.image_urls set presigned_url = ?, created = ? where image_id = ? and region = ?")) {

            while (attempt++ < MAX_DB_RETRIES) {
                if (attempt > 1)
                    backoff(attempt);

                ResultSet rs = null;

                try {
                    for (IdImageName image : images) {
                        select.setObject(1, image.id);
                        select.setString(2, region);
                        rs = select.executeQuery();

                        if (rs.next()) {
                            update.setString(1, image.url);
                            update.setTimestamp(2, Timestamp.from(Instant.now()));
                            update.setObject(3, image.id);
                            update.setString(4, region);
                            update.executeUpdate();
                        } else {
                            insert.setObject(1, image.id);
                            insert.setString(2, region);
                            insert.setString(3, image.url);
                            insert.executeUpdate();
                        }
                        rs.close();
                    }
                    conn.commit();
                    attempt = MAX_DB_RETRIES;
                } catch (SQLException e) {
                    DatabaseUtil.rollbackQuietly(conn);
                    if (attempt == MAX_DB_RETRIES || !DsqlLib.isConcurrencyConflict(e)) {
                        logger.error("Failing at attempt: " + attempt + " with SQL State " + e.getSQLState(), e);
                        throw new RuntimeException(e);
                    } else {
                        logger.warn("Concurrency collision on attempt " + attempt);
                    }
                } finally {
                    DatabaseUtil.closeQuietly(rs);
                }
            }
        }
    }

    private class IdImageName {
        private UUID id;
        private String imageName;
        private String url;

        public IdImageName(UUID id, String imageName){
            this.id = id;
            this.imageName = imageName;
        }
    }
}
