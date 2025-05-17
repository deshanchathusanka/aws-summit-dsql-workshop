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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlClient;


public class ClusterConfig {
    private final String endpoint;
    private final Region region;
    private final DsqlClient apiClient;
    private final String jdbcUrl;
    private final String databaseName;
    private final String databaseUsername;


    public ClusterConfig(String endpoint, Region region, DsqlClient apiClient, String databaseName, String databaseUsername) {
        this.region = region;
        this.apiClient = apiClient;
        this.databaseName = databaseName;
        this.databaseUsername = databaseUsername;
        this.endpoint = endpoint;
        this.jdbcUrl = String.format("jdbc:postgresql://%s:5432/%s", endpoint, databaseName);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Region getRegion() {
        return region;
    }

    public DsqlClient getApiClient() {
        return apiClient;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseUsername() {
        return databaseUsername;
    }
}
