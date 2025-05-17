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
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class RegistrationQueueHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger logger = LoggerFactory.getLogger(RegistrationQueueHandler.class);

    private final String cognitoPoolId = System.getenv("COGNITO_POOL_ID");
    private final CognitoIdentityProviderClient cognito;

    public RegistrationQueueHandler() {
        Region myRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();
        cognito = CognitoIdentityProviderClient.builder()
                .region(myRegion)
                .build();
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent sqsEvent, Context context) {
        Gson gson = new Gson();
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            try {
                UserInfo userInfo = gson.fromJson(message.getBody(), UserInfo.class);

                AdminGetUserRequest userRequest = AdminGetUserRequest.builder()
                        .username(userInfo.username())
                        .userPoolId(cognitoPoolId)
                        .build();

                try {
                    cognito.adminGetUser(userRequest);
                } catch (UserNotFoundException e) {
                    logger.info("Creating user " + userInfo.username());

                    AttributeType emailType = AttributeType.builder().name("email").value(userInfo.email()).build();
                    Collection<AttributeType> attributes = new ArrayList<>();
                    attributes.add(emailType);

                    AdminCreateUserRequest acuReq = AdminCreateUserRequest.builder()
                            .userPoolId(cognitoPoolId)
                            .username(userInfo.username())
                            .messageAction(MessageActionType.SUPPRESS)
                            .temporaryPassword(userInfo.password())
                            .userAttributes(attributes)
                            .build();

                    cognito.adminCreateUser(acuReq);

                    AdminSetUserPasswordRequest pwdReq = AdminSetUserPasswordRequest.builder()
                            .userPoolId(cognitoPoolId)
                            .username(userInfo.username())
                            .password(userInfo.password())
                            .permanent(true)
                            .build();

                    cognito.adminSetUserPassword(pwdReq);
                }
            } catch (Exception e) {
                logger.error("Error processing " + message.getMessageId(), e);
                failures.add(SQSBatchResponse.BatchItemFailure.builder().withItemIdentifier(message.getMessageId()).build());
            }
        }

        return SQSBatchResponse.builder().withBatchItemFailures(failures).build();
    }

    record UserInfo (String username, String email, String password) {}
}
