#!/bin/sh

if [ $# -lt 1 ]; then
    echo "Usage:  login.sh username" 1>&2
    exit 1
fi

if [ -z "$COGNITO_LOGIN" ]; then
    echo "The shell session is not initialized" 1>&2
    exit 1
fi

DATA=$(cat<<EOF
{
    "AuthParameters" : {
        "USERNAME" : "$1",
        "PASSWORD" : "$REWARDS_PASSWORD"
    },
    "AuthFlow" : "USER_PASSWORD_AUTH",
    "ClientId" : "$COGNITO_CLIENT_ID"
}
EOF
)

ACCESS_TOKEN=$(curl -s -X POST --data "${DATA}" \
    -H 'X-Amz-Target: AWSCognitoIdentityProviderService.InitiateAuth' \
    -H 'Content-Type: application/x-amz-json-1.1' \
    "$COGNITO_LOGIN" | cut -d':' -f 3 | cut -d'"' -f 2)

if [ -z "$ACCESS_TOKEN" ]; then
    echo "Login failed.  Re-initialize your session and try again." 1>&2
    exit 1
fi

if [ ${#ACCESS_TOKEN} -lt 50 ]; then
    echo $ACCESS_TOKEN 1>&2
    exit 1
fi

echo $ACCESS_TOKEN