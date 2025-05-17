#!/bin/sh

if [ -z "$ACCESS_TOKEN" ]; then
    echo "You must login first" 1>&2
    exit 1
fi

RESULT=$(curl -s -X POST  \
    -H "Content-Type: application/json"\
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    "${REWARDS_ENDPOINT}/cart/checkout")

if [ -n "$RESULT" ]; then
    echo $RESULT | python3 -m json.tool
fi
