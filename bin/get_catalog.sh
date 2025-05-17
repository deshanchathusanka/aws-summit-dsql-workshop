#!/bin/sh

if [ -z "$ACCESS_TOKEN" ]; then
    echo "You must login first" 1>&2
    exit 1
fi

RESULT=$(curl -s -H "Authorization: Bearer ${ACCESS_TOKEN}" "${REWARDS_ENDPOINT}/product")

if [ -n "$RESULT" ]; then
    echo $RESULT | python3 -m json.tool
fi