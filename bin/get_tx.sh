#!/bin/sh

if [ -z "$ACCESS_TOKEN" ]; then
    echo "You must login first" 1>&2
    exit 1
fi

if [ $# -lt 1 ]; then
    echo "Usage:  get_tx.sh transaction_id" 1>&2
    exit 1
fi

RESULT=$(curl -s -H "Authorization: Bearer ${ACCESS_TOKEN}" "${REWARDS_ENDPOINT}/points/tx/${1}")

if [ -n "$RESULT" ]; then
    echo $RESULT | python3 -m json.tool
fi
