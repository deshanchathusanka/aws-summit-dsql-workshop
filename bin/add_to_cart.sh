#!/bin/sh

if [ -z "$ACCESS_TOKEN" ]; then
    echo "You must login first" 1>&2
    exit 1
fi

if [ $# -lt 1 ]; then
    echo "Usage:  add_to_cart.sh product_id [quantity]" 1>&2
    exit 1
fi

if [ $# -gt 1 ]; then
    QUANTITY=$2
else
    QUANTITY=1
fi

DATA=$(cat<<EOF
{
    "itemId": "$1",
    "quantity": $QUANTITY
}
EOF
)

RESULT=$(curl -s -X POST --data "${DATA}" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}"\
    -H "Content-Type: application/json" \
    "${REWARDS_ENDPOINT}/cart/item")
    
if [ -n "$RESULT" ]; then
    echo $RESULT | python3 -m json.tool
fi