#!/bin/sh

if [ -z "$ACCESS_TOKEN" ]; then
    echo "You must login first" 1>&2
    exit 1
fi

if [ $# -lt 1 ]; then
    echo "Usage:  remove_item_from_cart.sh cart_item_id"
    echo
    echo "Note that cart_item_id is NOT the product ID.  It is the primary key from the xpoints.shopping_cart_items table that is returned by add_to_cart.sh"
    echo
    exit 1
fi

RESULT=$(curl -s -X DELETE  \
    -H "Authorization: Bearer ${ACCESS_TOKEN}"\
    -H "Content-Type: application/json" \
    "${REWARDS_ENDPOINT}/cart/item/${1}")

if [ -n "$RESULT" ]; then
    echo $RESULT | python3 -m json.tool
fi
