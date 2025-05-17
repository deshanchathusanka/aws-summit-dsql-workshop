#!/bin/bash

# Error if no Endpoint provided
if [ "$#" -ne 1 ]; then echo "Missing Endpoint. Usage: $0 Endpoint" >&2 && exit 1; fi

# Get region from the Endpoint provided
ENDPOINT_REGION=$(echo $1 | cut -d "." -f 3)

##############################
# Step 1: Generate IAM Token
##############################

# Aurora DSQL requires a valid IAM token as the password when connecting.
# Aurora DSQL provides tools for this and here we're using Python.
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname $1 --expires-in 14400)

##############################
# Step 2: Connect
##############################

# Aurora DSQL requires SSL and will reject your connection without it.
# Aurora DSQL currently only supports "require" mode.
export PGSSLMODE=require

# Connect with psql which will automatically use the values set in PGPASSWORD and PGSSLMODE.
# Quiet mode will suppress unnecessary warnings and chatty responses. Still outputs errors.
psql --host $1 --username admin --dbname postgres --quiet
