#!/bin/sh

echo "Fetching CloudFormation stack output parameters..."
OUTPUT=($(aws cloudformation describe-stacks --stack-name aurora-dsql-rewards --query 'Stacks[0].[Outputs[0:7].OutputValue]' --output text))

export API_ENDPOINT="${OUTPUT[4]}"
export REWARDS_ENDPOINT="${OUTPUT[2]}"
export COGNITO_POOL_ID="${OUTPUT[1]}"
export COGNITO_LOGIN="${OUTPUT[3]}"
export COGNITO_CLIENT_ID="${OUTPUT[0]}"
export REWARDS_PASSWORD="${OUTPUT[6]}"

echo "Fetching UI ZIP file..."
rm -f workshopui.zip
wget -q https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/2e2ca614-1fa7-4e65-936b-b495eb53d180/workshopui.zip
rm -rf workshopui
unzip -q workshopui.zip -d workshopui
rm -f workshopui.zip
cd workshopui

echo "Configuring web UI..."
./params.sh "${API_ENDPOINT}" "${COGNITO_CLIENT_ID}" "${COGNITO_POOL_ID}"

echo "Repackaging UI..."
zip -q workshopui.zip -r *
cd ..

echo "Done"
