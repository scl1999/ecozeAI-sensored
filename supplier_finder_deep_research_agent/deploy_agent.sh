#!/bin/bash
set -e

# Configuration
PROJECT_ID="ecoze-f216c"
REGION="us-central1"
AGENT_NAME="supplier_finder"
AGENT_DIR="$HOME/ecoze-firebase/eai-agents/supplier_finder"

# Ensure we are in the agent directory
cd "$AGENT_DIR"

echo "Deploying agent '$AGENT_NAME' to region '$REGION' in project '$PROJECT_ID'..."

# Deploy using adk
# Note: Assuming 'adk' is in the path. If not, use 'python3 -m google.adk.cli' or similar.
# We also need to pass the BROWSER_USE_SERVICE_URL. 
# Since we don't have the actual URL, we will use a placeholder or the one from env if set.
BROWSER_URL="${BROWSER_USE_SERVICE_URL:-http://localhost:8080/browse}"

echo "Using Browser Service URL: $BROWSER_URL"

# We need to make sure the requirements are installed or available for the build.
# The adk deploy command handles packaging.

export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
export GOOGLE_CLOUD_LOCATION="$REGION"

# Create a temporary env file for deployment
cp .env .env.deploy
# Ensure we use us-central1
sed -i '/GOOGLE_CLOUD_LOCATION/d' .env.deploy
echo "GOOGLE_CLOUD_LOCATION=us-central1" >> .env.deploy
echo "BROWSER_USE_SERVICE_URL=$BROWSER_URL" >> .env.deploy

# Go to parent directory as requested by adk
cd ..

adk deploy agent_engine \
  --display_name "$AGENT_NAME" \
  --env_file "$AGENT_DIR/.env.deploy" \
  supplier_finder

# Cleanup
rm "$AGENT_DIR/.env.deploy"

echo "Deployment initiated. Check console for status."
