#!/bin/bash

# Deployment script for JCPS API
# Usage: ./deploy.sh

set -e  # Exit on any error

echo "🚀 Starting deployment to Google Cloud Run..."

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ Google Cloud CLI not found. Please install it first."
    exit 1
fi

# Check if authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "❌ Not authenticated with Google Cloud. Please run: gcloud auth login"
    exit 1
fi

# Check if project is set
PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT" ]; then
    echo "❌ No project set. Please run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo "✅ Using project: $PROJECT"

# Deploy to Cloud Run
echo "📦 Deploying to Cloud Run..."
gcloud run deploy jcpscompare \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_MAPS_API_KEY="$GOOGLE_MAPS_API_KEY"

echo "✅ Deployment complete!"
echo "🌐 Your API should be available at: https://jcpscompare-615331482467.us-central1.run.app"

