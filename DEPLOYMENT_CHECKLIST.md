# Pre-Deployment Checklist

## Before Merging to Main:

### ✅ Code Quality
- [ ] All tests pass locally (`python3 -m pytest app/tests/ -v`)
- [ ] API runs locally without errors
- [ ] Environment variables are properly configured
- [ ] All dependencies are in requirements.txt

### ✅ Git Workflow
- [ ] Changes committed to develop branch
- [ ] Pull request created and reviewed
- [ ] All changes merged to main branch

### ✅ Production Readiness
- [ ] Google Maps API key is available
- [ ] Database files are up to date
- [ ] GIS data files are properly named and referenced
- [ ] No sensitive data in code (use environment variables)

## After Merging to Main:

### 🚀 Deployment Options

**Option 1: Automated (GitHub Actions)**
- [ ] Push to main triggers automatic deployment
- [ ] Check GitHub Actions tab for deployment status
- [ ] Verify API is responding at production URL

**Option 2: Manual Script**
```bash
# Set environment variable
export GOOGLE_MAPS_API_KEY="your-api-key-here"

# Run deployment script
./deploy.sh
```

**Option 3: Manual gcloud Commands**
```bash
# Authenticate (if needed)
gcloud auth login

# Set project
gcloud config set project YOUR_PROJECT_ID

# Deploy
gcloud run deploy jcpscompare \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_MAPS_API_KEY=your-api-key
```

### ✅ Post-Deployment Verification
- [ ] Test API endpoint: `curl https://jcpscompare-615331482467.us-central1.run.app/test`
- [ ] Test school zone lookup with a real address
- [ ] Verify frontend can connect to API
- [ ] Check Google Cloud Run logs for any errors

## 🚨 Emergency Rollback
If deployment fails:
```bash
# Rollback to previous version
gcloud run services update-traffic jcpscompare --to-revisions=LATEST=0
```

