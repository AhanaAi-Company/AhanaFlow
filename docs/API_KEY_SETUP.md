# AhanaFlow API Key Setup Guide

Complete guide for obtaining, configuring, and managing your AhanaFlow commercial license.

---

## Table of Contents

1. [Why You Need an API Key](#why-you-need-an-api-key)
2. [Getting Your API Key](#getting-your-api-key)
3. [Configuration Methods](#configuration-methods)
4. [Upgrading Your Plan](#upgrading-your-plan)
5. [Benefits Breakdown](#benefits-breakdown)
6. [Troubleshooting](#troubleshooting)

---

## Why You Need an API Key

AhanaFlow is **free for non-commercial use**. For commercial deployments, you need a valid API key.

### Commercial Use Definition

You need an API key if you're using AhanaFlow for:

✓ Production services that generate revenue  
✓ Commercial SaaS, PaaS, or infrastructure products  
✓ Processing data for commercial clients  
✓ Any for-profit business operation  

### What the API Key Unlocks

| Feature | Community (Free) | With API Key (Paid) |
|---------|------------------|---------------------|
| **Compression Ratio** | 50-60% (zstd baseline) | **88.7%** (trained dictionary) |
| **WAL Size** | Larger (baseline) | **5× smaller** |
| **Performance** | Full speed | Full speed (no overhead) |
| **Support** | Community forums | Email + Priority |
| **SLA** | None | 99.9% uptime guarantee |
| **Legal** | Non-commercial only | Production indemnification |

**Key Point:** The trained dictionary runs **locally on your infrastructure** — zero latency penalty, just better compression.

---

## Getting Your API Key

### Step 1: Visit the Website

Go to **[www.ahanaflow.com](https://www.ahanaflow.com)** and click **"Get API Key"** or **"Pricing"**.

### Step 2: Choose Your Plan

| Plan | Price | Best For |
|------|-------|----------|
| **Free** | $0/mo | Small projects (≤10K req/mo) |
| **Starter** | $49/mo | Single service/pod (100K req/mo) |
| **Professional** | $149/mo | Multiple services (1M req/mo) |
| **Business** | $499/mo | Production scale (10M req/mo) |
| **Enterprise** | Custom | On-prem, custom dictionaries, source access |

### Step 3: Complete Registration

**For Free Tier:**
1. Enter your email address
2. Verify your email
3. Receive your API key instantly

**For Paid Plans:**
1. Enter your email and payment details
2. Complete checkout via Stripe
3. Receive your API key via email within 5 minutes
4. Download your license certificate (optional, for compliance)

### Step 4: Store Your API Key Securely

```bash
# Example API key format
YOUR_AHANAFLOW_LICENSE_KEY
```

**Security Best Practices:**
- Never commit API keys to public repositories
- Use environment variables or secret management systems
- Rotate keys annually or after team member departures
- Use separate keys for dev/staging/production

### Step 5: Retrieve Proprietary Artifacts Through The Backend

Commercial customers should retrieve paid codec or dictionary artifacts through the
backend portal flow, not from the public repo. The backend manifest now supports:

- short-lived download grants
- customer-specific leak-attribution fingerprints
- per-customer PUZZLE-AUTH unlock keys for wrapped proprietary payloads

This keeps proprietary payload keys off the client until the backend has validated
the customer entitlement.

---

## Configuration Methods

### Method 1: Environment Variable (Recommended)

**Linux/macOS:**

```bash
# Add to ~/.bashrc or ~/.zshrc
export AHANAFLOW_LICENSE_KEY="YOUR_AHANAFLOW_LICENSE_KEY"

# Reload shell
source ~/.bashrc

# Verify
echo $AHANAFLOW_LICENSE_KEY
```

**Windows PowerShell:**

```powershell
# Set permanently
[System.Environment]::SetEnvironmentVariable("AHANAFLOW_LICENSE_KEY", "YOUR_AHANAFLOW_LICENSE_KEY", "User")

# Verify
$env:AHANAFLOW_LICENSE_KEY
```

**Docker:**

```bash
docker run -d \
  -e AHANAFLOW_LICENSE_KEY="YOUR_AHANAFLOW_LICENSE_KEY" \
  ghcr.io/ahanaai-company/ahanaflow:branch-33-controlled-deployment-v1.0
```

**Kubernetes:**

```bash
# Create secret
kubectl create secret generic ahanaflow-api-key \
  --from-literal=api-key=YOUR_AHANAFLOW_LICENSE_KEY \
  -n your-namespace

# Reference in deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ahanaflow
spec:
  template:
    spec:
      containers:
      - name: ahanaflow
        env:
        - name: AHANAFLOW_LICENSE_KEY
          valueFrom:
            secretKeyRef:
              name: ahanaflow-api-key
              key: api-key
```

### Method 2: License key file

There is no `.ahanaflow.conf` reader, no `api_key=` argument on `CompressedStateEngine`, and no `--api-key` CLI flag.

If you prefer not to put the key in the process environment, point the server at a file:

```bash
export AHANAFLOW_LICENSE_KEY_FILE="/run/secrets/ahanaflow/license_key"
python -m backend.universal_server.cli serve
```

`AHANAFLOW_LICENSE_KEY` (or `AHANAFLOW_LICENSE_KEY_FILE`) authenticates clients to the TCP server. It does not download or install a Pro codec.

---

## Verifying Your API Key

### Check Compression Tier

After configuring your API key, verify you're using Pro-tier compression:

```python
from backend.state_engine import CompressedStateEngine
import os

# Check if API key is set
api_key = os.environ.get("AHANAFLOW_LICENSE_KEY")
print(f"API Key configured: {'✓' if api_key else '✗'}")

# Create engine and check compression
engine = CompressedStateEngine("test.wal", durability_mode="safe")
stats = engine.get_stats()

# Pro tier shows 80%+ compression ratio
compression_ratio = stats.get("compression_ratio", 0)
print(f"Compression ratio: {compression_ratio:.1%}")

if compression_ratio > 0.80:
    print("✓ Using Pro-tier compression (88.7% trained dictionary)")
else:
    print("⚠ Using Community-tier compression (50-60% baseline)")
    print("  Ensure AHANAFLOW_LICENSE_KEY is set correctly")

engine.close()
os.remove("test.wal")  # Cleanup
```

Expected output with valid API key:

```
API Key configured: ✓
Compression ratio: 88.7%
✓ Using Pro-tier compression (88.7% trained dictionary)
```

---

## Upgrading Your Plan

### When to Upgrade

Monitor your usage at [www.ahanaflow.com/dashboard](https://www.ahanaflow.com/dashboard):

- **Requests/month:** Approaching your plan limit
- **Response time:** Need priority support for troubleshooting
- **Storage costs:** Larger WAL files eating into cloud storage budget

### How to Upgrade

1. Log in to [www.ahanaflow.com/dashboard](https://www.ahanaflow.com/dashboard)
2. Click **"Upgrade Plan"**
3. Select new tier
4. **No code changes required** — your existing API key automatically unlocks new features

### Plan Comparison

| Feature | Free | Starter | Professional | Business | Enterprise |
|---------|------|---------|--------------|----------|------------|
| **Requests/month** | 10K | 100K | 1M | 10M | Unlimited |
| **Compression** | 50-60% | 88.7% | 88.7% | 88.7% | Custom 90%+ |
| **Support** | Forums | Email | Priority Email | 24/7 Phone | Dedicated CSM |
| **SLA** | None | 99.5% | 99.9% | 99.95% | 99.99% |
| **Response Time** | N/A | <24h | <4h | <1h | <15min |
| **Features** | Core | Core + Pro compression | + Multi-region | + HA replication | + Source access |
| **Price** | $0 | $49/mo | $149/mo | $499/mo | Custom |

---

## Benefits Breakdown

### 1. Compression Savings (88.7% vs 50-60%)

**Example:** 1 million operations per day

| Tier | WAL Size | Monthly Storage | Annual Storage Cost (AWS S3) |
|------|----------|----------------|------------------------------|
| Community | ~30 GB | 900 GB | $20.70/year |
| Pro (API) | **~5.3 GB** | **159 GB** | **$3.65/year** |
| **Savings** | **83% smaller** | **741 GB saved** | **$17.05/year saved** |

For high-volume deployments (10M+ ops/day), the storage savings alone justify the API plan cost.

### 2. Performance Context

The trained dictionary runs **in-process** — compression/decompression happen in microseconds. Treat the older embedded throughput note below as implementation context, not as the primary public benchmark boundary for the deploy repo:

- **Throughput:** same runtime behavior as the community tier in the same embedding mode
- **Latency:** No added network calls (unlike API-based compression)
- **CPU:** Minimal increase (~5%) due to dictionary lookups

For current public performance positioning, use `docs/PRODUCTION_READINESS_REPORT.md`.

### 3. Support & SLA

| Issue Type | Community | Starter | Professional | Business | Enterprise |
|------------|-----------|---------|--------------|----------|------------|
| Bug report | GitHub issue (no SLA) | Email <24h | Email <4h | Phone <1h | Slack <15min |
| Production down | No support | <24h | <2h | <30min | Immediate |
| Feature request | May be ignored | Considered | Prioritized | Fast-tracked | Custom dev |
| Architecture review | None | None | 1× per quarter | Monthly | Weekly |

### 4. Legal Indemnification

**Community Tier:**
- "AS IS" with no warranties
- Use at your own risk
- No liability protection

**Commercial License (API Plans):**
- Legal right to use in production
- Liability coverage up to plan limits
- Warranty and indemnification clauses
- Compliance assistance for SOC 2, GDPR, HIPAA

### 5. Early Access

API plan holders get:
- Pre-release builds (v1.2 distributed features coming Q4 2026)
- Beta features (TLS, Prometheus, Kubernetes operator)
- Custom feature development for Enterprise tier

---

## Troubleshooting

### "API key invalid" Error

```
Error: API key is invalid or expired
```

**Solutions:**
1. Check for typos in your API key
2. Verify key hasn't expired (keys expire after 1 year by default)
3. Log in to www.ahanaflow.com/dashboard and verify key is active
4. Check your subscription status (payment failed?)

### "API key not recognized" Error

```
Warning: API key not found, using community-tier compression
```

**Solutions:**
1. Ensure `AHANAFLOW_LICENSE_KEY` environment variable is set
2. Check the variable is accessible to the process:
   ```bash
   printenv | grep AHANAFLOW
   ```
3. Restart the server after setting the environment variable
4. Verify no typos in variable name (case-sensitive)

### License key vs Pro codec

`AHANAFLOW_LICENSE_KEY` authenticates clients to the TCP server. It does not download or install a Pro codec, and it does not change the on-disk community compression path by itself.

Commercial codec artifacts are issued through the license portal / backend manifest flow, not by installing a package from a public index.

### Key Rotation

To rotate your API key:

1. Log in to www.ahanaflow.com/dashboard
2. Click **"Generate New Key"**
3. Update your configuration with the new key
4. Restart all AhanaFlow instances
5. Old key remains valid for 7 days (grace period)

---

## Getting More Help

### Support Channels

- **Documentation:** [www.ahanaflow.com/docs](https://www.ahanaflow.com/docs)
- **GitHub Issues:** [Report bugs or request features](https://github.com/AhanaAi-Company/AhanaFlow/issues)
- **Email Support:** support@ahanaai.com (paid plans only)
- **Status Page:** [status.ahanaflow.com](https://status.ahanaflow.com)

### Contact Sales

For enterprise inquiries:
- **Email:** sales@ahanaai.com
- **Schedule a Call:** [calendly.com/ahanaai-sales](https://calendly.com/ahanaai-sales)

---

## Next Steps

1. **[Deployment Guide](./DEPLOYMENT_GUIDE.md)** — Deploy AhanaFlow with your license key
2. **[Production Readiness Report](./PRODUCTION_READINESS_REPORT.md)** — Public benchmark boundary
3. **[Examples](../examples/)** — Working code samples
4. **[Secret Rotation Runbook](./SECRET_ROTATION_RUNBOOK.md)** — Runtime secret mounts

---

🌺 **AhanaFlow — 88.7% Compression, Zero Latency Penalty**
