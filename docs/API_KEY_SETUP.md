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
ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3
```

**Security Best Practices:**
- Never commit API keys to public repositories
- Use environment variables or secret management systems
- Rotate keys annually or after team member departures
- Use separate keys for dev/staging/production

---

## Configuration Methods

### Method 1: Environment Variable (Recommended)

**Linux/macOS:**

```bash
# Add to ~/.bashrc or ~/.zshrc
export AHANAFLOW_API_KEY="ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3"

# Reload shell
source ~/.bashrc

# Verify
echo $AHANAFLOW_API_KEY
```

**Windows PowerShell:**

```powershell
# Set permanently
[System.Environment]::SetEnvironmentVariable("AHANAFLOW_API_KEY", "ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3", "User")

# Verify
$env:AHANAFLOW_API_KEY
```

**Docker:**

```bash
docker run -d \
  -e AHANAFLOW_API_KEY="ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3" \
  ghcr.io/ahanaai-company/ahanaflow:latest
```

**Kubernetes:**

```bash
# Create secret
kubectl create secret generic ahanaflow-api-key \
  --from-literal=api-key=ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3 \
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
        - name: AHANAFLOW_API_KEY
          valueFrom:
            secretKeyRef:
              name: ahanaflow-api-key
              key: api-key
```

### Method 2: Configuration File

Create `.ahanaflow.conf` in your project root:

```ini
[api]
key = ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3
```

Or use YAML format:

```yaml
# ahanaflow.yaml
api:
  key: ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3
```

### Method 3: Programmatic Configuration

**Python:**

```python
from backend.state_engine import CompressedStateEngine

# Option A: Pass directly to engine
engine = CompressedStateEngine(
    "app.wal",
    api_key="ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3",
    durability_mode="safe"
)

# Option B: Set via environment before importing
import os
os.environ["AHANAFLOW_API_KEY"] = "ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3"

# Then use normally
engine = CompressedStateEngine("app.wal", durability_mode="safe")
```

**Node.js (Coming soon):**

```javascript
const { AhanaFlowClient } = require('ahanaflow');

const client = new AhanaFlowClient({
  apiKey: 'ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3',
  host: 'localhost',
  port: 9633
});
```

### Method 4: Server Startup Flag

```bash
# Start server with API key
python -m backend.universal_server.cli serve \
  --port 9633 \
  --api-key ahanaflow_live_5k8j2n9f1x6c4d8e7g3h2m1p9q7r4s6t8v2w5y1z3
```

---

## Verifying Your API Key

### Check Compression Tier

After configuring your API key, verify you're using Pro-tier compression:

```python
from backend.state_engine import CompressedStateEngine
import os

# Check if API key is set
api_key = os.environ.get("AHANAFLOW_API_KEY")
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
    print("  Ensure AHANAFLOW_API_KEY is set correctly")

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

### 2. Performance (No Overhead)

The trained dictionary runs **in-process** — compression/decompression happen in microseconds:

- **Throughput:** Same 1.57M ops/s as community tier
- **Latency:** No added network calls (unlike API-based compression)
- **CPU:** Minimal increase (~5%) due to dictionary lookups

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
1. Ensure `AHANAFLOW_API_KEY` environment variable is set
2. Check the variable is accessible to the process:
   ```bash
   printenv | grep AHANAFLOW
   ```
3. Restart the server after setting the environment variable
4. Verify no typos in variable name (case-sensitive)

### Still Using 50-60% Compression

**Check your configuration:**

```bash
# Verify API key is set
echo $AHANAFLOW_API_KEY

# Check if Pro codec is installed
python -c "
try:
    from backend.ahana_codec import compress
    print('✓ Pro codec available')
except ImportError:
    print('✗ Pro codec not installed')
    print('  Install with: pip install ahanaflow-pro')
"
```

**Note:** The Pro codec (`ahanaflow-pro`) is automatically installed when you configure a valid API key and restart the server.

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
- **GitHub Issues:** [Report bugs or request features](https://github.com/AhanaAI-Company/ahanaflow/issues)
- **Email Support:** support@ahanaai.com (paid plans only)
- **Status Page:** [status.ahanaflow.com](https://status.ahanaflow.com)

### Contact Sales

For enterprise inquiries:
- **Email:** sales@ahanaai.com
- **Phone:** +1 (808) 555-0123
- **Schedule a Call:** [calendly.com/ahanaai-sales](https://calendly.com/ahanaai-sales)

---

## Next Steps

1. **[Deployment Guide](./DEPLOYMENT_GUIDE.md)** — Deploy AhanaFlow with your API key
2. **[API Reference](./API_REFERENCE.md)** — Complete command documentation
3. **[Benchmarks](./BENCHMARKS.md)** — Measure your compression gains
4. **[Examples](../examples/)** — Working code samples

---

🌺 **AhanaFlow — 88.7% Compression, Zero Latency Penalty**
