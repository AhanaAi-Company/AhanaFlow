# GitHub Deployment Package - Internal Guide

**Created:** April 9, 2026  
**For:** Public GitHub repository `AhanaAI-Company/ahanaflow`  
**License:** Dual (Non-Commercial / Commercial)  
**Website:** www.ahanaflow.com

---

## Overview

This directory contains the **public-facing release package** for AhanaFlow. It includes:

✅ **Open-source components** (state engine, universal server, vector server)  
✅ **Documentation** (deployment guide, API key setup, examples)  
✅ **Licensing** (dual license: free non-commercial + paid commercial)  
✅ **Examples** (rate limiter, job queue, session store)  
✅ **Docker/Kubernetes** configs for deployment  

❌ **Excluded:** Proprietary `ahana_codec/` and `compression_service/` (binary-only distribution)

---

## What's Included

### Core Components (Open Source)

```
backend/
├── universal_server/       # KV + queues + streams (TCP server)
├── vector_server/          # Vector search with HNSW
├── state_engine/           # Compressed WAL engine
├── ahana_state/           # State management utilities
├── customer_db/           # Customer database server
└── stripe_webhook/        # Payment webhook handler
```

**Note:** `ahana_codec/` and `compression_service/` are **NOT included**. These are distributed as binary wheels to API key holders.

### Documentation

```
docs/
├── DEPLOYMENT_GUIDE.md    # Complete deployment instructions
├── API_KEY_SETUP.md       # How to configure commercial licenses
├── API_REFERENCE.md       # Command documentation (to be created)
├── BENCHMARKS.md          # Performance analysis (to be created)
└── ARCHITECTURE.md        # Internal design docs (to be created)
```

### Examples

```
examples/
├── rate_limiter.py        # Token bucket rate limiting
├── job_queue.py           # Background job processing
└── session_store.py       # User session management with TTL
```

### Supporting Files

```
LICENSE                    # Dual license (non-commercial free + commercial paid)
README.md                  # Project homepage with quick start
CONTRIBUTING.md            # Contribution guidelines
CHANGELOG.md               # Version history
.gitignore                 # Excludes proprietary components
requirements.txt           # Python dependencies
docker-compose.yml         # Multi-service Docker setup
Dockerfile                 # Container image build
```

---

## Licensing Strategy

### Community Edition (Free)

**Who can use:**
- Personal projects
- Academic research
- Open-source projects
- Non-profit organizations
- Development/testing environments

**What they get:**
- Full source code (except codec)
- Community-tier compression (50-60% with plain zstd)
- Community forum support
- MIT-like  permissive license for non-commercial use

### Commercial License (Paid)

**Who needs it:**
- Production commercial deployments
- SaaS/PaaS providers
- For-profit businesses
- Any revenue-generating use

**What they get:**
- API key that unlocks Pro compression (88.7% with trained dictionary)
- Binary wheel with `ahana_codec` package
- Priority email/phone support
- Production SLA (99.9% uptime)
- Legal indemnification

**Pricing:**
- **Free tier:** ≤10K req/mo ($0)
- **Starter:** 100K req/mo ($49/mo)
- **Professional:** 1M req/mo ($149/mo)
- **Business:** 10M req/mo ($499/mo)
- **Enterprise:** Custom (unlimited, on-prem, source access)

---

## Deployment Workflow

### Step 1: Prepare Repository

```bash
cd deploy_to_github

# Verify no proprietary code
grep -r "ahana_codec" . || echo "✓ No proprietary references"
grep -r "compression_service" . || echo "✓ No proprietary references"

# Check .gitignore
cat .gitignore | grep -E "ahana_codec|compression_service"
```

### Step 2: Create GitHub Repository

1. Go to https://github.com/AhanaAI-Company
2. Create new repository: `ahanaflow`
3. Make it **public**
4. Add description: "Compressed State & Event Engine — KV Store, Queues, Streams & Vector Search"
5. Add topics: `compression`, `database`, `redis-alternative`, `vector-database`, `python`

### Step 3: Push to GitHub

```bash
# Initialize git
cd deploy_to_github
git init
git add .
git commit -m "Initial release v1.0.0"

# Add remote
git remote add origin https://github.com/AhanaAI-Company/ahanaflow.git

# Push
git branch -M main
git push -u origin main

# Tag release
git tag -a v1.0.0 -m "AhanaFlow v1.0.0 - Initial Public Release"
git push origin v1.0.0
```

### Step 4: GitHub Settings

**Enable:**
- ✅ Issues (for bug reports)
- ✅ Discussions (for community Q&A)
- ✅ Wiki (for extended documentation)

**Add:**
- `README.md` → Should display automatically
- License badge
- GitHub Actions for CI/CD (optional)

### Step 5: Docker Registry

```bash
# Build image
git checkout branch-33-controlled-deployment-v1.0
docker build -t ghcr.io/ahanaai-company/ahanaflow:branch-33-controlled-deployment-v1.0 .

# Login to GitHub Container Registry
echo $GITHUB_PAT | docker login ghcr.io -u ahanaai-bot --password-stdin

# Push
docker push ghcr.io/ahanaai-company/ahanaflow:branch-33-controlled-deployment-v1.0
```

### Step 6: Website Integration

Update www.ahanaflow.com to point to:
- GitHub repo: https://github.com/AhanaAI-Company/ahanaflow
- Documentation: Link to GitHub docs/
- Examples: Link to GitHub examples/
- Download: `pip install ahanaflow` or GitHub Releases

---

## API Key Distribution

### Free Tier (≤10K req/mo)

- User visits www.ahanaflow.com
- Clicks "Get Free API Key"
- Enters email → instant key generation
- No credit card required
- Community compression (50-60%)

### Paid Tiers ($49+/mo)

- User visits www.ahanaflow.com/pricing
- Selects plan → checkout via Stripe
- Receives API key via email
- Downloads binary wheel: `pip install ahanaflow-pro --extra-index-url https://api.ahanaflow.com/wheels/`
- Unlocks 88.7% Pro compression

### Pro Codec Distribution

The `ahana_codec` package is distributed as a **binary Python wheel** (`.whl` file):

```bash
# Customer receives API key: ahanaflow_live_ABC123...
# Customer installs Pro codec:
export AHANAFLOW_API_KEY=ahanaflow_live_ABC123...
pip install ahanaflow-pro --extra-index-url https://api.ahanaflow.com/wheels/

# Verify Pro compression
python -c "from ahana_codec import compress; print('✓ Pro codec loaded')"
```

**Security:**
- Wheel contains compiled `.so` file (no Python source)
- Download requires valid API key
- Wheels are per-platform (linux_x86_64, macosx_arm64, win_amd64)
- License check on import → API key validation

---

## Marketing Launch Plan

### Phase 1: Soft Launch (Week 1)

- ✅ Push to GitHub
- ✅ Publish Docker image to GHCR
- 📢 Announce on Twitter/X (AhanaAI account)
- 📢 Post in relevant subreddits (r/Python, r/databases, r/redis)
- 📧 Email existing AhanaAI customers

### Phase 2: Community Traction (Week 2-4)

- 🔥 Submit to Hacker News (Show HN: AhanaFlow)
- 🚀 Submit to Product Hunt
- 📝 Write blog post: "Why we built a Redis alternative with 88.7% compression"
- 🎥 Create demo video (YouTube)
- 📊 Share benchmarks on social media

### Phase 3: SEO & Content (Month 2-3)

- 📝 Write technical deep-dives (compression algorithm, HNSW implementation)
- 🎓 Create tutorials (building a real-time leaderboard, RAG memory system)
- 🔗 Guest posts on relevant blogs (Redis migration guides)
- 📈 Monitor analytics and user feedback

---

## Support Channels

### Community (Free Users)

- **GitHub Issues:** Bug reports and feature requests
- **GitHub Discussions:** Q&A and general help
- **Documentation:** In-repo docs/ folder
- **Response Time:** Best effort (24-72 hours)

### Paid Plans

- **Email:** support@ahanaai.com
- **Priority:** <4 hour response time (Professional+)
- **Phone:** Business and Enterprise plans
- **Slack:** Enterprise plans get dedicated channel
- **SLA:** 99.9%+ uptime guarantee

---

## Monitoring & Analytics

Track:
- GitHub stars, forks, issues
- PyPI download counts
- Docker Hub / GHCR pull counts
- Website traffic to www.ahanaflow.com
- API key registrations (free vs paid)
- Conversion rate (free → paid upgrades)

**Tools:**
- Google Analytics (website)
- GitHub Insights (repo activity)
- Stripe Dashboard (revenue/churn)
- Plausible or PostHog (privacy-friendly analytics)

---

## Next Steps (Post-Launch)

### Week 1
- ✅ Monitor GitHub for issues
- ✅ Respond to community questions
- ✅ Fix any critical bugs quickly

### Month 1
- 📊 Analyze user feedback
- 🛠️ Prioritize most-requested features
- 📈 Optimize conversion funnel (free → paid)

### Quarter 1
- 🚀 Ship v1.1 (WAL compaction, TLS, Prometheus)
- 📝 Publish case studies from early customers
- 🎯 Reach 1,000 GitHub stars
- 💰 100+ paying customers

---

## Security Considerations

### Open Source Code

- No API keys or secrets in repository
- All sensitive config via environment variables
- Security audit before public release
- Dependency vulnerability scanning (Dependabot)

### Proprietary Codec

- Binary wheels only — no source published
- License check on import (requires valid API key)
- Anti-tampering for compiled `.so` files
- Regular security updates

### Legal Protection

- Dual license clearly stated in LICENSE file
- Terms of Service for commercial use
- Privacy policy for API key holders
- GDPR compliance for EU users

---

## Contact

**For deployment questions:**  
DevOps Team: devops@ahanaai.com

**For license questions:**  
Legal Team: licensing@ahanaai.com

**For sales inquiries:**  
Sales Team: sales@ahanaai.com

**General:**  
Main: hello@ahanaai.com

---

🌺 **AhanaFlow — Compression Reimagined**

*Internal document — Do not distribute publicly*
