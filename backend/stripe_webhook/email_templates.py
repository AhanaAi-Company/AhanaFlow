# Copyright 2026 AhanaAI. All rights reserved.
"""HTML email templates for AhanaFlow license delivery."""

from __future__ import annotations


def license_issued(customer_email: str, license_key: str, tier: str = "Pro", days: int = 365) -> dict:
    """Return {'subject': str, 'html': str, 'text': str} for a new license email."""
    subject = "Your AhanaFlow Pro License Key"

    # Plain text fallback
    text = f"""Welcome to AhanaFlow {tier}!

Your license key is active for {days} days.  Set it as an environment variable
before starting AhanaFlow:

  export AHANAFLOW_LICENSE_KEY="{license_key}"

Add that line to your shell profile (.bashrc / .zshrc) or your server's
systemd unit / Docker environment file so it persists across restarts.

Verify activation:
  python -c "from state_engine.codec import active_tier; print(active_tier())"
  # Expected output: pro

Questions?  Reply to this email or visit https://ahanazip.com/support

— The AhanaAI Team
"""

    # HTML email
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AhanaFlow Pro License</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0d1117; color: #e6edf3; margin: 0; padding: 0; }}
  .outer {{ max-width: 600px; margin: 40px auto; padding: 0 20px; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 12px;
           overflow: hidden; }}
  .header {{ background: linear-gradient(135deg, #6e40c9 0%, #1f6feb 100%);
             padding: 32px 40px; }}
  .header h1 {{ margin: 0; font-size: 24px; color: #fff; letter-spacing: -0.5px; }}
  .header p {{ margin: 6px 0 0; color: rgba(255,255,255,0.8); font-size: 14px; }}
  .body {{ padding: 36px 40px; }}
  .body p {{ line-height: 1.7; color: #8b949e; margin: 0 0 20px; }}
  .body .greeting {{ color: #e6edf3; font-size: 16px; font-weight: 500; }}
  .key-box {{ background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
              padding: 20px 24px; margin: 24px 0; position: relative; }}
  .key-box .label {{ font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                     color: #6e40c9; font-weight: 600; margin: 0 0 10px; }}
  .key-box code {{ font-family: 'JetBrains Mono', 'Fira Code', monospace;
                   font-size: 12px; color: #58a6ff; word-break: break-all;
                   line-height: 1.6; display: block; }}
  .steps {{ margin: 28px 0; }}
  .steps h3 {{ color: #e6edf3; font-size: 14px; margin: 0 0 16px; }}
  .step {{ display: flex; gap: 14px; margin-bottom: 14px; }}
  .step-num {{ background: #6e40c9; color: #fff; border-radius: 50%;
               width: 22px; height: 22px; display: flex; align-items: center;
               justify-content: center; font-size: 12px; font-weight: 700;
               flex-shrink: 0; margin-top: 2px; }}
  .step-text {{ color: #8b949e; font-size: 14px; line-height: 1.6; }}
  .step-text code {{ background: #0d1117; border: 1px solid #30363d;
                     border-radius: 4px; padding: 1px 6px; font-size: 12px;
                     color: #58a6ff; }}
  .expiry-note {{ background: #161b22; border-left: 3px solid #6e40c9;
                  border-radius: 0 6px 6px 0; padding: 14px 18px; margin: 24px 0;
                  font-size: 13px; color: #8b949e; }}
  .expiry-note strong {{ color: #e6edf3; }}
  .footer {{ padding: 24px 40px; border-top: 1px solid #21262d;
             text-align: center; }}
  .footer p {{ font-size: 12px; color: #484f58; margin: 0; line-height: 1.8; }}
  .footer a {{ color: #58a6ff; text-decoration: none; }}
</style>
</head>
<body>
<div class="outer">
  <div class="card">
    <div class="header">
      <h1>🌺 AhanaFlow {tier}</h1>
      <p>Your license is ready — activate in 60 seconds</p>
    </div>
    <div class="body">
      <p class="greeting">Welcome to AhanaFlow {tier}!</p>
      <p>Your license key is below.  It's valid for <strong style="color:#e6edf3">{days} days</strong>
         and will renew automatically with your subscription.</p>

      <div class="key-box">
        <div class="label">AHANAFLOW_LICENSE_KEY</div>
        <code>{license_key}</code>
      </div>

      <div class="steps">
        <h3>Activate in 3 steps:</h3>
        <div class="step">
          <div class="step-num">1</div>
          <div class="step-text">
            Set the environment variable on your server:<br>
            <code>export AHANAFLOW_LICENSE_KEY="&lt;paste key&gt;"</code><br>
            Add to <code>.bashrc</code>, <code>.env</code>, or your Docker/systemd config.
          </div>
        </div>
        <div class="step">
          <div class="step-num">2</div>
          <div class="step-text">
            Restart AhanaFlow (UniversalStateServer / VectorStateServerV2).
          </div>
        </div>
        <div class="step">
          <div class="step-num">3</div>
          <div class="step-text">
            Confirm Pro tier is active:<br>
            <code>python -c "from state_engine.codec import active_tier; print(active_tier())"</code>
          </div>
        </div>
      </div>

      <div class="expiry-note">
        <strong>Auto-renewal:</strong> Your key is refreshed automatically on each billing
        cycle.  If you cancel, the current key remains valid until it expires — no sudden
        service interruption.
      </div>

      <p>Need help?  <a href="https://ahanazip.com/support" style="color:#58a6ff">Visit our support page</a>
         or reply to this email.</p>
    </div>
    <div class="footer">
      <p>AhanaAI · <a href="https://ahanazip.com">ahanazip.com</a><br>
         You received this because you subscribed to AhanaFlow {tier}.<br>
         <a href="https://ahanazip.com/license-portal">Manage your license</a> ·
         <a href="https://ahanazip.com/support">Support</a></p>
    </div>
  </div>
</div>
</body>
</html>"""

    return {"subject": subject, "html": html, "text": text}


def license_renewed(customer_email: str, license_key: str, tier: str = "Pro", days: int = 32) -> dict:
    """Return email dict for a license renewal (invoice.payment_succeeded)."""
    subject = "AhanaFlow Pro — License Renewed"
    d = license_issued(customer_email, license_key, tier=tier, days=days)
    d["subject"] = subject
    # Swap the greeting in the HTML
    d["html"] = d["html"].replace(
        "Welcome to AhanaFlow",
        "Your AhanaFlow subscription has renewed ✓  Welcome back to AhanaFlow",
    )
    d["text"] = "Your AhanaFlow Pro license has been renewed.\n\n" + d["text"]
    return d


def license_expiring_soon(days_left: int, license_key: str) -> dict:
    """Return email dict for a 7-day pre-expiry warning."""
    subject = f"AhanaFlow Pro — License expires in {days_left} days"
    text = (
        f"Your AhanaFlow Pro license expires in {days_left} days.\n\n"
        f"Renew at https://ahanazip.com/billing to avoid falling back to community tier.\n\n"
        f"Current key (still valid): {license_key}"
    )
    html = f"""<!DOCTYPE html><html><body style="font-family:sans-serif;background:#0d1117;color:#e6edf3;padding:40px">
<div style="max-width:500px;margin:0 auto;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px">
  <h2 style="color:#f0883e;margin:0 0 16px">⚠️ License Expiring in {days_left} Days</h2>
  <p style="color:#8b949e;line-height:1.7">Your AhanaFlow Pro license expires soon.
  Renew now to keep the trained dictionary codec and 88.7% compression.</p>
  <a href="https://ahanazip.com/billing"
     style="display:inline-block;background:#6e40c9;color:#fff;padding:12px 24px;
            border-radius:8px;text-decoration:none;font-weight:600;margin-top:16px">
    Renew Subscription
  </a>
</div>
</body></html>"""
    return {"subject": subject, "html": html, "text": text}


def portal_access_code(customer_email: str, code: str, minutes_valid: int = 10) -> dict:
    subject = "Your AhanaFlow portal access code"
    text = (
        f"Use this AhanaFlow portal access code to manage your license and API keys: {code}\n\n"
        f"The code expires in {minutes_valid} minutes. If you did not request it, you can ignore this email.\n"
    )
    html = f"""<!DOCTYPE html><html><body style="font-family:sans-serif;background:#0d1117;color:#e6edf3;padding:40px">
<div style="max-width:480px;margin:0 auto;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px">
  <h2 style="margin:0 0 12px;color:#e6edf3">AhanaFlow Portal Access</h2>
  <p style="color:#8b949e;line-height:1.7">Use the one-time code below to open your license and API key portal. It expires in <strong style="color:#e6edf3">{minutes_valid} minutes</strong>.</p>
  <div style="margin:24px 0;padding:18px 20px;border:1px solid #30363d;border-radius:8px;background:#0d1117;text-align:center;font-size:28px;letter-spacing:0.3em;color:#58a6ff;font-weight:700">{code}</div>
  <p style="color:#8b949e;line-height:1.7">If you did not request this code, you can ignore this email.</p>
</div>
</body></html>"""
    return {"subject": subject, "html": html, "text": text}
