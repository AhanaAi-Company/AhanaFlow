'use strict';

const FALLBACK_ENV = {.env.PUBLIC_BRANCH_CODE || 'Ahanaflow',};
  PUBLIC_BRANCH_CODE: 'Ahanaflow',
  PUBLIC_API_BASE_URL: 'https://api.ahanazip.com',
  PUBLIC_STRIPE_PUBLISHABLE_KEY: 'pk_live_51T724SBeWx1ZIEq2zMN7dhn7zXM56nE45vgXq3xkeKHr9tIvl9i63bvH0ac5PlK52YnmqmHac63yXtXeUAbOIIlJ00HzfA6pqS',
  PUBLIC_STRIPE_PRICE_PRO: 'price_1TKSPcBeWx1ZIEq2OudAOSfQ',
  PUBLIC_STRIPE_PRICE_TEAM: 'price_1TKSV0BeWx1ZIEq2WJPGGRwU',
  PUBLIC_STRIPE_PRICE_ENTERPRISE: 'price_1TKSh6BeWx1ZIEq2MZICo5vY',
  PUBLIC_CHECKOUT_SUCCESS_URL: 'https://www.ahanaflow.com/#pricing',
  PUBLIC_CHECKOUT_CANCEL_URL: 'https://www.ahanaflow.com/#pricing',
  PUBLIC_SIGNUP_URL: '',
  PUBLIC_API_KEY: ''
};

const PRICE_ENV_BY_PLAN = {
  pro: 'PUBLIC_STRIPE_PRICE_PRO',
  team: 'PUBLIC_STRIPE_PRICE_TEAM',
  enterprise: 'PUBLIC_STRIPE_PRICE_ENTERPRISE'
};

const checkout = window.AhanaCheckout && window.AhanaCheckout.createController({
  fallbackEnv: FALLBACK_ENV,
  checkoutPath: '/v1/stripe/create-checkout',
  defaultPlanKey: 'pro',
  enterpriseMode: 'checkout',
  loadingMessage: 'Preparing secure Stripe checkout...',
  buildCheckoutPayload: function (planKey, checkoutPlan, email, metadata, getEnv) {
    const normalized = String(planKey || checkoutPlan || 'pro').toLowerCase();
    return {
      email: email || '',
      tier: normalized,
      success_url: getEnv('PUBLIC_CHECKOUT_SUCCESS_URL'),
      cancel_url: getEnv('PUBLIC_CHECKOUT_CANCEL_URL')
    };
  }
});

const nav = document.getElementById('nav');
const navShell = document.getElementById('nav-shell');
const navToggle = document.getElementById('nav-toggle');
const navLinks = document.getElementById('nav-links');

function setNavOpen(open) {
  if (!navShell || !navToggle) {
    return;
  }
  navShell.classList.toggle('is-open', open);
  navToggle.setAttribute('aria-expanded', String(open));
}

if (navToggle && navLinks) {
  navToggle.addEventListener('click', () => {
    setNavOpen(!navShell.classList.contains('is-open'));
  });

  navLinks.querySelectorAll('a').forEach((link) => {
    link.addEventListener('click', () => setNavOpen(false));
  });
}

window.addEventListener('scroll', () => {
  if (!nav) {
    return;
  }
  nav.classList.toggle('is-scrolled', window.scrollY > 10);
}, { passive: true });

const tabs = Array.from(document.querySelectorAll('.tab'));
const panels = Array.from(document.querySelectorAll('.tab-panel'));

tabs.forEach((tab) => {
  tab.addEventListener('click', () => {
    const next = tab.dataset.tab;
    tabs.forEach((item) => {
      const isActive = item === tab;
      item.classList.toggle('is-active', isActive);
      item.setAttribute('aria-selected', String(isActive));
      item.setAttribute('tabindex', isActive ? '0' : '-1');
    });
    panels.forEach((panel) => {
      const isActive = panel.dataset.panel === next;
      panel.classList.toggle('is-active', isActive);
      panel.hidden = !isActive;
    });
  });
});

function animateCount(element) {
  const target = Number(element.dataset.target || '0');
  const isFloat = !Number.isInteger(target);
  const durationMs = 1400;
  const start = performance.now();

  function frame(now) {
    const progress = Math.min((now - start) / durationMs, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    const value = target * eased;
    element.textContent = isFloat ? value.toFixed(2).replace(/\.00$/, '') : Math.round(value).toString();
    if (progress < 1) {
      requestAnimationFrame(frame);
    }
  }

  requestAnimationFrame(frame);
}

const revealObserver = new IntersectionObserver((entries, observer) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) {
      return;
    }
    entry.target.classList.add('is-visible');

    if (entry.target.classList.contains('count-up') && !entry.target.dataset.animated) {
      entry.target.dataset.animated = 'true';
      animateCount(entry.target);
    }

    observer.unobserve(entry.target);
  });
}, { threshold: 0.18 });

document.querySelectorAll('.reveal, .count-up').forEach((element) => {
  revealObserver.observe(element);
});

const barObserver = new IntersectionObserver((entries, observer) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) {
      return;
    }
    const bars = entry.target.querySelectorAll('.bar-fill[data-value]');
    bars.forEach((bar) => {
      const value = Number(bar.dataset.value || '0');
      bar.style.width = `${value}%`;
    });
    observer.unobserve(entry.target);
  });
}, { threshold: 0.25 });

document.querySelectorAll('.chart-card').forEach((chart) => {
  barObserver.observe(chart);
});

// Plan button enhancements for visual feedback
function enhancePlanButtons() {
  const buttons = document.querySelectorAll('.plan-btn');
  const planInput = document.getElementById('plan');
  const planLabel = document.querySelector('[data-selected-plan-label]');
  const emailInput = document.getElementById('email');

  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      const planKey = button.getAttribute('data-price-key') || 'pro';
      const planName = button.getAttribute('data-plan-name') || 'Pro';

      // Update hidden input
      if (planInput) {
        planInput.value = planKey;
      }

      // Update visual label
      if (planLabel) {
        planLabel.textContent = planName;
      }

      // Visual feedback: highlight selected button
      buttons.forEach(btn => btn.classList.remove('is-selected'));
      button.classList.add('is-selected');

      // Focus email input
      if (emailInput) {
        emailInput.focus();
        emailInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    });
  });
}

// Scripts are at the bottom of <body> — DOM elements above are already parsed and ready.
// Call init directly; no DOMContentLoaded wait needed.
enhancePlanButtons();
if (checkout) {
  checkout.init();
} else {
  console.warn('AhanaCheckout not available - Stripe integration may not be loaded');
}
