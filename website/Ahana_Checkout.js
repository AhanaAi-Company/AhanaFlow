(function () {
  "use strict";

  function createController(config) {
    const state = { stripe: null, checkoutInFlight: false };

    function shouldUseEnterpriseLead(checkoutPlan) {
      if (config.enterpriseMode === "checkout") {
        return false;
      }
      return checkoutPlan === "enterprise";
    }

    function getEnv(name) {
      if (window.ENV && typeof window.ENV[name] === "string" && window.ENV[name].trim()) {
        return window.ENV[name].trim();
      }
      if (config.fallbackEnv && typeof config.fallbackEnv[name] === "string") {
        return config.fallbackEnv[name];
      }
      return "";
    }

    function setMessage(message, isError) {
      const el = document.getElementById(config.messageElementId || "form-message");
      if (!el) return;
      el.textContent = message;
      el.style.color = isError ? "#a93b31" : "#167d78";
    }

    function initializeStripe() {
      if (state.stripe) return state.stripe;
      const publishableKey = getEnv("PUBLIC_STRIPE_PUBLISHABLE_KEY");
      if (!window.Stripe || !publishableKey) return null;
      state.stripe = window.Stripe(publishableKey);
      return state.stripe;
    }

    function checkoutPlanForKey(planKey) {
      if (typeof config.resolvePlan === "function") {
        return config.resolvePlan(planKey);
      }
      const key = String(planKey || "").toLowerCase();
      if (key === "basic") return "basic";
      if (key === "pro") return "pro";
      return "enterprise";
    }

    function focusSignup(planKey) {
      const planInput = document.getElementById(config.planInputId || "plan");
      const emailInput = document.getElementById(config.emailInputId || "email");
      if (planInput && planKey) {
        planInput.value = planKey;
      }
      if (emailInput) {
        emailInput.focus();
      }
    }

    function redirectToSalesAssistedSignup(email, planName) {
      const fallbackSignup = getEnv("PUBLIC_SIGNUP_URL");
      if (!fallbackSignup) return false;
      const url = new URL(fallbackSignup);
      if (email) url.searchParams.set("email", email);
      if (planName) url.searchParams.set("plan", planName);
      url.searchParams.set("branch", getEnv("PUBLIC_BRANCH_CODE"));
      window.location.href = url.toString();
      return true;
    }

    async function tryEnterpriseLeadCapture(apiBase, email, metadata, headers) {
      if (!config.enterpriseLeadPath) return false;
      const endpoint = apiBase + config.enterpriseLeadPath;
      const payload = typeof config.buildEnterpriseLeadPayload === "function"
        ? config.buildEnterpriseLeadPayload(email, metadata)
        : {
            name: email,
            email: email,
            message: metadata && metadata.planName ? "Plan: " + metadata.planName : "Enterprise"
          };

      try {
        const response = await fetch(endpoint, {
          method: "POST",
          headers,
          body: JSON.stringify(payload)
        });
        if (!response.ok) return false;
        setMessage(config.enterpriseLeadSuccessMessage || "Lead captured! Our team will follow up shortly.", false);
        return true;
      } catch (error) {
        return false;
      }
    }

    async function startCheckout(planKey, email, metadata) {
      if (!planKey || state.checkoutInFlight) return;
      state.checkoutInFlight = true;
      setMessage(config.loadingMessage || "Preparing bounded billing handoff...", false);

      try {
        const apiBase = getEnv("PUBLIC_API_BASE_URL").replace(/\/$/, "");
        const checkoutPlan = checkoutPlanForKey(planKey);
        const headers = { "Content-Type": "application/json" };
        const apiKey = getEnv("PUBLIC_API_KEY");
        if (apiKey) headers[config.apiKeyHeader || "x-api-key"] = apiKey;

        if (shouldUseEnterpriseLead(checkoutPlan)) {
          const captured = await tryEnterpriseLeadCapture(apiBase, email, metadata, headers);
          if (captured) return;
          if (!redirectToSalesAssistedSignup(email, metadata && metadata.planName ? metadata.planName : "Enterprise")) {
            throw new Error("Enterprise onboarding requires PUBLIC_SIGNUP_URL.");
          }
          return;
        }

        if (!email) {
          throw new Error("Work email is required before billing handoff.");
        }

        const payload = typeof config.buildCheckoutPayload === "function"
          ? config.buildCheckoutPayload(planKey, checkoutPlan, email, metadata || {}, getEnv)
          : { plan: checkoutPlan, customer_email: email };

        if (!payload) {
          throw new Error("Checkout payload could not be prepared.");
        }

        const response = await fetch(
          apiBase + (config.checkoutPath || "/v1/billing/checkout"),
          {
            method: "POST",
            headers,
            body: JSON.stringify(payload)
          }
        );

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error("Checkout session failed: " + response.status + " " + errorText);
        }

        const data = await response.json();

        if (data.sessionId) {
          const stripe = initializeStripe();
          if (!stripe) throw new Error("Stripe is unavailable. Set PUBLIC_STRIPE_PUBLISHABLE_KEY.");
          const result = await stripe.redirectToCheckout({ sessionId: data.sessionId });
          if (result && result.error) throw new Error(result.error.message || "Stripe redirect failed");
          return;
        }

        if (data.checkoutUrl) {
          window.location.href = data.checkoutUrl;
          return;
        }

        if (data.url) {
          window.location.href = data.url;
          return;
        }

        if (data.mode === "subscription" && Array.isArray(data.line_items)) {
          if (redirectToSalesAssistedSignup(email, metadata && metadata.planName ? metadata.planName : planKey)) {
            return;
          }
          setMessage(config.stubMessage || "Billing stub accepted. Continue with sales-assisted onboarding until live Stripe cutover is enabled.", false);
          return;
        }

        throw new Error("Backend did not return a live checkout handoff.");
      } catch (error) {
        if (redirectToSalesAssistedSignup(email, metadata && metadata.planName ? metadata.planName : planKey)) {
          return;
        }
        setMessage((error && error.message) || "Unable to start checkout.", true);
      } finally {
        state.checkoutInFlight = false;
      }
    }

    function bindPlanButtons() {
      document.querySelectorAll(config.planButtonSelector || ".plan-btn").forEach(function (button) {
        button.addEventListener("click", function () {
          const key = button.getAttribute("data-price-key") || config.defaultPlanKey || "pro";
          const planName = button.getAttribute(config.planNameAttribute || "data-plan-name") || "Unknown";
          focusSignup(key);
          setMessage("Enter a work email to continue with " + planName + ".", false);
        });
      });
    }

    function bindSignupForm() {
      const form = document.getElementById(config.formId || "signup-form");
      if (!form) return;

      form.addEventListener("submit", function (event) {
        event.preventDefault();
        const email = String((document.getElementById(config.emailInputId || "email") || {}).value || "").trim();
        const plan = String((document.getElementById(config.planInputId || "plan") || {}).value || config.defaultPlanKey || "pro");

        if (!email) {
          setMessage("Please enter your work email.", true);
          return;
        }

        startCheckout(plan, email, { trigger: "signup_form", planName: plan });
      });
    }

    function init() {
      initializeStripe();
      bindPlanButtons();
      bindSignupForm();
    }

    return {
      getEnv: getEnv,
      init: init,
      initializeStripe: initializeStripe,
      startCheckout: startCheckout,
      state: state
    };
  }

  window.AhanaCheckout = { createController: createController };
})();
