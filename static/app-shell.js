(function () {
  const appDataNode = document.getElementById("app-data");
  if (!appDataNode) {
    return;
  }

  const STORAGE_KEY = "wholefoods-deals-profile-v8";
  const DEVICE_ID_KEY = "wholefoods-deals-device-id-v1";
  const rawData = JSON.parse(appDataNode.textContent || "{}");
  const feedbackEndpoint = rawData.feedback_endpoint || "/api/fixes";
  const profileEndpoint = rawData.profile_endpoint || "/api/profile";
  const newsletterSignupEndpoint = rawData.newsletter_signup_endpoint || "/api/newsletter/signup";
  const newsletterOnboardingEndpoint = rawData.newsletter_onboarding_endpoint || "/api/newsletter/onboarding";
  const subcategoryOptions = rawData.subcategory_options || {};
  const initialCategoryOrder = rawData.category_order || {};
  const stores = rawData.stores || [];
  const retailerOrder = ["All", "Whole Foods", "Target", "H Mart"];
  const failedCategory = "Other/Failed";
  const preferredCategoryOrder = [
    "Produce",
    "Meat & Seafood",
    "Dairy & Eggs",
    "Pantry",
    "International",
    "Bakery",
    "Frozen",
    "Snacks",
    "Prepared Foods",
    "Household",
    "Baby",
    "Beverages",
    "Beauty & Personal Care",
    "Supplements & Wellness",
    "Alcohol",
  ];

  const nodes = {
    searchInput: document.getElementById("global-search"),
    searchMeta: document.getElementById("search-meta"),
    retailerChipRow: document.getElementById("retailer-chip-row"),
    storeChipRow: document.getElementById("store-chip-row"),
    savedListToggle: document.getElementById("saved-list-toggle"),
    newsletterToggle: document.getElementById("newsletter-toggle"),
    filterDrawer: document.getElementById("filter-drawer"),
    filterCategory: document.getElementById("filter-category"),
    filterSubcategory: document.getElementById("filter-subcategory"),
    filterDiscount: document.getElementById("filter-discount"),
    clearFilters: document.getElementById("clear-filters"),
    feedGrid: document.getElementById("feed-grid"),
    categorySheetBackdrop: document.getElementById("category-sheet-backdrop"),
    categorySheet: document.getElementById("category-sheet"),
    categorySheetTitle: document.getElementById("category-sheet-title"),
    categorySheetCopy: document.getElementById("category-sheet-copy"),
    categoryScopeRow: document.getElementById("category-scope-row"),
    categorySelect: document.getElementById("category-select"),
    subcategorySelect: document.getElementById("subcategory-select"),
    brandFixField: document.getElementById("brand-fix-field"),
    brandFixInput: document.getElementById("brand-fix-input"),
    queueSubcategoryFix: document.getElementById("queue-subcategory-fix"),
    queueBrandFix: document.getElementById("queue-brand-fix"),
    categorySheetClose: document.getElementById("category-sheet-close"),
    newsletterSheetBackdrop: document.getElementById("newsletter-sheet-backdrop"),
    newsletterSheet: document.getElementById("newsletter-sheet"),
    newsletterSheetClose: document.getElementById("newsletter-sheet-close"),
    newsletterEmail: document.getElementById("newsletter-email"),
    newsletterStatus: document.getElementById("newsletter-status"),
    newsletterCadence: document.getElementById("newsletter-cadence"),
    newsletterStoreRow: document.getElementById("newsletter-store-row"),
    newsletterCategoryRow: document.getElementById("newsletter-category-row"),
    newsletterDislikedCategoryRow: document.getElementById("newsletter-disliked-category-row"),
    newsletterFavoriteBrands: document.getElementById("newsletter-favorite-brands"),
    newsletterHiddenBrands: document.getElementById("newsletter-hidden-brands"),
    newsletterBudgetSensitivity: document.getElementById("newsletter-budget-sensitivity"),
    newsletterSampleGrid: document.getElementById("newsletter-sample-grid"),
    newsletterSave: document.getElementById("newsletter-save"),
  };

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function normalizeProduct(product, index) {
    const normalizedSubcategory = product.ai_subcategory || product.subcategory || "";
    const normalizedCategory = product.ai_category || product.category || "Pantry";
    const sources = Array.isArray(product.sources) ? product.sources : [];
    const sourceCategories = Array.isArray(product.source_categories) ? product.source_categories : [];
    return {
      ...product,
      key: product.asin || (product.asins && product.asins[0]) || `product-${index}`,
      brand: product.brand || "",
      category: normalizedCategory,
      subcategory: normalizedSubcategory,
      ai_category: product.ai_category || normalizedCategory,
      ai_subcategory: product.ai_subcategory || normalizedSubcategory,
      retailer: product.retailer || "Whole Foods",
      tags: Array.isArray(product.tags) ? product.tags : [],
      sources,
      source_labels: Array.isArray(product.source_labels) && product.source_labels.length
        ? product.source_labels
        : sources.map(sourceLabel),
      source_categories: sourceCategories,
      available_store_ids: Array.isArray(product.available_store_ids) ? product.available_store_ids : [],
      store_offers: Array.isArray(product.store_offers) ? product.store_offers : [],
      discount_percent: Number(product.discount_percent || 0),
      source_count: Number(product.source_count || sources.length || 0),
      category_confidence: Number(product.category_confidence || 0),
    };
  }

  function sourceLabel(source) {
    const labels = {
      "Search Deals": "Search",
      "Target Deals": "Target",
      "H Mart Deals": "H Mart",
    };
    return labels[source] || source;
  }

  function hydrateProducts(list) {
    return (list || []).map(normalizeProduct);
  }

  function sortCategoryNames(list) {
    const unique = Array.from(new Set((list || []).filter(Boolean)));
    return unique.sort((left, right) => {
      if (left === failedCategory || right === failedCategory) {
        return left === failedCategory ? 1 : -1;
      }
      const leftIndex = preferredCategoryOrder.indexOf(left);
      const rightIndex = preferredCategoryOrder.indexOf(right);
      if (leftIndex !== -1 || rightIndex !== -1) {
        if (leftIndex === -1) {
          return 1;
        }
        if (rightIndex === -1) {
          return -1;
        }
        return leftIndex - rightIndex;
      }
      return left.localeCompare(right);
    });
  }

  let products = hydrateProducts(rawData.products || []);
  let productByKey = new Map();
  let retailerList = [];

  const categoryList = sortCategoryNames(
    Array.isArray(rawData.categories) && rawData.categories.length
      ? rawData.categories.concat(products.map((product) => product.category || "Pantry"))
      : products.map((product) => product.category || "Pantry")
  );
  const subcategoryEntries = Object.entries(subcategoryOptions).flatMap(([category, subcategories]) =>
    Object.keys(subcategories || {}).map((subcategory) => ({ category, subcategory }))
  );
  const subcategoryToCategory = Object.fromEntries(
    subcategoryEntries.map((entry) => [entry.subcategory, entry.category])
  );

  function deriveRetailerList(list) {
    const retailerSet = new Set((list || []).map((product) => product.retailer).filter(Boolean));
    return retailerOrder.filter((retailer) => retailer === "All" || retailerSet.has(retailer));
  }

  function rebuildDerivedCollections() {
    productByKey = new Map(products.map((product) => [product.key, product]));
    retailerList = deriveRetailerList(products);
  }

  rebuildDerivedCollections();

  function createDeviceId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return `device-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function getOrCreateDeviceId() {
    try {
      const existing = localStorage.getItem(DEVICE_ID_KEY);
      if (existing) {
        return existing;
      }
      const next = createDeviceId();
      localStorage.setItem(DEVICE_ID_KEY, next);
      return next;
    } catch (error) {
      return createDeviceId();
    }
  }

  const deviceId = getOrCreateDeviceId();

  function defaultFilters() {
    return {
      category: "",
      subcategory: "",
      minDiscount: "0",
    };
  }

  function normalizeSelectedStoreIds(selectedStoreIds) {
    const selected = Array.isArray(selectedStoreIds)
      ? selectedStoreIds.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    return selected.length === 1 ? [selected[0]] : [];
  }

  function normalizeProfile(profile) {
    const source = profile || {};
    return {
      ...getDefaultProfile(),
      ...source,
      selectedStoreIds: normalizeSelectedStoreIds(source.selectedStoreIds),
      likedKeys: Array.isArray(source.likedKeys) ? source.likedKeys : [],
      dislikedKeys: Array.isArray(source.dislikedKeys) ? source.dislikedKeys : [],
      savedKeys: Array.isArray(source.savedKeys) ? source.savedKeys : [],
      categoryOrderByRetailer: source.categoryOrderByRetailer || { ...initialCategoryOrder },
      newsletterEnabled: Boolean(source.newsletterEnabled),
      newsletterCadence: source.newsletterCadence || "daily",
      newsletterOnboardingCompleted: Boolean(source.newsletterOnboardingCompleted),
      newsletterEmail: source.newsletterEmail || "",
      onboardingAnswers: source.onboardingAnswers || {},
      newsletterPreferences: normalizeNewsletterPreferences(source.newsletterPreferences || {}),
      filters: {
        ...defaultFilters(),
        ...(source.filters || {}),
      },
    };
  }

  function normalizeNewsletterPreferences(preferences) {
    const source = preferences || {};
    return {
      preferredCategories: Array.isArray(source.preferredCategories) ? source.preferredCategories : [],
      dislikedCategories: Array.isArray(source.dislikedCategories) ? source.dislikedCategories : [],
      favoriteBrands: Array.isArray(source.favoriteBrands) ? source.favoriteBrands : [],
      hiddenBrands: Array.isArray(source.hiddenBrands) ? source.hiddenBrands : [],
      preferredStoreIds: Array.isArray(source.preferredStoreIds) ? source.preferredStoreIds : [],
      budgetSensitivity: source.budgetSensitivity || "",
      cadenceSettings: source.cadenceSettings || {},
      onboardingAnswers: source.onboardingAnswers || {},
      sampledProductFeedback: source.sampledProductFeedback || {},
    };
  }

  function getDefaultProfile() {
    return {
      selectedStoreIds: [],
      filters: defaultFilters(),
      likedKeys: [],
      dislikedKeys: [],
      savedKeys: [],
      categoryOrderByRetailer: { ...initialCategoryOrder },
      newsletterEnabled: false,
      newsletterCadence: "daily",
      newsletterOnboardingCompleted: false,
      newsletterEmail: "",
      onboardingAnswers: {},
      newsletterPreferences: normalizeNewsletterPreferences({}),
    };
  }

  function loadProfile() {
    try {
      return normalizeProfile(JSON.parse(localStorage.getItem(STORAGE_KEY) || "null"));
    } catch (error) {
      return getDefaultProfile();
    }
  }

  const state = {
    profile: loadProfile(),
    query: "",
    activeRetailer: retailerList.includes("All") ? "All" : retailerList[0] || "All",
    categoryTargetKey: null,
    categorySheetMode: "feedback",
    categoryScope: "similar",
    viewMode: "all",
    newsletterOpen: false,
    newsletterOnboarding: {
      subscriber: {},
      preferences: normalizeNewsletterPreferences({}),
      categories: categoryList.slice(),
      sample_products: [],
      stores,
    },
  };

  function saveProfile() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state.profile));
    } catch (error) {
      console.warn("Could not save profile locally:", error);
    }

    fetch(profileEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        device_id: deviceId,
        profile: state.profile,
      }),
    }).catch((error) => {
      console.warn("Could not save profile remotely:", error);
    });
  }

  function parseCsvList(value) {
    return Array.from(new Set(String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)));
  }

  function toggleChoice(list, value) {
    const current = Array.isArray(list) ? list.slice() : [];
    return current.includes(value)
      ? current.filter((item) => item !== value)
      : current.concat(value);
  }

  function openNewsletterSheet() {
    state.newsletterOpen = true;
    nodes.newsletterSheetBackdrop.classList.remove("hidden");
    nodes.newsletterSheet.classList.remove("hidden");
    nodes.newsletterSheet.setAttribute("aria-hidden", "false");
    renderNewsletterSheet();
    loadNewsletterOnboarding();
  }

  function closeNewsletterSheet() {
    state.newsletterOpen = false;
    nodes.newsletterSheetBackdrop.classList.add("hidden");
    nodes.newsletterSheet.classList.add("hidden");
    nodes.newsletterSheet.setAttribute("aria-hidden", "true");
  }

  function renderNewsletterChipRow(node, values, selectedValues, dataKey) {
    node.innerHTML = (values || []).map((entry) => {
      const value = entry.value || entry.id || entry.name;
      const label = entry.label || entry.name || entry.value || value;
      const selected = (selectedValues || []).includes(value);
      return `<button class="chip ${selected ? "is-selected" : ""}" data-newsletter-key="${escapeHtml(dataKey)}" data-newsletter-value="${escapeHtml(value)}" type="button">${escapeHtml(label)}</button>`;
    }).join("");
  }

  function sampleFeedbackForProduct(key) {
    return (state.profile.newsletterPreferences || {}).sampledProductFeedback?.[key] || "";
  }

  function renderNewsletterSamples() {
    const samples = state.newsletterOnboarding.sample_products || [];
    if (!samples.length) {
      nodes.newsletterSampleGrid.innerHTML = `<div class="empty-state">We’ll pull products from your saved list and strongest deals once the feed is loaded.</div>`;
      return;
    }

    nodes.newsletterSampleGrid.innerHTML = samples.map((product, index) => {
      const key = product.key || product.asin || `sample-${index}`;
      const feedback = sampleFeedbackForProduct(key);
      const image = product.image
        ? `<img src="${escapeHtml(product.image)}" alt="${escapeHtml(product.name)}">`
        : `<span class="image-fallback">No image</span>`;
      return `
        <article class="newsletter-sample-card">
          <div class="newsletter-sample-media">${image}</div>
          <div class="newsletter-sample-copy">
            <p class="deal-meta-line">${escapeHtml(product.retailer || "Whole Foods")} · ${escapeHtml(product.category || "Pantry")}</p>
            <h4>${escapeHtml(product.name)}</h4>
            <p class="prime">${escapeHtml(product.prime_price || product.current_price || "")}</p>
          </div>
          <div class="deal-actions">
            <button class="deal-action ${feedback === "thumbs_up" ? "is-active" : ""}" data-newsletter-feedback="thumbs_up" data-newsletter-product="${escapeHtml(key)}" type="button">Care</button>
            <button class="deal-action is-subtle ${feedback === "thumbs_down" ? "is-active" : ""}" data-newsletter-feedback="thumbs_down" data-newsletter-product="${escapeHtml(key)}" type="button">Nope</button>
            <button class="deal-action ${feedback === "save" ? "is-active" : ""}" data-newsletter-feedback="save" data-newsletter-product="${escapeHtml(key)}" type="button">Save</button>
          </div>
        </article>
      `;
    }).join("");
  }

  function emailLooksValid(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }

  function setNewsletterStatus(message, type = "error") {
    if (!nodes.newsletterStatus) {
      return;
    }
    nodes.newsletterStatus.textContent = message || "";
    nodes.newsletterStatus.classList.toggle("hidden", !message);
    nodes.newsletterStatus.classList.toggle("is-error", Boolean(message) && type === "error");
    nodes.newsletterStatus.classList.toggle("is-success", Boolean(message) && type === "success");
  }

  async function responseErrorMessage(response, fallback) {
    try {
      const payload = await response.json();
      if (payload?.error) {
        return payload.error;
      }
      if (payload?.message) {
        return payload.message;
      }
    } catch (error) {
      try {
        const text = await response.text();
        if (text) {
          return text;
        }
      } catch (_) {
        // Keep the cleaner fallback if the response body has already been read.
      }
    }
    return fallback;
  }

  function renderNewsletterSheet() {
    if (!nodes.newsletterSheet) {
      return;
    }
    const subscriber = state.newsletterOnboarding.subscriber || {};
    const preferences = state.profile.newsletterPreferences || normalizeNewsletterPreferences({});
    nodes.newsletterEmail.value = state.profile.newsletterEmail || subscriber.email || "";
    nodes.newsletterCadence.value = state.profile.newsletterCadence || subscriber.cadence || "daily";
    nodes.newsletterFavoriteBrands.value = (preferences.favoriteBrands || []).join(", ");
    nodes.newsletterHiddenBrands.value = (preferences.hiddenBrands || []).join(", ");
    nodes.newsletterBudgetSensitivity.value = preferences.budgetSensitivity || "";
    renderNewsletterChipRow(
      nodes.newsletterStoreRow,
      (state.newsletterOnboarding.stores || []).map((store) => ({ value: store.id, label: store.label || store.name || store.id })),
      preferences.preferredStoreIds || state.profile.selectedStoreIds || [],
      "preferredStoreIds"
    );
    renderNewsletterChipRow(
      nodes.newsletterCategoryRow,
      (state.newsletterOnboarding.categories || categoryList).map((name) => ({ value: name, label: name })),
      preferences.preferredCategories || [],
      "preferredCategories"
    );
    renderNewsletterChipRow(
      nodes.newsletterDislikedCategoryRow,
      (state.newsletterOnboarding.categories || categoryList).map((name) => ({ value: name, label: name })),
      preferences.dislikedCategories || [],
      "dislikedCategories"
    );
    renderNewsletterSamples();
  }

  async function loadNewsletterOnboarding() {
    try {
      const response = await fetch(`${newsletterOnboardingEndpoint}?device_id=${encodeURIComponent(deviceId)}`);
      if (!response.ok) {
        renderNewsletterSheet();
        return;
      }
      const payload = await response.json();
      if (payload?.onboarding) {
        state.newsletterOnboarding = {
          ...state.newsletterOnboarding,
          ...payload.onboarding,
        };
        if (payload.onboarding.preferences) {
          state.profile.newsletterPreferences = normalizeNewsletterPreferences(payload.onboarding.preferences);
        }
        renderNewsletterSheet();
      }
    } catch (error) {
      console.warn("Could not load newsletter onboarding:", error);
    }
  }

  async function saveNewsletterPreferences() {
    const currentPreferences = normalizeNewsletterPreferences({
      ...(state.profile.newsletterPreferences || {}),
      favoriteBrands: parseCsvList(nodes.newsletterFavoriteBrands.value),
      hiddenBrands: parseCsvList(nodes.newsletterHiddenBrands.value),
      budgetSensitivity: nodes.newsletterBudgetSensitivity.value,
    });
    const email = (nodes.newsletterEmail.value || "").trim().toLowerCase();
    nodes.newsletterEmail.closest(".sheet-field")?.classList.remove("has-error");
    setNewsletterStatus("");
    if (!emailLooksValid(email)) {
      nodes.newsletterEmail.closest(".sheet-field")?.classList.add("has-error");
      setNewsletterStatus("Enter a valid email address before saving your digest preferences.");
      nodes.newsletterSheet.scrollTo({ top: 0, behavior: "smooth" });
      nodes.newsletterEmail.focus();
      return;
    }
    nodes.newsletterSave.disabled = true;
    nodes.newsletterSave.textContent = "Saving...";
    state.profile.newsletterEnabled = true;
    state.profile.newsletterEmail = email;
    state.profile.newsletterCadence = nodes.newsletterCadence.value || "daily";
    state.profile.newsletterOnboardingCompleted = true;
    state.profile.newsletterPreferences = currentPreferences;
    saveProfile();

    const signupResponse = await fetch(newsletterSignupEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        device_id: deviceId,
        email: state.profile.newsletterEmail,
        cadence: state.profile.newsletterCadence,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "America/New_York",
      }),
    });
    if (!signupResponse.ok) {
      throw new Error(await responseErrorMessage(signupResponse, `Newsletter signup failed with status ${signupResponse.status}`));
    }

    const onboardingResponse = await fetch(newsletterOnboardingEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        device_id: deviceId,
        email: state.profile.newsletterEmail,
        cadence: state.profile.newsletterCadence,
        preferences: currentPreferences,
        answers: {
          cadence: state.profile.newsletterCadence,
          preferredStoreIds: currentPreferences.preferredStoreIds,
          preferredCategories: currentPreferences.preferredCategories,
          dislikedCategories: currentPreferences.dislikedCategories,
          favoriteBrands: currentPreferences.favoriteBrands,
          hiddenBrands: currentPreferences.hiddenBrands,
          budgetSensitivity: currentPreferences.budgetSensitivity,
          sampledProductFeedback: currentPreferences.sampledProductFeedback,
        },
      }),
    });
    if (!onboardingResponse.ok) {
      throw new Error(await responseErrorMessage(onboardingResponse, `Newsletter onboarding failed with status ${onboardingResponse.status}`));
    }
    const payload = await onboardingResponse.json();
    if (payload?.profile) {
      state.profile = normalizeProfile(payload.profile);
      saveProfile();
    }
    if (payload?.onboarding) {
      state.newsletterOnboarding = {
        ...state.newsletterOnboarding,
        ...payload.onboarding,
      };
    }
    renderFeed();
    renderNewsletterSheet();
    setNewsletterStatus("Newsletter preferences saved.", "success");
    nodes.newsletterSave.disabled = false;
    nodes.newsletterSave.textContent = "Save newsletter preferences";
  }

  function toggleValue(list, value) {
    const next = new Set(list || []);
    if (next.has(value)) {
      next.delete(value);
    } else {
      next.add(value);
    }
    return Array.from(next);
  }

  function effectiveSubcategory(product) {
    return product.subcategory || "";
  }

  function effectiveCategory(product) {
    return product.category || subcategoryToCategory[effectiveSubcategory(product)] || "Pantry";
  }

  function isFailedProduct(product) {
    return product.classification_status === "failed" || effectiveCategory(product) === failedCategory;
  }

  function selectedStoreLabel() {
    const selected = state.profile.selectedStoreIds || [];
    if (!selected.length) {
      return "All stores";
    }
    return stores
      .filter((store) => selected.includes(store.id))
      .map((store) => store.name || store.label || store.id)
      .join(", ") || "Selected stores";
  }

  function productVisibleForStores(product) {
    if (product.retailer !== "Whole Foods") {
      return true;
    }
    const selected = state.profile.selectedStoreIds || [];
    if (!selected.length) {
      return true;
    }
    const available = product.available_store_ids || [];
    if (!available.length) {
      return true;
    }
    return selected.some((storeId) => available.includes(storeId));
  }

  function textContainsQuery(product, query) {
    if (!query) {
      return true;
    }
    const haystack = [
      product.name,
      product.raw_name,
      product.brand,
      effectiveCategory(product),
      effectiveSubcategory(product),
      product.asin,
      product.retailer,
      (product.tags || []).join(" "),
      (product.sources || []).join(" "),
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  }

  function filterProduct(product) {
    const filters = state.profile.filters || defaultFilters();
    if (state.activeRetailer !== "All" && product.retailer !== state.activeRetailer) {
      return false;
    }
    if (!productVisibleForStores(product)) {
      return false;
    }
    if (!textContainsQuery(product, state.query)) {
      return false;
    }
    if (filters.category && effectiveCategory(product) !== filters.category) {
      return false;
    }
    if (filters.subcategory && effectiveSubcategory(product) !== filters.subcategory) {
      return false;
    }
    if (Number(filters.minDiscount || 0) && (product.discount_percent || 0) < Number(filters.minDiscount || 0)) {
      return false;
    }
    return true;
  }

  function scopedProducts() {
    return products.filter((product) => {
      if (state.viewMode === "saved" && !(state.profile.savedKeys || []).includes(product.key)) {
        return false;
      }
      return filterProduct(product);
    });
  }

  function hasActiveFilters() {
    const filters = state.profile.filters || defaultFilters();
    return Boolean(
      state.query
      || filters.category
      || filters.subcategory
      || Number(filters.minDiscount || 0)
    );
  }

  function parsePrice(value) {
    const match = String(value || "").match(/\$([0-9]+(?:\.[0-9]+)?)/);
    return match ? Number(match[1]) : Number.POSITIVE_INFINITY;
  }

  function buildAffinityCounts(keys) {
    const counts = { categories: {}, brands: {}, tags: {} };
    (keys || []).forEach((key) => {
      const product = productByKey.get(key);
      if (!product) {
        return;
      }
      const category = effectiveCategory(product);
      if (category) {
        counts.categories[category] = (counts.categories[category] || 0) + 1;
      }
      if (product.brand) {
        counts.brands[product.brand] = (counts.brands[product.brand] || 0) + 1;
      }
      (product.tags || []).forEach((tag) => {
        counts.tags[tag] = (counts.tags[tag] || 0) + 1;
      });
    });
    return counts;
  }

  function queryScore(product) {
    if (!state.query) {
      return 0;
    }
    const query = state.query;
    const name = (product.name || "").toLowerCase();
    const brand = (product.brand || "").toLowerCase();
    if (name.startsWith(query)) {
      return 140;
    }
    if (name.includes(query)) {
      return 100;
    }
    if (brand.startsWith(query)) {
      return 75;
    }
    if (brand.includes(query)) {
      return 55;
    }
    return 10;
  }

  function sourceCategoriesText(product) {
    return (product.source_categories || []).join(" ").toLowerCase();
  }

  function currentSelectedStoreId() {
    return (state.profile.selectedStoreIds || [])[0] || "";
  }

  function shelfAuthorityScore(product) {
    const category = effectiveCategory(product);
    const subcategory = effectiveSubcategory(product);
    const sourceCategoryText = sourceCategoriesText(product);
    const selectedStoreId = currentSelectedStoreId();
    let score = 0;

    if (selectedStoreId && (product.available_store_ids || []).includes(selectedStoreId)) {
      score += 28;
    }

    if (state.activeRetailer === "All" && product.retailer === "Whole Foods") {
      score += 10;
    }

    if (category === "Produce") {
      if (sourceCategoryText.includes("fresh produce")) {
        score += 220;
      }
      if (subcategory === "Fruits" || subcategory === "Vegetables") {
        score += 34;
      } else if (subcategory === "Cut Fruit & Veg") {
        score += 24;
      } else if (subcategory === "Salad Greens" || subcategory === "Fresh Herbs" || subcategory === "Mushrooms") {
        score += 16;
      }
      if (product.retailer === "Whole Foods") {
        score += 18;
      }
      return score;
    }

    if (category === "Meat & Seafood") {
      if (sourceCategoryText.includes("/meat/") || sourceCategoryText.includes("/seafood/")) {
        score += 80;
      }
      return score;
    }

    if (category === "Prepared Foods") {
      if (sourceCategoryText.includes("instant food") || sourceCategoryText.includes("quick food") || sourceCategoryText.includes("deli")) {
        score += 55;
      }
      return score;
    }

    if (category === "Pantry") {
      if (sourceCategoryText.includes("/oil & seasoning") || sourceCategoryText.includes("/canned food") || sourceCategoryText.includes("/seaweed & dried produce/")) {
        score += 26;
      }
    }

    return score;
  }

  function baseDealScore(product) {
    let score = (product.discount_percent || 0) * 3;
    if (product.prime_price) {
      score += 18;
    }
    if (product.basis_price) {
      score += 10;
    }
    if (!product.discount_percent && !product.basis_price) {
      score -= 18;
    }
    score += shelfAuthorityScore(product);
    score += Math.max(0, (product.source_count || 0) - 1) * 10;
    score += Math.round((product.category_confidence || 0) * 12);
    return score;
  }

  function preferenceScore(product, liked, disliked) {
    let score = 0;
    const category = effectiveCategory(product);
    const subcategory = effectiveSubcategory(product);
    const preferences = state.profile.newsletterPreferences || normalizeNewsletterPreferences({});
    if ((state.profile.likedKeys || []).includes(product.key)) {
      score += 72;
    }
    if ((state.profile.dislikedKeys || []).includes(product.key)) {
      score -= 180;
    }
    if ((state.profile.savedKeys || []).includes(product.key)) {
      score += 120;
    }
    if ((preferences.preferredCategories || []).includes(category)) {
      score += 42;
    }
    if (subcategory && (preferences.preferredCategories || []).includes(subcategory)) {
      score += 28;
    }
    if ((preferences.dislikedCategories || []).includes(category) || (subcategory && (preferences.dislikedCategories || []).includes(subcategory))) {
      score -= 110;
    }
    score += (liked.categories[category] || 0) * 16;
    score -= (disliked.categories[category] || 0) * 20;
    if (product.brand) {
      if ((preferences.favoriteBrands || []).includes(product.brand)) {
        score += 38;
      }
      if ((preferences.hiddenBrands || []).includes(product.brand)) {
        score -= 120;
      }
      score += (liked.brands[product.brand] || 0) * 16;
      score -= (disliked.brands[product.brand] || 0) * 22;
    }
    (product.tags || []).forEach((tag) => {
      score += (liked.tags[tag] || 0) * 8;
      score -= (disliked.tags[tag] || 0) * 10;
    });
    return score;
  }

  function formFactorKey(product) {
    const brandTokens = String(product.brand || "").toLowerCase().split(/\s+/).filter(Boolean);
    const brandSet = new Set(brandTokens);
    const stopwords = new Set(["organic", "fresh", "market", "whole", "foods", "brand", "natural", "mini", "large", "small", "count", "ct", "oz", "lb", "ea", "pack", "bag", "box"]);
    const tokens = String(product.name || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .split(/\s+/)
      .filter((token) => token && !stopwords.has(token) && !brandSet.has(token) && !/\d/.test(token));
    return tokens.slice(0, 3).join(" ");
  }

  function diversityPenalty(product, seen) {
    let penalty = 0;
    if (product.brand) {
      penalty += (seen.brands[product.brand] || 0) * 32;
    }
    const subcategory = effectiveSubcategory(product);
    if (subcategory) {
      penalty += (seen.subcategories[subcategory] || 0) * 18;
    }
    const form = formFactorKey(product);
    if (form) {
      penalty += (seen.forms[form] || 0) * 16;
    }
    return penalty;
  }

  function scoreProduct(product, liked, disliked) {
    return baseDealScore(product) + queryScore(product) + preferenceScore(product, liked, disliked);
  }

  function recommendationReason(product, liked) {
    if (product.brand && liked.brands[product.brand]) {
      return `You liked ${product.brand}`;
    }
    const category = effectiveCategory(product);
    if (category && liked.categories[category]) {
      return `You liked ${category}`;
    }
    const tag = (product.tags || []).find((value) => liked.tags[value]);
    if (tag) {
      return `You liked ${tag}`;
    }
    if ((product.source_count || 0) > 1) {
      return "Seen in multiple Whole Foods sources";
    }
    if ((product.discount_percent || 0) >= 30) {
      return "Strong discount";
    }
    return "Good deal match";
  }

  function rankProductList(list, mode) {
    const liked = buildAffinityCounts(state.profile.likedKeys);
    const disliked = buildAffinityCounts(state.profile.dislikedKeys);
    const ranked = list.map((product) => ({
      ...product,
      _score: scoreProduct(product, liked, disliked),
      _why: recommendationReason(product, liked),
    }));

    ranked.sort((left, right) => {
      return right._score - left._score || (right.discount_percent || 0) - (left.discount_percent || 0) || (left.name || "").localeCompare(right.name || "");
    });
    const selected = [];
    const remaining = ranked.slice();
    const seen = { brands: {}, subcategories: {}, forms: {} };
    while (remaining.length) {
      let bestIndex = 0;
      let bestScore = null;
      remaining.slice(0, 24).forEach((product, index) => {
        const adjusted = product._score - diversityPenalty(product, seen);
        if (bestScore === null || adjusted > bestScore) {
          bestScore = adjusted;
          bestIndex = index;
        }
      });
      const chosen = remaining.splice(bestIndex, 1)[0];
      selected.push(chosen);
      if (chosen.brand) {
        seen.brands[chosen.brand] = (seen.brands[chosen.brand] || 0) + 1;
      }
      const subcategory = effectiveSubcategory(chosen);
      if (subcategory) {
        seen.subcategories[subcategory] = (seen.subcategories[subcategory] || 0) + 1;
      }
      const form = formFactorKey(chosen);
      if (form) {
        seen.forms[form] = (seen.forms[form] || 0) + 1;
      }
    }
    return selected;
  }

  function buildCategoryShelves() {
    const grouped = new Map();
    scopedProducts().forEach((product) => {
      const category = effectiveCategory(product);
      if (!grouped.has(category)) {
        grouped.set(category, []);
      }
      grouped.get(category).push(product);
    });
    return sortCategoryNames(Array.from(grouped.keys())).map((category) => {
      const items = grouped.get(category) || [];
      return {
        category,
        total: items.length,
        items: rankProductList(items),
      };
    });
  }

  function renderRetailerChips() {
    nodes.retailerChipRow.innerHTML = retailerList.map((retailer) => {
      const selected = state.activeRetailer === retailer;
      return `<button class="chip ${selected ? "is-selected" : ""}" data-retailer="${escapeHtml(retailer)}" type="button">${escapeHtml(retailer)}</button>`;
    }).join("");
  }

  function renderStoreChips() {
    if (!stores.length) {
      nodes.storeChipRow.innerHTML = "";
      return;
    }
    const selectedStoreId = (state.profile.selectedStoreIds || [])[0] || "";
    nodes.storeChipRow.innerHTML = [
      `<button class="store-chip ${selectedStoreId ? "" : "is-selected"}" data-store-id="" type="button">All stores</button>`,
    ].concat(stores.map((store) => {
      const hasProducts = products.some((product) => product.retailer === "Whole Foods" && (product.available_store_ids || []).includes(store.id));
      const disabled = store.needs_store_id || (!hasProducts && !store.is_active);
      const label = store.label || store.name || store.id;
      const title = disabled ? "Store metadata is ready, but this store needs a verified Whole Foods store ID before scraping." : label;
      return `<button class="store-chip ${selectedStoreId === store.id ? "is-selected" : ""} ${disabled ? "is-disabled" : ""}" data-store-id="${escapeHtml(store.id)}" type="button" title="${escapeHtml(title)}"${disabled ? " aria-disabled=\"true\"" : ""}>${escapeHtml(label)}</button>`;
    })).join("");
  }

  function renderFilterOptions() {
    const filters = state.profile.filters || defaultFilters();
    nodes.filterCategory.innerHTML = `<option value="">Any category</option>` + categoryList
      .map((category) => `<option value="${escapeHtml(category)}"${filters.category === category ? " selected" : ""}>${escapeHtml(category)}</option>`)
      .join("");
    const subcategories = filters.category
      ? Object.keys(subcategoryOptions[filters.category] || {})
      : subcategoryEntries.map((entry) => entry.subcategory);
    nodes.filterSubcategory.innerHTML = `<option value="">Any subcategory</option>` + Array.from(new Set(subcategories)).sort((a, b) => a.localeCompare(b))
      .map((subcategory) => `<option value="${escapeHtml(subcategory)}"${filters.subcategory === subcategory ? " selected" : ""}>${escapeHtml(subcategory)}</option>`)
      .join("");
    nodes.filterDiscount.value = filters.minDiscount || "0";
  }

  function renderStatus() {
    const visible = scopedProducts();
    const savedCount = (state.profile.savedKeys || []).length;
    nodes.searchMeta.textContent = state.viewMode === "saved"
      ? `${visible.length.toLocaleString()} saved items`
      : `${visible.length.toLocaleString()} live deals`;
    if (nodes.savedListToggle) {
      nodes.savedListToggle.textContent = state.viewMode === "saved"
        ? "Back to deals"
        : `Saved list (${savedCount})`;
      nodes.savedListToggle.classList.toggle("is-selected", state.viewMode === "saved");
    }
    if (nodes.newsletterToggle) {
      nodes.newsletterToggle.textContent = state.profile.newsletterEnabled
        ? "Newsletter preferences"
        : "Sign up for newsletter!";
    }
  }

  function metaLine(product) {
    const pieces = [];
    const retailer = product.retailer || "";
    const brand = product.brand || "";
    if (brand && brand.toLowerCase() !== retailer.toLowerCase()) {
      pieces.push(escapeHtml(product.brand));
    }
    if (state.activeRetailer === "All") {
      pieces.push(escapeHtml(product.retailer));
    }
    if (isFailedProduct(product)) {
      pieces.push("Needs Review");
    }
    const subcategory = effectiveSubcategory(product);
    if (subcategory && subcategory !== effectiveCategory(product)) {
      pieces.push(escapeHtml(subcategory));
    }
    return pieces.length ? `<p class="deal-meta-line">${pieces.join(' <span class="meta-separator">·</span> ')}</p>` : "";
  }

  function priceLabel(product) {
    return product.prime_price ? `<p class="prime">${escapeHtml(product.prime_price)}</p>` : "";
  }

  function regularLabel(product) {
    if (!product.basis_price) {
      return "";
    }
    const regularText = String(product.basis_price);
    const normalized = regularText.toLowerCase();
    if (normalized.includes("vary") || normalized.startsWith("regular")) {
      return `<p class="deal-regular">${escapeHtml(regularText)}</p>`;
    }
    return `<p class="deal-regular">Was ${escapeHtml(regularText)}</p>`;
  }

  function discountLabel(product) {
    if (!product.discount) {
      return "";
    }
    const discountText = String(product.discount).replace(/\s+off$/i, "");
    return `<span class="deal-discount"><span class="deal-discount-kicker">Save</span>${escapeHtml(discountText)}</span>`;
  }

  function renderProductCard(product, options) {
    const liked = (state.profile.likedKeys || []).includes(product.key);
    const disliked = (state.profile.dislikedKeys || []).includes(product.key);
    const saved = (state.profile.savedKeys || []).includes(product.key);
    const imageMarkup = `
      <div class="deal-card-top">
        <div class="deal-image ${product.image ? "" : "is-empty"}">
          ${product.image ? `<img src="${escapeHtml(product.image)}" alt="${escapeHtml(product.name)}">` : `<span class="image-fallback">No image</span>`}
        </div>
        <button class="save-toggle ${saved ? "is-saved" : ""}" data-action="toggle-save" data-key="${escapeHtml(product.key)}" type="button">${saved ? "Saved" : "Save"}</button>
      </div>
    `;
    const titleMarkup = product.url
      ? `<h3 class="deal-title"><a href="${escapeHtml(product.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(product.name)}</a></h3>`
      : `<h3 class="deal-title">${escapeHtml(product.name)}</h3>`;
    const failedMarkup = isFailedProduct(product)
      ? `<p class="classification-warning">Was ${escapeHtml(product.failed_from_category || "Unknown")} · ${escapeHtml(product.failed_from_subcategory || "Unknown")}</p>`
      : "";

    return `
      <article class="deal-card" data-key="${escapeHtml(product.key)}">
        ${imageMarkup}
        ${metaLine(product)}
        ${titleMarkup}
        ${failedMarkup}
        <div class="deal-price-row">
          ${priceLabel(product)}
          ${discountLabel(product)}
        </div>
        ${regularLabel(product)}
        <div class="deal-actions">
          <button class="deal-action ${liked ? "is-active" : ""}" data-action="more-like-this" data-key="${escapeHtml(product.key)}" type="button">More</button>
          <button class="deal-action is-subtle ${disliked ? "is-active" : ""}" data-action="less-like-this" data-key="${escapeHtml(product.key)}" type="button">Less</button>
        </div>
        <button class="link-action feedback-link" data-action="change-category" data-key="${escapeHtml(product.key)}" type="button">This doesn't belong here</button>
      </article>
    `;
  }

  function renderEmpty(message) {
    nodes.feedGrid.className = "";
    nodes.feedGrid.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  }

  function renderShelves() {
    const shelves = buildCategoryShelves();
    if (!shelves.length) {
      renderEmpty(state.viewMode === "saved" ? "No saved items match these filters yet." : "No deals are available for these filters right now.");
      return;
    }

    nodes.feedGrid.className = "category-sections";
    nodes.feedGrid.innerHTML = shelves
      .map((shelf) => `
        <section class="category-section">
          <div class="category-section-head">
            <h3>${escapeHtml(shelf.category)}</h3>
          </div>
          <div class="category-track">
            ${shelf.items.map((product) => renderProductCard(product)).join("")}
          </div>
        </section>
      `)
      .join("");
  }

  function brandSignature(product) {
    const retailer = (product.retailer || "Unknown").toLowerCase();
    if (product.brand) {
      return `brand:${retailer}:${product.brand.toLowerCase()}`;
    }
    return `name:${retailer}:${(product.raw_name || product.name || "").toLowerCase()}`;
  }

  function subcategorySignature(product) {
    const retailer = (product.retailer || "Unknown").toLowerCase();
    if (product.brand) {
      const subcategory = (product.subcategory || product.category || "Pantry").toLowerCase();
      return `subcategory:${retailer}:${product.brand.toLowerCase()}:${subcategory}`;
    }
    if (product.subcategory) {
      return `subcategory:${retailer}:${product.subcategory.toLowerCase()}`;
    }
    return null;
  }

  function openCategorySheet(product, options) {
    const nextOptions = options || {};
    state.categoryTargetKey = product.key;
    state.categorySheetMode = nextOptions.mode || "feedback";
    state.categoryScope = nextOptions.scope || (subcategorySignature(product) ? "similar" : "item");
    renderCategorySheet(product);
    nodes.categorySheetBackdrop.classList.remove("hidden");
    nodes.categorySheet.classList.remove("hidden");
    nodes.categorySheet.setAttribute("aria-hidden", "false");
  }

  function closeCategorySheet() {
    state.categoryTargetKey = null;
    nodes.categorySheetBackdrop.classList.add("hidden");
    nodes.categorySheet.classList.add("hidden");
    nodes.categorySheet.setAttribute("aria-hidden", "true");
  }

  function renderSubcategorySelect(category, selectedSubcategory) {
    const currentOptions = Object.keys(subcategoryOptions[category] || {});
    nodes.subcategorySelect.innerHTML = currentOptions
      .map((subcategory) => `<option value="${escapeHtml(subcategory)}"${selectedSubcategory === subcategory ? " selected" : ""}>${escapeHtml(subcategory)}</option>`)
      .join("");
  }

  function renderCategorySheet(product) {
    const isGoldMode = state.categorySheetMode === "gold";
    const hasSimilar = Boolean(subcategorySignature(product));
    nodes.categorySheetTitle.textContent = isGoldMode ? "Fix this category" : "Improve this item";
    nodes.categorySheetCopy.textContent = isGoldMode
      ? "Save a gold label for this exact product. This only updates category placement for the next refresh."
      : "Send feedback for the next refresh. Brand fixes preview immediately here.";
    nodes.queueSubcategoryFix.textContent = isGoldMode ? "Save gold label" : "Send shelf feedback";
    nodes.brandFixField.classList.toggle("hidden", isGoldMode);
    nodes.categoryScopeRow.classList.toggle("hidden", isGoldMode);
    if (!isGoldMode) {
      nodes.categoryScopeRow.innerHTML = [
        `<button class="chip ${state.categoryScope === "item" ? "is-selected" : ""}" data-category-scope="item" type="button">Just this item</button>`,
        hasSimilar ? `<button class="chip ${state.categoryScope === "similar" ? "is-selected" : ""}" data-category-scope="similar" type="button">Similar items too</button>` : "",
      ].join("");
    }
    const currentCategory = effectiveCategory(product);
    nodes.categorySelect.innerHTML = categoryList
      .map((category) => `<option value="${escapeHtml(category)}"${currentCategory === category ? " selected" : ""}>${escapeHtml(category)}</option>`)
      .join("");
    renderSubcategorySelect(nodes.categorySelect.value, effectiveSubcategory(product));
    nodes.brandFixInput.value = product.brand || "";
  }

  async function submitFix(payload) {
    const response = await fetch(feedbackEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(`Fix request failed with status ${response.status}`);
    }
    return response.json();
  }

  function applySubcategoryOverride(product, category, subcategory) {
    closeCategorySheet();
    submitFix({
      kind: "subcategory",
      scope: state.categoryScope,
      product_key: product.key,
      signature: subcategorySignature(product),
      retailer: product.retailer,
      category,
      subcategory,
    }).then(() => {
      window.alert("Shelf feedback saved for the next refresh.");
    }).catch((error) => {
      console.warn("Could not apply subcategory fix:", error);
    });
  }

  function applyGoldCategoryLabel(product, category, subcategory) {
    products = products.map((candidate) => {
      if (candidate.key === product.key) {
        return {
          ...candidate,
          category,
          subcategory,
          ai_category: category,
          ai_subcategory: subcategory,
          classification_status: "gold",
        };
      }
      return candidate;
    });
    rebuildDerivedCollections();
    closeCategorySheet();
    renderFeed();
    submitFix({
      kind: "gold_category",
      category,
      subcategory,
      product: {
        asin: product.asin,
        url: product.url,
        name: product.name,
        raw_name: product.raw_name,
        brand: product.brand,
        retailer: product.retailer,
      },
    }).then(() => {
      window.alert("Gold label saved for the next refresh.");
    }).catch((error) => {
      console.warn("Could not save gold label:", error);
    });
  }

  function applyLocalBrandPreview(product, brand) {
    const signature = brandSignature(product);
    products = products.map((candidate) => {
      if (candidate.key === product.key || (state.categoryScope === "similar" && brandSignature(candidate) === signature)) {
        return { ...candidate, brand };
      }
      return candidate;
    });
    rebuildDerivedCollections();
  }

  function applyBrandOverride(product, brand) {
    const cleanedBrand = (brand || "").trim();
    if (!cleanedBrand) {
      return;
    }
    closeCategorySheet();
    applyLocalBrandPreview(product, cleanedBrand);
    renderFeed();
    submitFix({
      kind: "brand",
      scope: state.categoryScope,
      product_key: product.key,
      signature: brandSignature(product),
      retailer: product.retailer,
      brand: cleanedBrand,
    }).then(() => {
      console.info("Brand feedback saved for the next refresh.");
    }).catch((error) => {
      console.warn("Could not apply brand fix:", error);
    });
  }

  function moveCategory(category, direction) {
    const shelves = buildCategoryShelves().map((shelf) => shelf.category);
    const orderKey = state.activeRetailer === "All" ? "All" : state.activeRetailer;
    const currentOrder = (state.profile.categoryOrderByRetailer || {})[orderKey]
      ? state.profile.categoryOrderByRetailer[orderKey].filter((item) => shelves.includes(item))
      : [];
    const workingOrder = currentOrder.concat(shelves.filter((item) => !currentOrder.includes(item)));
    const index = workingOrder.indexOf(category);
    const swapIndex = direction === "up" ? index - 1 : index + 1;
    if (index === -1 || swapIndex < 0 || swapIndex >= workingOrder.length) {
      return;
    }
    const nextOrder = workingOrder.slice();
    [nextOrder[index], nextOrder[swapIndex]] = [nextOrder[swapIndex], nextOrder[index]];
    state.profile.categoryOrderByRetailer = {
      ...(state.profile.categoryOrderByRetailer || {}),
      [orderKey]: nextOrder,
    };
    saveProfile();
    renderFeed();
  }

  function applyPreferenceSignals(product, direction) {
    const currentKey = direction === "up" ? "likedKeys" : "dislikedKeys";
    const oppositeKey = direction === "up" ? "dislikedKeys" : "likedKeys";
    state.profile[oppositeKey] = (state.profile[oppositeKey] || []).filter((key) => key !== product.key);
    state.profile[currentKey] = toggleValue(state.profile[currentKey], product.key);
    saveProfile();
  }

  function renderFeed() {
    renderRetailerChips();
    renderStoreChips();
    renderFilterOptions();
    renderStatus();
    renderShelves();
  }

  async function loadRemoteProfile() {
    try {
      const response = await fetch(`${profileEndpoint}?device_id=${encodeURIComponent(deviceId)}`);
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      if (payload && payload.profile) {
        state.profile = normalizeProfile(payload.profile);
        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(state.profile));
        } catch (error) {
          console.warn("Could not refresh local profile cache:", error);
        }
        renderFeed();
        return;
      }
      saveProfile();
    } catch (error) {
      console.warn("Could not load profile remotely:", error);
    }
  }

  function handleAction(action, key) {
    if (action === "move-category-up") {
      moveCategory(key, "up");
      return;
    }
    if (action === "move-category-down") {
      moveCategory(key, "down");
      return;
    }
    const product = productByKey.get(key);
    if (!product) {
      return;
    }
    if (action === "more-like-this") {
      applyPreferenceSignals(product, "up");
      renderFeed();
      return;
    }
    if (action === "less-like-this") {
      applyPreferenceSignals(product, "down");
      renderFeed();
      return;
    }
    if (action === "toggle-save") {
      state.profile.savedKeys = toggleValue(state.profile.savedKeys || [], product.key);
      saveProfile();
      renderFeed();
      return;
    }
    if (action === "change-category") {
      openCategorySheet(product, { mode: "gold", scope: "item" });
    }
  }

  function updateFilter(key, value) {
    state.profile.filters = {
      ...defaultFilters(),
      ...(state.profile.filters || {}),
      [key]: value,
    };
    if (key === "category") {
      state.profile.filters.subcategory = "";
    }
    saveProfile();
    renderFeed();
  }

  document.body.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-action]");
    if (actionButton) {
      handleAction(actionButton.dataset.action, actionButton.dataset.key || actionButton.dataset.category);
      return;
    }

    const retailerButton = event.target.closest("[data-retailer]");
    if (retailerButton) {
      state.activeRetailer = retailerButton.dataset.retailer;
      renderFeed();
      return;
    }

    const storeButton = event.target.closest("[data-store-id]");
    if (storeButton && !storeButton.classList.contains("is-disabled")) {
      state.profile.selectedStoreIds = storeButton.dataset.storeId ? [storeButton.dataset.storeId] : [];
      saveProfile();
      renderFeed();
      return;
    }

    const scopeButton = event.target.closest("[data-category-scope]");
    if (scopeButton && state.categoryTargetKey) {
      state.categoryScope = scopeButton.dataset.categoryScope;
      renderCategorySheet(productByKey.get(state.categoryTargetKey));
      return;
    }

    const newsletterChip = event.target.closest("[data-newsletter-key]");
    if (newsletterChip) {
      const key = newsletterChip.dataset.newsletterKey;
      const value = newsletterChip.dataset.newsletterValue;
      const preferences = normalizeNewsletterPreferences(state.profile.newsletterPreferences || {});
      preferences[key] = toggleChoice(preferences[key] || [], value);
      state.profile.newsletterPreferences = preferences;
      renderNewsletterSheet();
      return;
    }

    const newsletterFeedbackButton = event.target.closest("[data-newsletter-feedback]");
    if (newsletterFeedbackButton) {
      const productKey = newsletterFeedbackButton.dataset.newsletterProduct;
      const action = newsletterFeedbackButton.dataset.newsletterFeedback;
      const preferences = normalizeNewsletterPreferences(state.profile.newsletterPreferences || {});
      preferences.sampledProductFeedback = {
        ...(preferences.sampledProductFeedback || {}),
        [productKey]: action,
      };
      state.profile.newsletterPreferences = preferences;
      if (action === "thumbs_up") {
        applyPreferenceSignals(productByKey.get(productKey) || { key: productKey }, "up");
      } else if (action === "thumbs_down") {
        applyPreferenceSignals(productByKey.get(productKey) || { key: productKey }, "down");
      } else if (action === "save") {
        state.profile.savedKeys = toggleValue(state.profile.savedKeys || [], productKey);
        saveProfile();
      }
      renderFeed();
      renderNewsletterSheet();
    }
  });

  nodes.clearFilters.addEventListener("click", () => {
    state.profile.filters = defaultFilters();
    saveProfile();
    renderFeed();
  });
  nodes.filterCategory.addEventListener("change", () => updateFilter("category", nodes.filterCategory.value));
  nodes.filterSubcategory.addEventListener("change", () => updateFilter("subcategory", nodes.filterSubcategory.value));
  nodes.filterDiscount.addEventListener("change", () => updateFilter("minDiscount", nodes.filterDiscount.value));
  nodes.categorySheetBackdrop.addEventListener("click", closeCategorySheet);
  nodes.categorySheetClose.addEventListener("click", closeCategorySheet);
  nodes.queueSubcategoryFix.addEventListener("click", () => {
    const product = productByKey.get(state.categoryTargetKey);
    if (product) {
      if (state.categorySheetMode === "gold") {
        applyGoldCategoryLabel(product, nodes.categorySelect.value, nodes.subcategorySelect.value);
      } else {
        applySubcategoryOverride(product, nodes.categorySelect.value, nodes.subcategorySelect.value);
      }
    }
  });
  nodes.queueBrandFix.addEventListener("click", () => {
    const product = productByKey.get(state.categoryTargetKey);
    if (product) {
      applyBrandOverride(product, nodes.brandFixInput.value);
    }
  });
  nodes.searchInput.addEventListener("input", () => {
    state.query = (nodes.searchInput.value || "").trim().toLowerCase();
    renderFeed();
  });
  if (nodes.savedListToggle) {
    nodes.savedListToggle.addEventListener("click", () => {
      state.viewMode = state.viewMode === "saved" ? "all" : "saved";
      renderFeed();
    });
  }
  nodes.categorySelect.addEventListener("change", () => {
    renderSubcategorySelect(nodes.categorySelect.value, "");
  });
  if (nodes.newsletterToggle) {
    nodes.newsletterToggle.addEventListener("click", openNewsletterSheet);
  }
  if (nodes.newsletterSheetBackdrop) {
    nodes.newsletterSheetBackdrop.addEventListener("click", closeNewsletterSheet);
  }
  if (nodes.newsletterSheetClose) {
    nodes.newsletterSheetClose.addEventListener("click", closeNewsletterSheet);
  }
  if (nodes.newsletterSave) {
    nodes.newsletterSave.addEventListener("click", () => {
      saveNewsletterPreferences().catch((error) => {
        console.warn("Could not save newsletter preferences:", error);
        setNewsletterStatus(error.message || "Could not save newsletter preferences right now.");
        nodes.newsletterSave.disabled = false;
        nodes.newsletterSave.textContent = "Save newsletter preferences";
      });
    });
  }

  renderFeed();
  loadRemoteProfile();
})();
