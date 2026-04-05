(function () {
  const appDataNode = document.getElementById("app-data");
  if (!appDataNode) {
    return;
  }

  const STORAGE_KEY = "wholefoods-deals-profile-v1";
  const rawData = JSON.parse(appDataNode.textContent || "{}");
  const products = (rawData.products || []).map((product, index) => ({
    ...product,
    key: product.asin || (product.asins && product.asins[0]) || `product-${index}`,
    brand: product.brand || "",
    category: product.category || "Pantry",
    tags: Array.isArray(product.tags) ? product.tags : [],
    sources: Array.isArray(product.sources) ? product.sources : [],
    available_store_ids: Array.isArray(product.available_store_ids) ? product.available_store_ids : [],
    discount_percent: Number(product.discount_percent || 0),
  }));
  const stores = rawData.stores || [];

  const categoryList = Array.from(new Set(products.map((product) => product.category).filter(Boolean))).sort();
  const tagList = Array.from(
    new Set(products.flatMap((product) => product.tags || []).filter(Boolean))
  ).sort();
  const brandList = Array.from(
    new Set(products.map((product) => product.brand).filter(Boolean))
  )
    .sort()
    .slice(0, 28);

  const nodes = {
    installTrigger: document.getElementById("install-trigger"),
    installSheet: document.getElementById("install-sheet"),
    installClose: document.getElementById("install-close"),
    preferencesTrigger: document.getElementById("preferences-trigger"),
    preferencesClose: document.getElementById("preferences-close"),
    preferencesSheet: document.getElementById("preferences-sheet"),
    saveProfile: document.getElementById("save-profile"),
    resetProfile: document.getElementById("reset-profile"),
    backdrop: document.getElementById("sheet-backdrop"),
    searchInput: document.getElementById("global-search"),
    summaryLine: document.getElementById("summary-line"),
    searchMeta: document.getElementById("search-meta"),
    storeSummary: document.getElementById("store-summary"),
    forYouGrid: document.getElementById("for-you-grid"),
    forYouHighlights: document.getElementById("for-you-highlights"),
    forYouCount: document.getElementById("for-you-count"),
    forYouCopy: document.getElementById("for-you-copy"),
    categoryChipRow: document.getElementById("category-chip-row"),
    categoryGrid: document.getElementById("category-grid"),
    categorySummary: document.getElementById("category-summary"),
    savedGrid: document.getElementById("saved-grid"),
    savedCount: document.getElementById("saved-count"),
    hiddenGrid: document.getElementById("hidden-grid"),
    hiddenCount: document.getElementById("hidden-count"),
    searchGrid: document.getElementById("search-grid"),
    searchCount: document.getElementById("search-count"),
    searchCopy: document.getElementById("search-copy"),
    storeChipRow: document.getElementById("store-chip-row"),
    preferenceCategoryRow: document.getElementById("preference-category-row"),
    dietChipRow: document.getElementById("diet-chip-row"),
    brandChipRow: document.getElementById("brand-chip-row"),
    tabButtons: Array.from(document.querySelectorAll(".tab-button")),
    panels: Array.from(document.querySelectorAll(".panel")),
  };

  function getDefaultProfile() {
    return {
      selectedStoreIds: stores.filter((store) => store.is_active).map((store) => store.id),
      favoriteCategories: [],
      favoriteTags: [],
      favoriteBrands: [],
      savedKeys: [],
      hiddenKeys: [],
      openedKeys: [],
      onboardingCompleted: false,
    };
  }

  function loadProfile() {
    try {
      const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "null");
      return { ...getDefaultProfile(), ...(saved || {}) };
    } catch (error) {
      return getDefaultProfile();
    }
  }

  const state = {
    profile: loadProfile(),
    activeTab: "for-you",
    query: "",
    activeCategory: "All",
  };

  function saveProfile() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.profile));
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

  function textContainsQuery(product, query) {
    if (!query) {
      return true;
    }
    const haystack = [
      product.name,
      product.brand,
      product.category,
      product.asin,
      (product.asins || []).join(" "),
      (product.tags || []).join(" "),
      (product.sources || []).join(" "),
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(query);
  }

  function productVisibleForStores(product) {
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

  function getAffinityCounts(keys) {
    const counts = { categories: {}, brands: {}, tags: {} };
    keys.forEach((key) => {
      const product = products.find((item) => item.key === key);
      if (!product) {
        return;
      }
      if (product.category) {
        counts.categories[product.category] = (counts.categories[product.category] || 0) + 1;
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

  function scoreProduct(product) {
    const reasons = [];
    let score = product.discount_percent || 0;

    if ((product.sources || []).length > 1) {
      score += 14;
      reasons.push("Seen in multiple deal feeds");
    }

    if (state.profile.favoriteCategories.includes(product.category)) {
      score += 38;
      reasons.push(`Matches your ${product.category} picks`);
    }

    if (state.profile.favoriteBrands.includes(product.brand)) {
      score += 34;
      reasons.push(`From a brand you like: ${product.brand}`);
    }

    const matchingTags = (product.tags || []).filter((tag) => state.profile.favoriteTags.includes(tag));
    if (matchingTags.length) {
      score += 18 * matchingTags.length;
      reasons.push(`Fits your ${matchingTags.join(", ")} preferences`);
    }

    const affinities = getAffinityCounts([
      ...(state.profile.savedKeys || []),
      ...(state.profile.openedKeys || []),
    ]);

    if (product.category && affinities.categories[product.category]) {
      score += affinities.categories[product.category] * 7;
      reasons.push(`Similar to categories you keep revisiting`);
    }

    if (product.brand && affinities.brands[product.brand]) {
      score += affinities.brands[product.brand] * 8;
      reasons.push(`Similar to brands you've opened before`);
    }

    const affinityTagMatch = (product.tags || []).find((tag) => affinities.tags[tag]);
    if (affinityTagMatch) {
      score += affinities.tags[affinityTagMatch] * 5;
      reasons.push(`Aligned with your recent ${affinityTagMatch} interest`);
    }

    if (product.prime_price) {
      score += 4;
    }

    return { score, explanation: reasons[0] || "Strong overall deal value" };
  }

  function getVisibleProducts() {
    return products.filter((product) => {
      if ((state.profile.hiddenKeys || []).includes(product.key)) {
        return false;
      }
      if (!productVisibleForStores(product)) {
        return false;
      }
      return textContainsQuery(product, state.query);
    });
  }

  function getRecommendedProducts() {
    return getVisibleProducts()
      .map((product) => ({ ...product, _score: scoreProduct(product) }))
      .sort((left, right) => {
        if (right._score.score !== left._score.score) {
          return right._score.score - left._score.score;
        }
        return (right.discount_percent || 0) - (left.discount_percent || 0);
      });
  }

  function renderEmpty(target, message) {
    target.innerHTML = `<div class="empty-state">${message}</div>`;
  }

  function formatSources(product) {
    return (product.sources || [])
      .map((source) => `<span class="source-pill">${source}</span>`)
      .join("");
  }

  function formatTags(product) {
    return (product.tags || [])
      .slice(0, 4)
      .map((tag) => `<span class="chip is-muted">${tag}</span>`)
      .join("");
  }

  function renderProductCard(product, explanation) {
    const saved = (state.profile.savedKeys || []).includes(product.key);
    const hidden = (state.profile.hiddenKeys || []).includes(product.key);
    const imageMarkup = product.image
      ? `<div class="deal-image"><img src="${product.image}" alt="${escapeHtml(product.name)}"></div>`
      : `<div class="deal-image"><div class="empty-state">No image</div></div>`;
    const url = product.url || "#";
    return `
      <article class="deal-card ${hidden ? "is-hidden-card" : ""}" data-key="${product.key}">
        ${imageMarkup}
        <div class="deal-brand">${escapeHtml(product.brand || product.category || "Deal")}</div>
        <h3 class="deal-title"><a href="${url}" target="_blank" rel="noopener noreferrer">${escapeHtml(product.emoji || "🛒")} ${escapeHtml(product.name)}</a></h3>
        ${product.prime_price ? `<p class="prime">🔥 ${escapeHtml(product.prime_price)}</p>` : ""}
        ${product.basis_price ? `<p class="deal-regular">Regular ${escapeHtml(product.basis_price)}</p>` : ""}
        ${product.discount ? `<p class="deal-discount">${escapeHtml(product.discount)}</p>` : ""}
        <div class="deal-meta">Category: ${escapeHtml(product.category || "Pantry")}${product.unit_price ? ` · Unit: ${escapeHtml(product.unit_price)}` : ""}</div>
        ${explanation ? `<div class="deal-explanation">${escapeHtml(explanation)}</div>` : ""}
        <div class="deal-pill-row">${formatSources(product)}${formatTags(product)}</div>
        <div class="deal-actions">
          <button class="deal-action ${saved ? "is-active" : ""}" data-action="save" data-key="${product.key}" type="button">${saved ? "Saved" : "Save"}</button>
          <button class="deal-action is-subtle ${hidden ? "is-active" : ""}" data-action="hide" data-key="${product.key}" type="button">${hidden ? "Hidden" : "Hide"}</button>
          <button class="deal-action" data-action="open" data-key="${product.key}" type="button">View</button>
        </div>
      </article>
    `;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderGrid(target, items, emptyMessage, explanationFn) {
    if (!items.length) {
      renderEmpty(target, emptyMessage);
      return;
    }
    target.innerHTML = items.map((item) => renderProductCard(item, explanationFn ? explanationFn(item) : "")).join("");
  }

  function renderHighlights(recommended) {
    const highlights = [];
    if (state.profile.favoriteCategories.length) {
      highlights.push(`Favorite categories: ${state.profile.favoriteCategories.slice(0, 3).join(", ")}`);
    }
    if (state.profile.favoriteBrands.length) {
      highlights.push(`Favorite brands: ${state.profile.favoriteBrands.slice(0, 3).join(", ")}`);
    }
    if (state.profile.favoriteTags.length) {
      highlights.push(`Priority tags: ${state.profile.favoriteTags.slice(0, 3).join(", ")}`);
    }
    if (!highlights.length) {
      highlights.push("Save or hide deals to train the feed.");
    }
    nodes.forYouHighlights.innerHTML = highlights
      .map((text) => `<span class="chip is-selected">${escapeHtml(text)}</span>`)
      .join("");
    nodes.forYouCopy.textContent = recommended.length
      ? "Your best matches, blending discounts with what you've told the app and what you've interacted with."
      : "Set a few preferences or broaden your search to build your personalized feed.";
  }

  function renderCategoryChips() {
    const chips = ["All", ...categoryList];
    nodes.categoryChipRow.innerHTML = chips
      .map((category) => {
        const selected = state.activeCategory === category;
        return `<button class="chip ${selected ? "is-selected" : ""}" data-category="${escapeHtml(category)}" type="button">${escapeHtml(category)}</button>`;
      })
      .join("");
  }

  function renderPreferenceChips(target, values, selectedValues) {
    target.innerHTML = values
      .map((value) => {
        const selected = selectedValues.includes(value);
        return `<button class="chip ${selected ? "is-selected" : ""}" data-value="${escapeHtml(value)}" type="button">${escapeHtml(value)}</button>`;
      })
      .join("");
  }

  function updateHeaderSummary(visibleCount) {
    const selectedStores = stores.filter((store) => state.profile.selectedStoreIds.includes(store.id));
    nodes.storeSummary.textContent = selectedStores.length
      ? `Store: ${selectedStores.map((store) => store.name).join(", ")}`
      : "Store: All";
    nodes.searchMeta.textContent = state.query
      ? `Showing ${visibleCount} matching products`
      : `Showing ${visibleCount} products`;
    nodes.summaryLine.textContent = state.profile.onboardingCompleted
      ? "Curated deals that learn from what you save, hide and search."
      : "Choose categories, tags and brands to build a custom grocery feed.";
  }

  function renderPanels() {
    const visibleProducts = getVisibleProducts();
    const recommended = getRecommendedProducts().slice(0, 80);
    const savedProducts = products.filter((product) => (state.profile.savedKeys || []).includes(product.key));
    const hiddenProducts = products.filter((product) => (state.profile.hiddenKeys || []).includes(product.key));
    const categoryProducts = visibleProducts
      .filter((product) => state.activeCategory === "All" || product.category === state.activeCategory)
      .sort((left, right) => (right.discount_percent || 0) - (left.discount_percent || 0))
      .slice(0, 80);
    const searchProducts = visibleProducts
      .slice()
      .sort((left, right) => (right.discount_percent || 0) - (left.discount_percent || 0))
      .slice(0, 120);

    renderHighlights(recommended);
    renderGrid(
      nodes.forYouGrid,
      recommended,
      "No personalized matches yet. Try choosing a category or searching for a favorite item.",
      (item) => item._score.explanation
    );
    renderGrid(
      nodes.categoryGrid,
      categoryProducts,
      "No products match this category right now.",
      (item) => `Top ${item.category} deal with ${item.discount || "strong value"}`
    );
    renderGrid(
      nodes.savedGrid,
      savedProducts,
      "Save deals to build a shortlist.",
      () => "Saved by you"
    );
    renderGrid(
      nodes.hiddenGrid,
      hiddenProducts,
      "Nothing hidden right now.",
      () => "Hidden from your feed"
    );
    renderGrid(
      nodes.searchGrid,
      searchProducts,
      state.query ? "No search matches. Try another term." : "Search to explore the full catalog.",
      (item) => `Found in ${item.sources.join(", ")}`
    );

    nodes.forYouCount.textContent = `${recommended.length} picks`;
    nodes.savedCount.textContent = `${savedProducts.length} saved`;
    nodes.hiddenCount.textContent = `${hiddenProducts.length} hidden`;
    nodes.searchCount.textContent = `${searchProducts.length} results`;
    nodes.categorySummary.textContent = state.activeCategory === "All"
      ? "All categories"
      : `${state.activeCategory} deals`;
    nodes.searchCopy.textContent = state.query
      ? `Results for "${state.query}" across the combined catalog.`
      : "Search across the combined catalog of flyer, all deals and search deals.";

    updateHeaderSummary(visibleProducts.length);
    renderCategoryChips();
  }

  function openSheet(sheet) {
    nodes.backdrop.classList.remove("hidden");
    sheet.classList.remove("hidden");
    sheet.setAttribute("aria-hidden", "false");
  }

  function closeSheets() {
    nodes.backdrop.classList.add("hidden");
    [nodes.preferencesSheet, nodes.installSheet].forEach((sheet) => {
      sheet.classList.add("hidden");
      sheet.setAttribute("aria-hidden", "true");
    });
  }

  function setActiveTab(tab) {
    state.activeTab = tab;
    nodes.tabButtons.forEach((button) => {
      button.classList.toggle("is-active", button.dataset.tab === tab);
    });
    nodes.panels.forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.panel === tab);
    });
  }

  function handleAction(action, key) {
    if (action === "save") {
      state.profile.savedKeys = toggleValue(state.profile.savedKeys, key);
      saveProfile();
      renderPanels();
      return;
    }

    if (action === "hide") {
      state.profile.hiddenKeys = toggleValue(state.profile.hiddenKeys, key);
      state.profile.savedKeys = (state.profile.savedKeys || []).filter((savedKey) => savedKey !== key);
      saveProfile();
      renderPanels();
      return;
    }

    if (action === "open") {
      state.profile.openedKeys = toggleValue(state.profile.openedKeys, key);
      saveProfile();
      const product = products.find((item) => item.key === key);
      if (product && product.url) {
        window.open(product.url, "_blank", "noopener,noreferrer");
      }
      renderPanels();
    }
  }

  function bindDynamicEvents() {
    document.body.addEventListener("click", (event) => {
      const tabButton = event.target.closest("[data-tab]");
      if (tabButton) {
        setActiveTab(tabButton.dataset.tab);
        return;
      }

      const actionButton = event.target.closest("[data-action]");
      if (actionButton) {
        handleAction(actionButton.dataset.action, actionButton.dataset.key);
        return;
      }

      const categoryButton = event.target.closest("[data-category]");
      if (categoryButton) {
        state.activeCategory = categoryButton.dataset.category;
        renderPanels();
        return;
      }

      const preferenceValue = event.target.closest("#preference-category-row [data-value]");
      if (preferenceValue) {
        state.profile.favoriteCategories = toggleValue(state.profile.favoriteCategories, preferenceValue.dataset.value);
        renderPreferenceRows();
        renderPanels();
        return;
      }

      const dietValue = event.target.closest("#diet-chip-row [data-value]");
      if (dietValue) {
        state.profile.favoriteTags = toggleValue(state.profile.favoriteTags, dietValue.dataset.value);
        renderPreferenceRows();
        renderPanels();
        return;
      }

      const brandValue = event.target.closest("#brand-chip-row [data-value]");
      if (brandValue) {
        state.profile.favoriteBrands = toggleValue(state.profile.favoriteBrands, brandValue.dataset.value);
        renderPreferenceRows();
        renderPanels();
        return;
      }

      const storeValue = event.target.closest("#store-chip-row [data-value]");
      if (storeValue) {
        state.profile.selectedStoreIds = toggleValue(state.profile.selectedStoreIds, storeValue.dataset.value);
        if (!state.profile.selectedStoreIds.length && stores[0]) {
          state.profile.selectedStoreIds = [stores[0].id];
        }
        renderPreferenceRows();
        renderPanels();
      }
    });
  }

  function renderPreferenceRows() {
    renderPreferenceChips(
      nodes.storeChipRow,
      stores.map((store) => store.id),
      state.profile.selectedStoreIds
    );
    nodes.storeChipRow.querySelectorAll("[data-value]").forEach((button, index) => {
      button.textContent = stores[index].label || stores[index].name;
    });
    renderPreferenceChips(nodes.preferenceCategoryRow, categoryList, state.profile.favoriteCategories);
    renderPreferenceChips(nodes.dietChipRow, tagList, state.profile.favoriteTags);
    renderPreferenceChips(nodes.brandChipRow, brandList, state.profile.favoriteBrands);
  }

  function bindStaticEvents() {
    nodes.searchInput.addEventListener("input", () => {
      state.query = (nodes.searchInput.value || "").trim().toLowerCase();
      if (state.query && state.activeTab !== "search") {
        setActiveTab("search");
      }
      renderPanels();
    });

    nodes.preferencesTrigger.addEventListener("click", () => openSheet(nodes.preferencesSheet));
    nodes.preferencesClose.addEventListener("click", closeSheets);
    nodes.installTrigger.addEventListener("click", () => openSheet(nodes.installSheet));
    nodes.installClose.addEventListener("click", closeSheets);
    nodes.backdrop.addEventListener("click", closeSheets);

    nodes.saveProfile.addEventListener("click", () => {
      state.profile.onboardingCompleted = true;
      saveProfile();
      closeSheets();
      renderPanels();
    });

    nodes.resetProfile.addEventListener("click", () => {
      state.profile = getDefaultProfile();
      state.activeCategory = "All";
      nodes.searchInput.value = "";
      state.query = "";
      saveProfile();
      renderPreferenceRows();
      renderPanels();
    });
  }

  function registerServiceWorker() {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/service-worker.js").catch(function () {
        return null;
      });
    }
  }

  renderPreferenceRows();
  renderPanels();
  bindStaticEvents();
  bindDynamicEvents();
  registerServiceWorker();
  setActiveTab("for-you");

  if (!state.profile.onboardingCompleted) {
    openSheet(nodes.preferencesSheet);
  }
})();
