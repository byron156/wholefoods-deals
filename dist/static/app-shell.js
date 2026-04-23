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
      filters: {
        ...defaultFilters(),
        ...(source.filters || {}),
      },
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

  function baseDealScore(product) {
    let score = (product.discount_percent || 0) * 4;
    if (product.prime_price) {
      score += 18;
    }
    if (product.basis_price) {
      score += 10;
    }
    if (!product.discount_percent && !product.basis_price) {
      score -= 18;
    }
    score += Math.max(0, (product.source_count || 0) - 1) * 10;
    score += Math.round((product.category_confidence || 0) * 12);
    return score;
  }

  function preferenceScore(product, liked, disliked) {
    let score = 0;
    const category = effectiveCategory(product);
    if ((state.profile.likedKeys || []).includes(product.key)) {
      score += 24;
    }
    if ((state.profile.dislikedKeys || []).includes(product.key)) {
      score -= 90;
    }
    score += (liked.categories[category] || 0) * 16;
    score -= (disliked.categories[category] || 0) * 20;
    if (product.brand) {
      score += (liked.brands[product.brand] || 0) * 16;
      score -= (disliked.brands[product.brand] || 0) * 22;
    }
    (product.tags || []).forEach((tag) => {
      score += (liked.tags[tag] || 0) * 8;
      score -= (disliked.tags[tag] || 0) * 10;
    });
    return score;
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
    return ranked;
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
    return product.discount ? `<span class="deal-discount">${escapeHtml(product.discount)}</span>` : "";
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

  renderFeed();
  loadRemoteProfile();
})();
