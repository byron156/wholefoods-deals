(function () {
  const appDataNode = document.getElementById("app-data");
  if (!appDataNode) {
    return;
  }

  const STORAGE_KEY = "wholefoods-deals-profile-v6";
  const rawData = JSON.parse(appDataNode.textContent || "{}");
  const feedbackEndpoint = rawData.feedback_endpoint || "/fixes-to-deploy";
  const subcategoryOptions = rawData.subcategory_options || {};
  const initialCategoryOrder = rawData.category_order || {};
  const products = (rawData.products || []).map((product, index) => ({
    ...product,
    key: product.asin || (product.asins && product.asins[0]) || `product-${index}`,
    brand: product.brand || "",
    category: product.category || "Pantry",
    retailer: product.retailer || "Whole Foods",
    tags: Array.isArray(product.tags) ? product.tags : [],
    sources: Array.isArray(product.sources) ? product.sources : [],
    available_store_ids: Array.isArray(product.available_store_ids) ? product.available_store_ids : [],
    discount_percent: Number(product.discount_percent || 0),
    source_count: Number(product.source_count || (Array.isArray(product.sources) ? product.sources.length : 0)),
    category_confidence: Number(product.category_confidence || 0),
  }));
  const stores = rawData.stores || [];
  const retailerOrder = ["Whole Foods", "Target", "H Mart"];
  const productByKey = new Map(products.map((product) => [product.key, product]));
  const retailerSet = new Set(products.map((product) => product.retailer).filter(Boolean));
  const retailerList = retailerOrder.filter((retailer) => retailerSet.has(retailer));

  const nodes = {
    searchInput: document.getElementById("global-search"),
    searchMeta: document.getElementById("search-meta"),
    retailerChipRow: document.getElementById("retailer-chip-row"),
    feedGrid: document.getElementById("feed-grid"),
    categorySheetBackdrop: document.getElementById("category-sheet-backdrop"),
    categorySheet: document.getElementById("category-sheet"),
    categorySheetTitle: document.getElementById("category-sheet-title"),
    categorySheetCopy: document.getElementById("category-sheet-copy"),
    categoryScopeRow: document.getElementById("category-scope-row"),
    subcategorySelect: document.getElementById("subcategory-select"),
    brandFixInput: document.getElementById("brand-fix-input"),
    queueSubcategoryFix: document.getElementById("queue-subcategory-fix"),
    queueBrandFix: document.getElementById("queue-brand-fix"),
    categorySheetClose: document.getElementById("category-sheet-close"),
  };
  const categoryList = Array.isArray(rawData.categories) && rawData.categories.length
    ? rawData.categories.slice()
    : Array.from(new Set(products.map((product) => product.category || "Pantry"))).sort((left, right) => left.localeCompare(right));
  const subcategoryEntries = Object.entries(subcategoryOptions).flatMap(([category, subcategories]) =>
    Object.keys(subcategories || {}).map((subcategory) => ({ category, subcategory }))
  );

  function getDefaultProfile() {
    return {
      selectedStoreIds: stores.filter((store) => store.is_active).map((store) => store.id),
      likedKeys: [],
      dislikedKeys: [],
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
    query: "",
    activeRetailer: retailerList[0] || "Whole Foods",
    categoryTargetKey: null,
    categoryScope: "similar",
    categoryOverridesByKey: {},
    categoryOverridesBySignature: {},
    categoryOrderByRetailer: { ...initialCategoryOrder },
  };

  function saveProfile() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.profile));
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
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
      effectiveCategory(product),
      effectiveSubcategory(product),
      product.asin,
      (product.tags || []).join(" "),
      product.retailer,
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

  function buildAffinityCounts(keys) {
    const counts = { categories: {}, brands: {}, tags: {} };
    (keys || []).forEach((key) => {
      const product = productByKey.get(key);
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

  function queryScore(product) {
    if (!state.query) {
      return 0;
    }

    const query = state.query;
    const name = (product.name || "").toLowerCase();
    const brand = (product.brand || "").toLowerCase();
    const category = (product.category || "").toLowerCase();

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
    if (category.includes(query)) {
      return 35;
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
    score += Math.max(0, (product.source_count || 0) - 1) * 8;
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

  function retailerProducts() {
    return products.filter((product) =>
      product.retailer === state.activeRetailer &&
      productVisibleForStores(product)
    );
  }

  function visibleProducts() {
    return retailerProducts().filter((product) => textContainsQuery(product, state.query));
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

  function effectiveSubcategory(product) {
    const itemOverride = (state.categoryOverridesByKey || {})[product.key];
    if (itemOverride) {
      return itemOverride;
    }
    const signature = subcategorySignature(product);
    if (signature) {
      const similarOverride = (state.categoryOverridesBySignature || {})[signature];
      if (similarOverride) {
        return similarOverride;
      }
    }
    return product.subcategory || "";
  }

  function effectiveCategory(product) {
    const subcategory = effectiveSubcategory(product);
    if (subcategory) {
      const entry = subcategoryEntries.find((item) => item.subcategory === subcategory);
      if (entry) {
        return entry.category;
      }
    }
    return product.category || "Pantry";
  }

  function rankProductList(list) {
    const liked = buildAffinityCounts(state.profile.likedKeys);
    const disliked = buildAffinityCounts(state.profile.dislikedKeys);

    return list
      .map((product) => ({
        ...product,
        _score: scoreProduct(product, liked, disliked),
      }))
      .sort((left, right) => {
        if (right._score !== left._score) {
          return right._score - left._score;
        }
        if ((right.discount_percent || 0) !== (left.discount_percent || 0)) {
          return (right.discount_percent || 0) - (left.discount_percent || 0);
        }
        return (left.name || "").localeCompare(right.name || "");
      });
  }

  function buildCategoryShelves() {
    const grouped = new Map();

    retailerProducts().forEach((product) => {
      const category = effectiveCategory(product);
      if (!grouped.has(category)) {
        grouped.set(category, []);
      }
      grouped.get(category).push(product);
    });

    const orderedCategories = state.categoryOrderByRetailer[state.activeRetailer] || [];

    return Array.from(grouped.entries())
      .map(([category, items]) => ({
        category,
        total: items.length,
        items: rankProductList(items).slice(0, 18),
      }))
      .sort((left, right) => {
        const leftIndex = orderedCategories.indexOf(left.category);
        const rightIndex = orderedCategories.indexOf(right.category);
        if (leftIndex !== -1 || rightIndex !== -1) {
          if (leftIndex === -1) {
            return 1;
          }
          if (rightIndex === -1) {
            return -1;
          }
          if (leftIndex !== rightIndex) {
            return leftIndex - rightIndex;
          }
        }
        if (right.total !== left.total) {
          return right.total - left.total;
        }
        return left.category.localeCompare(right.category);
      });
  }

  function renderRetailerChips() {
    nodes.retailerChipRow.innerHTML = retailerList
      .map((retailer) => {
        const selected = state.activeRetailer === retailer;
        return `<button class="chip ${selected ? "is-selected" : ""}" data-retailer="${escapeHtml(retailer)}" type="button">${escapeHtml(retailer)}</button>`;
      })
      .join("");
  }

  function renderEmpty(message) {
    nodes.feedGrid.className = "";
    nodes.feedGrid.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  }

  function priceLabel(product) {
    if (!product.prime_price) {
      return "";
    }
    return `<p class="prime">${escapeHtml(product.prime_price)}</p>`;
  }

  function regularLabel(product) {
    if (!product.basis_price) {
      return "";
    }
    const regularText = String(product.basis_price);
    const normalized = regularText.toLowerCase();
    if (normalized.includes("vary")) {
      return `<p class="deal-regular">${escapeHtml(regularText)}</p>`;
    }
    if (normalized.startsWith("regular")) {
      return `<p class="deal-regular">${escapeHtml(regularText)}</p>`;
    }
    return `<p class="deal-regular">Was ${escapeHtml(regularText)}</p>`;
  }

  function discountLabel(product) {
    if (!product.discount) {
      return "";
    }
    return `<span class="deal-discount">${escapeHtml(product.discount)}</span>`;
  }

  function metaLine(product) {
    const pieces = [];
    if (product.brand) {
      pieces.push(product.brand);
    }
    const subcategory = effectiveSubcategory(product);
    if (subcategory && subcategory !== effectiveCategory(product)) {
      pieces.push(subcategory);
    }
    if (!pieces.length) {
      return "";
    }
    return `<p class="deal-meta-line">${escapeHtml(pieces.join(" · "))}</p>`;
  }

  function renderProductCard(product) {
    const liked = (state.profile.likedKeys || []).includes(product.key);
    const disliked = (state.profile.dislikedKeys || []).includes(product.key);
    const imageMarkup = product.image
      ? `
          <div class="deal-image">
            <img src="${escapeHtml(product.image)}" alt="${escapeHtml(product.name)}">
          </div>
        `
      : "";

    const titleMarkup = product.url
      ? `<h3 class="deal-title"><a href="${escapeHtml(product.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(product.name)}</a></h3>`
      : `<h3 class="deal-title">${escapeHtml(product.name)}</h3>`;

    return `
      <article class="deal-card" data-key="${escapeHtml(product.key)}">
        ${imageMarkup}
        ${metaLine(product)}
        ${titleMarkup}
        <div class="deal-price-row">
          ${priceLabel(product)}
          ${discountLabel(product)}
        </div>
        ${regularLabel(product)}
        <div class="deal-actions">
          <button class="deal-action ${liked ? "is-active" : ""}" data-action="more-like-this" data-key="${escapeHtml(product.key)}" type="button">More</button>
          <button class="deal-action is-subtle ${disliked ? "is-active" : ""}" data-action="less-like-this" data-key="${escapeHtml(product.key)}" type="button">Less</button>
        </div>
        <button class="link-action" data-action="change-category" data-key="${escapeHtml(product.key)}" type="button">This doesn't belong here</button>
      </article>
    `;
  }

  function renderSearchResults() {
    const ranked = rankProductList(visibleProducts()).slice(0, 120);
    if (!ranked.length) {
      renderEmpty("No deals match that search yet.");
      return;
    }

    nodes.feedGrid.className = "product-grid is-search-results";
    nodes.feedGrid.innerHTML = ranked.map(renderProductCard).join("");
    nodes.searchMeta.textContent = `${visibleProducts().length.toLocaleString()} results`;
  }

  function renderShelves() {
    const shelves = buildCategoryShelves();
    if (!shelves.length) {
      renderEmpty("No deals are available right now.");
      return;
    }

    nodes.feedGrid.className = "category-sections";
    nodes.feedGrid.innerHTML = shelves
      .map((shelf) => `
        <section class="category-section">
          <div class="category-section-head">
            <h3>${escapeHtml(shelf.category)}</h3>
            <div class="category-head-actions">
              <button class="link-action" data-action="move-category-up" data-category="${escapeHtml(shelf.category)}" type="button">Up</button>
              <button class="link-action" data-action="move-category-down" data-category="${escapeHtml(shelf.category)}" type="button">Down</button>
            </div>
          </div>
          <div class="category-track">
            ${shelf.items.map(renderProductCard).join("")}
          </div>
        </section>
      `)
      .join("");

    const liveCount = retailerProducts().length;
    nodes.searchMeta.textContent = `${liveCount.toLocaleString()} live deals`;
  }

  function openCategorySheet(product) {
    state.categoryTargetKey = product.key;
    state.categoryScope = subcategorySignature(product) ? "similar" : "item";
    nodes.categorySheetTitle.textContent = "Improve this item";
    nodes.categorySheetCopy.textContent = "Queue a shelf or brand fix to review and deploy later.";
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

  function renderCategorySheet(product) {
    const hasSimilar = Boolean(subcategorySignature(product));
    nodes.categoryScopeRow.innerHTML = [
      `<button class="chip ${state.categoryScope === "item" ? "is-selected" : ""}" data-category-scope="item" type="button">Just this item</button>`,
      hasSimilar
        ? `<button class="chip ${state.categoryScope === "similar" ? "is-selected" : ""}" data-category-scope="similar" type="button">Similar items too</button>`
        : "",
    ].join("");
    nodes.subcategorySelect.innerHTML = subcategoryEntries
      .map((entry) => `<option value="${escapeHtml(entry.subcategory)}"${effectiveSubcategory(product) === entry.subcategory ? " selected" : ""}>${escapeHtml(entry.category)} - ${escapeHtml(entry.subcategory)}</option>`)
      .join("");
    nodes.brandFixInput.value = product.brand || "";
  }

  async function submitFix(payload) {
    const response = await fetch(feedbackEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error(`Fix request failed with status ${response.status}`);
    }
  }

  function applySubcategoryOverride(product, subcategory) {
    if (state.categoryScope === "similar") {
      const signature = subcategorySignature(product);
      if (signature) {
        state.categoryOverridesBySignature = {
          ...(state.categoryOverridesBySignature || {}),
          [signature]: subcategory,
        };
      }
    } else {
      state.categoryOverridesByKey = {
        ...(state.categoryOverridesByKey || {}),
        [product.key]: subcategory,
      };
    }

    closeCategorySheet();
    renderFeed();
    submitFix({
      kind: "subcategory",
      scope: state.categoryScope,
      product_key: product.key,
      signature: subcategorySignature(product),
      subcategory,
    }).catch((error) => {
      console.warn("Could not queue subcategory fix:", error);
    });
  }

  function applyBrandOverride(product, brand) {
    const cleanedBrand = (brand || "").trim();
    if (!cleanedBrand) {
      return;
    }

    if (state.categoryScope === "similar") {
      const signature = brandSignature(product);
      products.forEach((candidate) => {
        if (brandSignature(candidate) === signature) {
          candidate.brand = cleanedBrand;
        }
      });
    } else {
      product.brand = cleanedBrand;
    }

    closeCategorySheet();
    renderFeed();
    submitFix({
      kind: "brand",
      scope: state.categoryScope,
      product_key: product.key,
      signature: brandSignature(product),
      brand: cleanedBrand,
    }).catch((error) => {
      console.warn("Could not queue brand fix:", error);
    });
  }

  function moveCategory(category, direction) {
    const shelves = buildCategoryShelves().map((shelf) => shelf.category);
    const currentOrder = state.categoryOrderByRetailer[state.activeRetailer]
      ? state.categoryOrderByRetailer[state.activeRetailer].filter((item) => shelves.includes(item))
      : [];
    const workingOrder = currentOrder.concat(shelves.filter((item) => !currentOrder.includes(item)));
    const index = workingOrder.indexOf(category);
    if (index === -1) {
      return;
    }
    const swapIndex = direction === "up" ? index - 1 : index + 1;
    if (swapIndex < 0 || swapIndex >= workingOrder.length) {
      return;
    }

    const nextOrder = workingOrder.slice();
    [nextOrder[index], nextOrder[swapIndex]] = [nextOrder[swapIndex], nextOrder[index]];
    state.categoryOrderByRetailer = {
      ...state.categoryOrderByRetailer,
      [state.activeRetailer]: nextOrder,
    };
    renderFeed();
    submitFix({
      kind: "category_order",
      retailer: state.activeRetailer,
      order: nextOrder,
    }).catch((error) => {
      console.warn("Could not queue category order fix:", error);
    });
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

    if (state.query) {
      renderSearchResults();
      return;
    }

    renderShelves();
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

    if (action === "change-category") {
      openCategorySheet(product);
      return;
    }
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

    const scopeButton = event.target.closest("[data-category-scope]");
    if (scopeButton && state.categoryTargetKey) {
      state.categoryScope = scopeButton.dataset.categoryScope;
      renderCategorySheet(productByKey.get(state.categoryTargetKey));
      return;
    }

  });

  nodes.categorySheetBackdrop.addEventListener("click", closeCategorySheet);
  nodes.categorySheetClose.addEventListener("click", closeCategorySheet);
  nodes.queueSubcategoryFix.addEventListener("click", () => {
    const product = productByKey.get(state.categoryTargetKey);
    if (!product) {
      return;
    }
    applySubcategoryOverride(product, nodes.subcategorySelect.value);
  });
  nodes.queueBrandFix.addEventListener("click", () => {
    const product = productByKey.get(state.categoryTargetKey);
    if (!product) {
      return;
    }
    applyBrandOverride(product, nodes.brandFixInput.value);
  });

  nodes.searchInput.addEventListener("input", () => {
    state.query = (nodes.searchInput.value || "").trim().toLowerCase();
    renderFeed();
  });

  renderFeed();
})();
