(function () {
  const appDataNode = document.getElementById("app-data");
  if (!appDataNode) {
    return;
  }

  const STORAGE_KEY = "wholefoods-deals-profile-v4";
  const rawData = JSON.parse(appDataNode.textContent || "{}");
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
    contextPill: document.getElementById("context-pill"),
    retailerChipRow: document.getElementById("retailer-chip-row"),
    feedGrid: document.getElementById("feed-grid"),
    feedCount: document.getElementById("feed-count"),
    feedTitle: document.getElementById("feed-title"),
  };

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
      product.category,
      product.subcategory,
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

    if ((state.profile.likedKeys || []).includes(product.key)) {
      score += 24;
    }
    if ((state.profile.dislikedKeys || []).includes(product.key)) {
      score -= 90;
    }

    score += (liked.categories[product.category] || 0) * 14;
    score -= (disliked.categories[product.category] || 0) * 20;

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

  function visibleProducts() {
    return products.filter((product) =>
      product.retailer === state.activeRetailer &&
      productVisibleForStores(product) &&
      textContainsQuery(product, state.query)
    );
  }

  function rankedProducts() {
    const liked = buildAffinityCounts(state.profile.likedKeys);
    const disliked = buildAffinityCounts(state.profile.dislikedKeys);

    return visibleProducts()
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

  function renderRetailerChips() {
    nodes.retailerChipRow.innerHTML = retailerList
      .map((retailer) => {
        const selected = state.activeRetailer === retailer;
        return `<button class="chip ${selected ? "is-selected" : ""}" data-retailer="${escapeHtml(retailer)}" type="button">${escapeHtml(retailer)}</button>`;
      })
      .join("");
  }

  function renderEmpty(message) {
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
    if (product.subcategory && product.subcategory !== product.category) {
      pieces.push(product.subcategory);
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
            ${
              product.url
                ? `<a href="${escapeHtml(product.url)}" target="_blank" rel="noopener noreferrer"><img src="${escapeHtml(product.image)}" alt="${escapeHtml(product.name)}"></a>`
                : `<img src="${escapeHtml(product.image)}" alt="${escapeHtml(product.name)}">`
            }
          </div>
        `
      : "";

    const titleMarkup = product.url
      ? `<h3 class="deal-title"><a href="${escapeHtml(product.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(product.name)}</a></h3>`
      : `<h3 class="deal-title">${escapeHtml(product.name)}</h3>`;

    return `
      <article class="deal-card" data-key="${escapeHtml(product.key)}">
        ${imageMarkup}
        <div class="deal-topline">
          <span class="retailer-badge">${escapeHtml(product.retailer)}</span>
          <span class="category-pill">${escapeHtml(product.category)}</span>
        </div>
        ${metaLine(product)}
        ${titleMarkup}
        <div class="deal-price-row">
          ${priceLabel(product)}
          ${discountLabel(product)}
        </div>
        ${regularLabel(product)}
        <div class="deal-actions">
          <button class="deal-action ${liked ? "is-active" : ""}" data-action="more-like-this" data-key="${escapeHtml(product.key)}" type="button">${liked ? "More" : "More"}</button>
          <button class="deal-action is-subtle ${disliked ? "is-active" : ""}" data-action="less-like-this" data-key="${escapeHtml(product.key)}" type="button">${disliked ? "Less" : "Less"}</button>
        </div>
      </article>
    `;
  }

  function applyPreferenceSignals(product, direction) {
    const currentKey = direction === "up" ? "likedKeys" : "dislikedKeys";
    const oppositeKey = direction === "up" ? "dislikedKeys" : "likedKeys";

    state.profile[oppositeKey] = (state.profile[oppositeKey] || []).filter((key) => key !== product.key);
    state.profile[currentKey] = toggleValue(state.profile[currentKey], product.key);
    saveProfile();
  }

  function renderFeed() {
    const ranked = rankedProducts().slice(0, 120);
    if (!ranked.length) {
      renderEmpty(state.query ? "No deals match that search yet." : "No deals are available right now.");
    } else {
      nodes.feedGrid.innerHTML = ranked.map(renderProductCard).join("");
    }

    const visibleCount = visibleProducts().length;
    nodes.searchMeta.textContent = state.query
      ? `${visibleCount.toLocaleString()} results`
      : `${visibleCount.toLocaleString()} live deals`;

    if (state.activeRetailer === "Whole Foods") {
      nodes.contextPill.textContent = "Columbus Circle";
    } else if (state.activeRetailer === "Target") {
      nodes.contextPill.textContent = "Target Grocery";
    } else {
      nodes.contextPill.textContent = "H Mart Sale";
    }

    nodes.feedTitle.textContent = state.query ? `Results in ${state.activeRetailer}` : `Best in ${state.activeRetailer}`;
    nodes.feedCount.textContent = `${ranked.length.toLocaleString()} shown`;
    renderRetailerChips();
  }

  function handleAction(action, key) {
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
    }
  }

  document.body.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-action]");
    if (actionButton) {
      handleAction(actionButton.dataset.action, actionButton.dataset.key);
      return;
    }

    const retailerButton = event.target.closest("[data-retailer]");
    if (retailerButton) {
      state.activeRetailer = retailerButton.dataset.retailer;
      renderFeed();
    }
  });

  nodes.searchInput.addEventListener("input", () => {
    state.query = (nodes.searchInput.value || "").trim().toLowerCase();
    renderFeed();
  });

  renderFeed();
})();
