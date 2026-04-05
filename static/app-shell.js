(function () {
  const appDataNode = document.getElementById("app-data");
  if (!appDataNode) {
    return;
  }

  const STORAGE_KEY = "wholefoods-deals-profile-v2";
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
  const retailerOrder = ["Whole Foods", "Target", "H Mart"];
  const retailerSet = new Set(products.map((product) => product.retailer || "Whole Foods").filter(Boolean));
  const retailerList = retailerOrder.filter((retailer) => retailerSet.has(retailer));

  const nodes = {
    searchInput: document.getElementById("global-search"),
    summaryLine: document.getElementById("summary-line"),
    searchMeta: document.getElementById("search-meta"),
    storeSummary: document.getElementById("store-summary"),
    retailerChipRow: document.getElementById("retailer-chip-row"),
    forYouGrid: document.getElementById("for-you-grid"),
    forYouHighlights: document.getElementById("for-you-highlights"),
    forYouCount: document.getElementById("for-you-count"),
    forYouCopy: document.getElementById("for-you-copy"),
    feedTitle: document.getElementById("feed-title"),
  };

  function getDefaultProfile() {
    return {
      selectedStoreIds: stores.filter((store) => store.is_active).map((store) => store.id),
      likedKeys: [],
      dislikedKeys: [],
      favoriteCategories: [],
      dislikedCategories: [],
      favoriteBrands: [],
      dislikedBrands: [],
      favoriteTags: [],
      dislikedTags: [],
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
      .replace(/\"/g, "&quot;")
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
      product.asin,
      (product.asins || []).join(" "),
      (product.tags || []).join(" "),
      (product.sources || []).join(" "),
      product.retailer || "",
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

  function productVisibleForRetailer(product) {
    if (!state.activeRetailer) {
      return true;
    }
    return (product.retailer || "Whole Foods") === state.activeRetailer;
  }

  function positiveAffinityCounts() {
    const counts = { categories: {}, brands: {}, tags: {} };
    (state.profile.likedKeys || []).forEach((key) => {
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
    const positive = positiveAffinityCounts();

    if ((product.sources || []).length > 1) {
      score += 12;
      reasons.push("Seen in multiple deal feeds");
    }

    if (state.profile.favoriteCategories.includes(product.category)) {
      score += 36;
      reasons.push(`More ${product.category} like you asked for`);
    }
    if (state.profile.dislikedCategories.includes(product.category)) {
      score -= 60;
      reasons.push(`Less ${product.category} because of your downvotes`);
    }

    if (product.brand && state.profile.favoriteBrands.includes(product.brand)) {
      score += 30;
      reasons.push(`More from ${product.brand}`);
    }
    if (product.brand && state.profile.dislikedBrands.includes(product.brand)) {
      score -= 44;
      reasons.push(`Less from ${product.brand}`);
    }

    const positiveTagMatches = (product.tags || []).filter((tag) => state.profile.favoriteTags.includes(tag));
    const negativeTagMatches = (product.tags || []).filter((tag) => state.profile.dislikedTags.includes(tag));
    if (positiveTagMatches.length) {
      score += 16 * positiveTagMatches.length;
      reasons.push(`Matches your ${positiveTagMatches[0]} preferences`);
    }
    if (negativeTagMatches.length) {
      score -= 18 * negativeTagMatches.length;
      reasons.push(`Showing less ${negativeTagMatches[0]} items`);
    }

    if (positive.categories[product.category]) {
      score += positive.categories[product.category] * 8;
      reasons.push(`Close to items you've upvoted before`);
    }
    if (product.brand && positive.brands[product.brand]) {
      score += positive.brands[product.brand] * 10;
      reasons.push(`Brand you've upvoted before`);
    }

    return { score, explanation: reasons[0] || "Strong overall deal value" };
  }

  function visibleProducts() {
    return products.filter((product) =>
      productVisibleForStores(product) &&
      productVisibleForRetailer(product) &&
      textContainsQuery(product, state.query)
    );
  }

  function recommendedProducts() {
    return visibleProducts()
      .map((product) => ({ ...product, _score: scoreProduct(product) }))
      .sort((left, right) => {
        if (right._score.score !== left._score.score) {
          return right._score.score - left._score.score;
        }
        return (right.discount_percent || 0) - (left.discount_percent || 0);
      });
  }

  function formatSources(product) {
    return (product.sources || [])
      .slice(0, 3)
      .map((source) => `<span class="source-pill">${source}</span>`)
      .join("");
  }

  function renderRetailerChips() {
    const counts = {};
    products.forEach((product) => {
      const retailer = product.retailer || "Whole Foods";
      counts[retailer] = (counts[retailer] || 0) + 1;
    });
    const chips = retailerList;
    nodes.retailerChipRow.innerHTML = chips
      .map((retailer) => {
        const selected = state.activeRetailer === retailer;
        const label = `${retailer} (${counts[retailer] || 0})`;
        return `<button class="chip ${selected ? "is-selected" : ""}" data-retailer="${escapeHtml(retailer)}" type="button">${escapeHtml(label)}</button>`;
      })
      .join("");
  }

  function formatTags(product) {
    return (product.tags || [])
      .slice(0, 3)
      .map((tag) => `<span class="chip is-muted">${tag}</span>`)
      .join("");
  }

  function renderEmpty(target, message) {
    target.innerHTML = `<div class="empty-state">${message}</div>`;
  }

  function renderProductCard(product, explanation) {
    const liked = (state.profile.likedKeys || []).includes(product.key);
    const disliked = (state.profile.dislikedKeys || []).includes(product.key);

    return `
      <article class="deal-card" data-key="${product.key}">
        ${
          product.image
            ? `<div class="deal-image"><img src="${product.image}" alt="${escapeHtml(product.name)}"></div>`
            : ""
        }
        <div class="deal-heading-row">
          <div class="deal-brand">${escapeHtml(product.brand || (product.retailer || "Deal"))}</div>
          <span class="category-pill">${escapeHtml(product.category || "Pantry")}</span>
        </div>
        <h3 class="deal-title"><a href="${product.url || "#"}" target="_blank" rel="noopener noreferrer">${escapeHtml(product.name)}</a></h3>
        <div class="retailer-row">
          <span class="retailer-badge">${escapeHtml(product.retailer || "Whole Foods")}</span>
          ${product.subcategory ? `<span class="retailer-category">${escapeHtml(product.subcategory)}</span>` : ""}
        </div>
        ${product.prime_price ? `<p class="prime">${escapeHtml(product.prime_price)}</p>` : ""}
        ${product.basis_price ? `<p class="deal-regular">Regular ${escapeHtml(product.basis_price)}</p>` : ""}
        ${product.discount ? `<p class="deal-discount">${escapeHtml(product.discount)}</p>` : ""}
        ${explanation ? `<div class="deal-explanation">${escapeHtml(explanation)}</div>` : ""}
        <div class="deal-pill-row">${formatSources(product)}${formatTags(product)}</div>
        <div class="deal-actions">
          <button class="deal-action ${liked ? "is-active" : ""}" data-action="more-like-this" data-key="${product.key}" type="button">${liked ? "More like this ✓" : "More like this"}</button>
          <button class="deal-action is-subtle ${disliked ? "is-active" : ""}" data-action="less-like-this" data-key="${product.key}" type="button">${disliked ? "Less like this ✓" : "Less like this"}</button>
          <button class="deal-action" data-action="open" data-key="${product.key}" type="button">View</button>
        </div>
      </article>
    `;
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
      highlights.push(`More ${state.profile.favoriteCategories.slice(0, 2).join(", ")}`);
    }
    if (state.profile.dislikedCategories.length) {
      highlights.push(`Less ${state.profile.dislikedCategories.slice(0, 2).join(", ")}`);
    }
    if (!highlights.length) {
      highlights.push("Use More like this and Less like this to shape the feed.");
    }
    nodes.forYouHighlights.innerHTML = highlights
      .map((text) => `<span class="chip is-selected">${escapeHtml(text)}</span>`)
      .join("");
    nodes.forYouCopy.textContent = recommended.length
      ? "Your feed is ranked by discounts and what you've upvoted."
      : "Start upvoting a few items to personalize the feed.";
  }

  function applyPreferenceSignals(product, direction) {
    const isPositive = direction === "up";
    const currentKey = isPositive ? "likedKeys" : "dislikedKeys";
    const oppositeKey = isPositive ? "dislikedKeys" : "likedKeys";
    const alreadySelected = (state.profile[currentKey] || []).includes(product.key);

    state.profile[currentKey] = (state.profile[currentKey] || []).filter((key) => key !== product.key);
    state.profile[oppositeKey] = (state.profile[oppositeKey] || []).filter((key) => key !== product.key);
    if (!alreadySelected) {
      state.profile[currentKey] = [...state.profile[currentKey], product.key];
    }

    const mapping = isPositive
      ? [
          ["favoriteCategories", product.category],
          ["favoriteBrands", product.brand],
        ]
      : [
          ["dislikedCategories", product.category],
          ["dislikedBrands", product.brand],
        ];

    const oppositeMapping = isPositive
      ? [
          ["dislikedCategories", product.category],
          ["dislikedBrands", product.brand],
        ]
      : [
          ["favoriteCategories", product.category],
          ["favoriteBrands", product.brand],
        ];

    oppositeMapping.forEach(([key, value]) => {
      state.profile[key] = (state.profile[key] || []).filter((entry) => entry !== value);
    });

    if (!alreadySelected) {
      mapping.forEach(([key, value]) => {
        if (value) {
          state.profile[key] = toggleValue(state.profile[key], value);
        }
      });

      (product.tags || []).slice(0, 3).forEach((tag) => {
        const preferredKey = isPositive ? "favoriteTags" : "dislikedTags";
        const oppositePreferredKey = isPositive ? "dislikedTags" : "favoriteTags";
        state.profile[oppositePreferredKey] = (state.profile[oppositePreferredKey] || []).filter((entry) => entry !== tag);
        state.profile[preferredKey] = toggleValue(state.profile[preferredKey], tag);
      });
    }
  }

  function renderPanels() {
    const visible = visibleProducts();
    const recommended = recommendedProducts().slice(0, 120);

    renderHighlights(recommended);
    renderGrid(
      nodes.forYouGrid,
      recommended,
      "No personalized matches yet. Upvote a few items to start shaping the feed.",
      (item) => item._score.explanation
    );

    const selectedStores = stores.filter((store) => state.profile.selectedStoreIds.includes(store.id));
    if (state.activeRetailer === "Whole Foods") {
      nodes.storeSummary.textContent = selectedStores.length
        ? `Store: ${selectedStores.map((store) => store.name).join(", ")}`
        : "Store: All";
    } else {
      nodes.storeSummary.textContent = `Retailer: ${state.activeRetailer}`;
    }
    nodes.searchMeta.textContent = state.query
      ? `Showing ${visible.length} matching products`
      : `Showing ${visible.length} products`;
    if (state.activeRetailer) {
      nodes.searchMeta.textContent += ` in ${state.activeRetailer}`;
    }
    nodes.summaryLine.textContent = "One feed. Three stores.";
    nodes.forYouCount.textContent = `${recommended.length} deals`;
    if (nodes.feedTitle) {
      nodes.feedTitle.textContent = state.query ? `${state.activeRetailer} Results` : `${state.activeRetailer} Deals`;
    }
    nodes.forYouCopy.textContent = state.query
      ? `Results for "${state.query}" in ${state.activeRetailer}.`
      : `Best deals in ${state.activeRetailer}, ranked for value and what you've upvoted.`;

    renderRetailerChips();
  }

  function handleAction(action, key) {
    const product = products.find((item) => item.key === key);
    if (!product) {
      return;
    }

    if (action === "more-like-this") {
      applyPreferenceSignals(product, "up");
      saveProfile();
      renderPanels();
      return;
    }

    if (action === "less-like-this") {
      applyPreferenceSignals(product, "down");
      saveProfile();
      renderPanels();
      return;
    }

    if (action === "open" && product.url) {
      window.open(product.url, "_blank", "noopener,noreferrer");
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
      renderPanels();
    }
  });

  nodes.searchInput.addEventListener("input", () => {
    state.query = (nodes.searchInput.value || "").trim().toLowerCase();
    renderPanels();
  });

  renderPanels();
})();
