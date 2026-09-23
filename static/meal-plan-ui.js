(function () {
  'use strict';
  const data = JSON.parse(document.getElementById('planner-data').textContent);
  const form = document.getElementById('planner-form');
  const key = 'grocery-weekly-plan-v1';
  let saved = {};
  try { saved = JSON.parse(localStorage.getItem(key) || '{}') || {}; } catch (_) {}
  if (!saved || typeof saved !== 'object' || Array.isArray(saved)) saved = {};
  let checked = new Set(Array.isArray(saved.checked) ? saved.checked : []);
  let pantry = new Set(Array.isArray(saved.pantry) ? saved.pantry : []);
  let plan;
  let signature = '';
  const escape = text => String(text ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const amount = item => `${Number(item.quantity.toFixed(2))} ${item.unit}`;
  const safeURL = url => {try {const u = new URL(url); return ['https:','http:'].includes(u.protocol) ? u.href : '';}catch (_) {return '';}};
  for (const name of ['servings','retailer','store','diet']) {
    if (saved.options?.[name] !== undefined) form.elements[name].value = saved.options[name];
  }
  // Migrate the previous automatic non-Prime default once; preserve future choices.
  form.elements.prime.checked = OfferPricing.readPrime(saved.version === 2 ? saved.options?.prime !== false : true);
  function options() {
    return {servings:Number(form.elements.servings.value),retailer:form.elements.retailer.value || 'Whole Foods',store:form.elements.store.value,vegetarian:form.elements.diet.value === 'vegetarian',diet:form.elements.diet.value,prime:form.elements.prime.checked,pantry:[...pantry]};
  }
  function persist() {
    try {localStorage.setItem(key,JSON.stringify({version:2,options:plan.options,signature,checked:[...checked],pantry:[...pantry]}));} catch (_) {}
  }
  function renderGroceries() {
    const remaining = plan.groceries.filter(item => !pantry.has(item.id) && !checked.has(item.id)).length;
    document.getElementById('grocery-progress').textContent = `${remaining} of ${plan.groceries.length} ingredients left to shop`;
    let group = '';
    document.getElementById('grocery-list').innerHTML = plan.groceries.map(item => {
      const heading = item.group !== group ? `<h3 class="grocery-group">${escape(item.group)}</h3>` : '';
      group = item.group;
      const deal = item.deal;
      const url = deal && safeURL(deal.url);
      const product = deal ? (url ? `<a href="${escape(url)}" target="_blank" rel="noopener noreferrer">${escape(deal.name)}</a>` : escape(deal.name)) : '';
      const status = pantry.has(item.id) ? 'Already in your pantry' : deal ? `<span class="deal-label">${deal.discount}% off · ${escape(deal.priceText)}${deal.isPrime ? ' · Prime' : ''}</span> · ${escape(deal.retailer)}${deal.store_id ? ' · selected location' : ''}<br>${product}` : 'No matching sale found · buy at regular price';
      return `${heading}<div class="grocery-row ${checked.has(item.id) ? 'checked' : ''}"><input id="check-${item.id}" data-check="${item.id}" type="checkbox" ${checked.has(item.id) ? 'checked' : ''} aria-label="Bought ${escape(item.name)}"><label for="check-${item.id}"><strong class="ingredient-name">${escape(item.name)}</strong> · ${amount(item)}<small>${status}</small></label><button type="button" class="pantry-button" data-pantry="${item.id}" aria-pressed="${pantry.has(item.id)}">${pantry.has(item.id) ? 'In pantry ✓' : 'Already have it'}</button></div>`;
    }).join('');
  }
  function build(resetChecks) {
    if (resetChecks) checked.clear();
    OfferPricing.savePrime(form.elements.prime.checked);
    document.getElementById("planner-results").hidden = false;
    form.elements.store.disabled = !['All','Whole Foods'].includes(form.elements.retailer.value);
    plan = MealPlanner.generate(data.products,options());
    signature = JSON.stringify([plan.days.map(day=>day.meals.map(meal=>meal.id)),plan.options.servings,plan.options.retailer,plan.options.store,plan.options.prime]);
    if (!resetChecks && saved.signature !== signature) checked.clear();
    document.getElementById('planner-status').textContent = `21 meals · ${plan.options.servings} ${plan.options.servings === 1 ? 'person' : 'people'} · ${plan.matchedCount} of ${plan.groceries.length} ingredients matched to recorded sales${plan.matchedCount ? '' : '. No usable sale matches; this is a regular-price fallback menu'}.`;
    document.getElementById('week-grid').innerHTML = plan.days.map(day => `<article class="day-card"><h3>Day ${day.day}</h3>${day.meals.map(meal => `<details><summary><span class="meal-slot">${meal.slot}</span><span class="meal-title">${escape(meal.name)}</span><span class="meal-sale">${meal.saleIngredients.length ? `${meal.saleIngredients.length} ingredients on sale` : 'Regular-price ingredients'}</span></summary><ul class="meal-ingredients">${meal.items.map(item=>`<li>${amount(item)} ${escape(item.name)}</li>`).join('')}</ul><p class="meal-method">${escape(meal.method)}</p></details>`).join('')}</article>`).join('');
    renderGroceries();persist();
  }
  form.addEventListener('submit', event => {event.preventDefault();build(true);});
  form.elements.retailer.addEventListener('change', () => {form.elements.store.disabled = !['All','Whole Foods'].includes(form.elements.retailer.value);});
  document.getElementById('grocery-list').addEventListener('change', event => {
    const id = event.target.dataset.check;
    if (!id) return;
    event.target.checked ? checked.add(id) : checked.delete(id);
    renderGroceries();persist();
    document.getElementById(`check-${id}`).focus();
  });
  document.getElementById('grocery-list').addEventListener('click', event => {
    const button = event.target.closest('[data-pantry]');
    if (!button) return;
    const id = button.dataset.pantry;
    pantry.has(id) ? pantry.delete(id) : pantry.add(id);
    renderGroceries();persist();
    document.querySelector(`[data-pantry="${id}"]`).focus();
  });
  document.getElementById('print-plan').addEventListener('click', () => window.print());
  let openBeforePrint = [];
  window.addEventListener('beforeprint', () => {openBeforePrint = [...document.querySelectorAll('details')].map(el=>el.open);document.querySelectorAll('details').forEach(el=>el.open=true);});
  window.addEventListener('afterprint', () => document.querySelectorAll('details').forEach((el,index)=>el.open=openBeforePrint[index]));
  document.getElementById('download-plan').addEventListener('click', () => {
    const lines = [`WEEKLY MEAL PLAN — ${plan.options.servings} people`,data.catalog_note,'Prices are recorded offers; confirm availability. Quantities are recipe needs, not package counts.',''];
    for (const day of plan.days) {
      lines.push(`DAY ${day.day}`);
      for (const meal of day.meals) lines.push(`${meal.slot.toUpperCase()}: ${meal.name}`,meal.items.map(i=>`${amount(i)} ${i.name}`).join('; '),meal.method,'');
    }
    lines.push('GROCERY LIST');
    for (const item of plan.groceries) lines.push(`${checked.has(item.id) ? '[x]' : '[ ]'} ${item.name}: ${amount(item)}${pantry.has(item.id) ? ' — ALREADY IN PANTRY' : item.deal ? ` — ${item.deal.retailer}: ${item.deal.name}, ${item.deal.priceText}${item.deal.isPrime ? ' Prime' : ''} per listed offer (${item.deal.discount}% off)` : ' — no matching sale; regular price'}`);
    const url = URL.createObjectURL(new Blob([lines.join('\n')],{type:'text/plain;charset=utf-8'}));
    const a = document.createElement('a');a.href=url;a.download='weekly-meal-plan.txt';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
  });
  form.addEventListener('change', () => {
    if (plan) {
      document.getElementById('planner-results').hidden = true;
      document.getElementById('planner-status').textContent = 'Preferences changed. Click Build my week to update your menu and grocery list.';
    }
  });
  form.elements.store.disabled = !['All','Whole Foods'].includes(form.elements.retailer.value);
})();
