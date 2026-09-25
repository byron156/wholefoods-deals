# Price accuracy incident, September 21–22, 2026

The reported mismatches were reproducible. They were not simply a Prime rounding error.

- Flyer hydration copied promotion-wide prices and percent/range offers onto every linked ASIN. It linked those synthetic prices to pickup/delivery product pages even though their offer context differed.
- Store offers were merged field by field, skipping empty values. A newer empty Prime or regular price could leave an older discount intact. The primary-store projection also skipped missing fields.
- The home page filtered products by store but kept displaying the primary store's price. It rendered only `prime_price`, leaving percent-only offers with no price.
- Price normalization treated `current_price` as a Prime fallback, incorrectly labelling ordinary retailer prices as Prime prices.
- Search refreshes failed because the retailer's location sheet now defaults to Delivery. Pickup uses an expandable store card in `iframe[name="store-web-page"]`. Reused data had no observation timestamps; the page called the newest file modification time “Last updated.”
- Category rules used substring matches (`ham` matched `chamomile`, `egg` matched `veggie`), and a best-effort classifier accepted predictions below 5% confidence. Matching now requires word boundaries; explicit product forms and retailer categories take precedence; predictions below 55% are withheld for review. The classifier cache version was advanced so old decisions are rebuilt.
- The audit counted missing discounts and unbranded produce as errors, and flagged ordinary olive oil as personal care. These heuristics did not measure whether a price was safe to publish.

## Verified observations

On September 21, on the retailer website, explicitly selecting **Pickup at Columbus Circle** showed zucchini at **$2.77/lb**, **$3.29/lb** crossed out, and a **$2.49 Prime** benefit. Corn (B07VLMMG6R) showed **$4.99 with no sale**. Cantaloupe Chunks Small (B09V3LKPCH) showed **currently not sold in Columbus Circle / out of stock**. The supplied screenshots used delivery to ZIP 10001, a different context.

A focused browser refresh captured 28 available product records from the current Columbus Circle deals page. These are timestamped product-price observations, not a complete store crawl. Weekly flyer and H Mart HTTP sources were refreshed independently. Older unverified records remain in the audit; they do not become fresh because a file was rewritten.

On September 22, the scheduled refresh collected 2,336 fresh Columbus Circle records. Upper West Side failed and retained its original undated records, which are withheld. Cantaloupe was now in stock at Columbus Circle at $5.42/lb, $6.09/lb regular, $4.88/lb Prime. Availability and prices changed between observations.

The API importer also dropped `variableUnitOfMeasure.pricingUom`, losing per-pound labels. It now retains that selling unit separately from the comparison unit price. A metadata-only backfill checked 364 records with missing selling units, including fresh produce, meat and dairy. It repaired 37 weighted offers and withheld 75 whose units could not be verified; it did not import prices from another location. Cantaloupe was corrected from its directly observed product page.

## Enforcement

- Observations keep collection time, price source, channel, store and validity dates. Missing provenance, unknown expiry syntax, unavailable items, and observations older than 72 hours are withheld.
- An offer is replaced atomically. Missing current fields cannot resurrect older sale values.
- Weekly flyer promotions retain their advertised scope and link to the promotion. They are excluded from ingredient matching and product-price digests. Percentage/range text is never converted into a guessed individual price.
- Browsing and meal planning share the same price/store selector. Digests honor their saved Prime preference, defaulting to Prime. Savings are recalculated from the selected prices.
- A page load never generates a meal plan. Submission builds it; preference changes hide obsolete results until the user rebuilds.
- The audit reports publishable and withheld records separately. `Latest source check` is an actual observation time, not a build timestamp.

The published catalog is deliberately smaller until additional product offers are collected with verified context. No claim is made that every retained record or every retailer's full catalog has been repaired. Unit, merge, stale-data, pricing-mode and meal-selection regression tests cover the reported failure paths.

### Complete weekly-promotion coverage

`scripts/collect_sale_coverage.py` reads each supported store's flyer, then pages
`/api/wwos/sales-flyer/grouped-promotion` through its reported total. Flyer and
promotion-page previews are not complete eligibility lists. Source row totals
include duplicates, so the report keeps source rows and unique ASINs separate.
A changed total, repeated page, truncated response, or wrong store fails collection.
The previous collection is replaced only after both stores finish successfully.

Eligible products carry the retailer's promotion terms and store context. Their
online prices are refreshed independently by `scripts/recover_catalog.py`.
An absent online price does not cancel an advertised flyer promotion, and a
flyer percentage never becomes a guessed dollar price. If identity metadata is
absent from the retailer API, the ASIN remains in the parent promotion's full
eligibility list; the metadata gap remains explicit in the coverage report.

`reports/sale_coverage.json` reconciles the advertised identities against the
shopper catalog. Static builds fail if any advertised promotion or membership
is lost. The quality audit shows this scoped coverage separately from its
identity-deduplicated sale-product count. This proves coverage of the two-store
weekly flyer, not completeness of online-only sales or other retailers.

Online search now includes the valid offset=500 boundary page. A page containing
only products seen in earlier sorts no longer stops collection; later pages can
contain new identities. Each sort reports its own listed ASINs and reported total.
HTTP errors and server result-window limits must not be called complete listings.
Price recovery preserves these listing reports instead of overwriting them.
