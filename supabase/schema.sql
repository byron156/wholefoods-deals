create extension if not exists pgcrypto;

create table if not exists public.stores (
    id text primary key,
    slug text unique not null,
    name text not null,
    city text,
    state text,
    latitude double precision,
    longitude double precision,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.products (
    id uuid primary key default gen_random_uuid(),
    product_key text unique not null,
    primary_asin text,
    canonical_name text not null,
    brand text,
    variation text,
    image_url text,
    canonical_url text,
    category text,
    tags jsonb not null default '[]'::jsonb,
    diets jsonb not null default '[]'::jsonb,
    source_coverage jsonb not null default '[]'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.product_offers (
    id uuid primary key default gen_random_uuid(),
    product_id uuid not null references public.products(id) on delete cascade,
    store_id text references public.stores(id) on delete set null,
    source_type text not null check (source_type in ('flyer', 'all_deals', 'search_deals', 'target_deals', 'hmart_deals')),
    regular_price text,
    sale_price text,
    prime_price text,
    unit_price text,
    discount_text text,
    normalized_discount_percent integer not null default 0,
    availability text,
    scraped_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists product_offers_product_id_idx on public.product_offers(product_id);
create index if not exists product_offers_store_id_idx on public.product_offers(store_id);
create index if not exists product_offers_source_type_idx on public.product_offers(source_type);

create table if not exists public.scrape_runs (
    id uuid primary key default gen_random_uuid(),
    source_type text not null,
    store_scope jsonb not null default '[]'::jsonb,
    status text not null default 'started',
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    product_count integer not null default 0,
    diagnostics jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.user_profiles (
    id uuid primary key default gen_random_uuid(),
    auth_user_id uuid unique,
    display_name text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.device_profiles (
    id uuid primary key default gen_random_uuid(),
    device_id text unique not null,
    primary_store_id text references public.stores(id) on delete set null,
    comparison_store_ids jsonb not null default '[]'::jsonb,
    onboarding_answers jsonb not null default '{}'::jsonb,
    app_settings jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.user_preferences (
    id uuid primary key default gen_random_uuid(),
    user_profile_id uuid references public.user_profiles(id) on delete cascade,
    device_profile_id uuid references public.device_profiles(id) on delete cascade,
    preferred_categories jsonb not null default '[]'::jsonb,
    preferred_diets jsonb not null default '[]'::jsonb,
    favorite_brands jsonb not null default '[]'::jsonb,
    hidden_brands jsonb not null default '[]'::jsonb,
    disliked_categories jsonb not null default '[]'::jsonb,
    budget_sensitivity text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint user_preferences_owner_check check (
        user_profile_id is not null or device_profile_id is not null
    )
);

create table if not exists public.user_feedback_events (
    id uuid primary key default gen_random_uuid(),
    user_profile_id uuid references public.user_profiles(id) on delete cascade,
    device_profile_id uuid references public.device_profiles(id) on delete cascade,
    product_id uuid not null references public.products(id) on delete cascade,
    event_type text not null check (event_type in ('save', 'hide', 'open', 'favorite_brand', 'favorite_category', 'dismiss', 'add_to_list')),
    event_value text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint user_feedback_owner_check check (
        user_profile_id is not null or device_profile_id is not null
    )
);

create index if not exists user_feedback_events_product_id_idx on public.user_feedback_events(product_id);
create index if not exists user_feedback_events_event_type_idx on public.user_feedback_events(event_type);

create table if not exists public.saved_items (
    id uuid primary key default gen_random_uuid(),
    user_profile_id uuid references public.user_profiles(id) on delete cascade,
    device_profile_id uuid references public.device_profiles(id) on delete cascade,
    product_id uuid not null references public.products(id) on delete cascade,
    notes text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint saved_items_owner_check check (
        user_profile_id is not null or device_profile_id is not null
    )
);

create unique index if not exists saved_items_user_unique_idx
    on public.saved_items(user_profile_id, device_profile_id, product_id);

create table if not exists public.recommendation_snapshots (
    id uuid primary key default gen_random_uuid(),
    user_profile_id uuid references public.user_profiles(id) on delete cascade,
    device_profile_id uuid references public.device_profiles(id) on delete cascade,
    store_scope jsonb not null default '[]'::jsonb,
    generated_at timestamptz not null default now(),
    recommendations jsonb not null default '[]'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    constraint recommendation_snapshots_owner_check check (
        user_profile_id is not null or device_profile_id is not null
    )
);

create table if not exists public.newsletter_subscribers (
    id uuid primary key default gen_random_uuid(),
    email text unique not null,
    device_profile_id uuid not null references public.device_profiles(id) on delete cascade,
    status text not null default 'active' check (status in ('active', 'paused', 'unsubscribed')),
    cadence text not null default 'daily' check (cadence in ('daily', 'few-times-week', 'weekly')),
    timezone text not null default 'America/New_York',
    onboarded_at timestamptz,
    unsubscribed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists newsletter_subscribers_device_profile_idx on public.newsletter_subscribers(device_profile_id);

create table if not exists public.newsletter_preferences (
    id uuid primary key default gen_random_uuid(),
    subscriber_id uuid unique not null references public.newsletter_subscribers(id) on delete cascade,
    preferred_categories jsonb not null default '[]'::jsonb,
    disliked_categories jsonb not null default '[]'::jsonb,
    favorite_brands jsonb not null default '[]'::jsonb,
    hidden_brands jsonb not null default '[]'::jsonb,
    cadence_settings jsonb not null default '{}'::jsonb,
    onboarding_answers jsonb not null default '{}'::jsonb,
    sampled_product_feedback jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.newsletter_deliveries (
    id uuid primary key default gen_random_uuid(),
    subscriber_id uuid not null references public.newsletter_subscribers(id) on delete cascade,
    digest_date date not null,
    sent_at timestamptz not null default now(),
    status text not null default 'queued' check (status in ('queued', 'sent', 'skipped', 'failed')),
    payload_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists newsletter_deliveries_subscriber_idx on public.newsletter_deliveries(subscriber_id);
create index if not exists newsletter_deliveries_digest_date_idx on public.newsletter_deliveries(digest_date);

create table if not exists public.newsletter_feedback_tokens (
    token text primary key,
    subscriber_id uuid not null references public.newsletter_subscribers(id) on delete cascade,
    product_key text not null,
    action text not null check (action in ('thumbs_up', 'thumbs_down', 'save')),
    expires_at timestamptz not null,
    used_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists newsletter_feedback_tokens_subscriber_idx on public.newsletter_feedback_tokens(subscriber_id);
create index if not exists newsletter_feedback_tokens_product_idx on public.newsletter_feedback_tokens(product_key);

create table if not exists public.newsletter_events (
    id uuid primary key default gen_random_uuid(),
    subscriber_id uuid not null references public.newsletter_subscribers(id) on delete cascade,
    event_type text not null check (event_type in ('signup', 'onboarding_completed', 'email_sent', 'email_failed', 'thumbs_up', 'thumbs_down', 'save', 'open', 'click')),
    product_key text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.taxonomy_fixes (
    id text primary key,
    fix_type text not null check (fix_type in ('subcategory', 'brand')),
    scope text not null check (scope in ('item', 'similar')),
    product_key text,
    signature text,
    retailer text,
    value text not null,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists taxonomy_fixes_fix_type_idx on public.taxonomy_fixes(fix_type);
create index if not exists taxonomy_fixes_signature_idx on public.taxonomy_fixes(signature);
create index if not exists taxonomy_fixes_product_key_idx on public.taxonomy_fixes(product_key);

create table if not exists public.retailer_category_orders (
    retailer text primary key,
    ordered_categories jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
