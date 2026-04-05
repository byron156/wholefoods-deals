import json
import os
from functools import lru_cache

try:
    from supabase import Client, create_client
except ImportError:  # pragma: no cover
    Client = None
    create_client = None


@lru_cache(maxsize=1)
def get_supabase_client():
    if create_client is None:
        return None

    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        return None

    try:
        return create_client(url, key)
    except Exception:
        return None


def supabase_enabled():
    return get_supabase_client() is not None


def load_fixes_from_supabase():
    client = get_supabase_client()
    if client is None:
        return None

    fixes = {
        "subcategory_overrides_by_key": {},
        "subcategory_overrides_by_signature": {},
        "brand_overrides_by_key": {},
        "brand_overrides_by_signature": {},
        "category_order": {},
    }

    try:
        taxonomy_rows = (
            client.table("taxonomy_fixes")
            .select("*")
            .eq("status", "active")
            .execute()
            .data
            or []
        )
        for row in taxonomy_rows:
            fix_type = row.get("fix_type")
            scope = row.get("scope")
            value = row.get("value")
            product_key = row.get("product_key")
            signature = row.get("signature")
            if fix_type == "subcategory":
                if scope == "item" and product_key and value:
                    fixes["subcategory_overrides_by_key"][product_key] = value
                elif scope == "similar" and signature and value:
                    fixes["subcategory_overrides_by_signature"][signature] = value
            elif fix_type == "brand":
                if scope == "item" and product_key and value:
                    fixes["brand_overrides_by_key"][product_key] = value
                elif scope == "similar" and signature and value:
                    fixes["brand_overrides_by_signature"][signature] = value

        order_rows = client.table("retailer_category_orders").select("*").execute().data or []
        for row in order_rows:
            retailer = row.get("retailer")
            ordered_categories = row.get("ordered_categories")
            if retailer and isinstance(ordered_categories, list):
                fixes["category_order"][retailer] = ordered_categories
    except Exception:
        return None

    return fixes


def save_fix_to_supabase(*, fix_id, fix_type, scope=None, product_key=None, signature=None, retailer=None, value=None):
    client = get_supabase_client()
    if client is None:
        return False

    try:
        if fix_type == "category_order":
            client.table("retailer_category_orders").upsert(
                {
                    "retailer": retailer,
                    "ordered_categories": value or [],
                }
            ).execute()
            return True

        client.table("taxonomy_fixes").upsert(
            {
                "id": fix_id,
                "fix_type": fix_type,
                "scope": scope,
                "product_key": product_key,
                "signature": signature,
                "retailer": retailer,
                "value": value,
                "status": "active",
            }
        ).execute()
        return True
    except Exception:
        return False


def load_device_profile_from_supabase(device_id):
    client = get_supabase_client()
    if client is None:
        return None

    try:
        rows = (
            client.table("device_profiles")
            .select("device_id, comparison_store_ids, app_settings")
            .eq("device_id", device_id)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return None

    if not rows:
        return None

    row = rows[0]
    settings = row.get("app_settings") or {}
    return {
        "selectedStoreIds": settings.get("selectedStoreIds") or row.get("comparison_store_ids") or [],
        "likedKeys": settings.get("likedKeys") or [],
        "dislikedKeys": settings.get("dislikedKeys") or [],
    }


def save_device_profile_to_supabase(device_id, profile):
    client = get_supabase_client()
    if client is None:
        return False

    try:
        client.table("device_profiles").upsert(
            {
                "device_id": device_id,
                "comparison_store_ids": profile.get("selectedStoreIds") or [],
                "app_settings": {
                    "selectedStoreIds": profile.get("selectedStoreIds") or [],
                    "likedKeys": profile.get("likedKeys") or [],
                    "dislikedKeys": profile.get("dislikedKeys") or [],
                },
            }
        ).execute()
        return True
    except Exception:
        return False
