import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from uuid import uuid4

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
    except Exception as error:
        print(f"Supabase load_fixes_from_supabase failed: {error}")
        return None

    return fixes


def save_fix_to_supabase(*, fix_id, fix_type, scope=None, product_key=None, signature=None, retailer=None, value=None, status="active"):
    client = get_supabase_client()
    if client is None:
        return False

    try:
        client.table("taxonomy_fixes").upsert(
            {
                "id": fix_id,
                "fix_type": fix_type,
                "scope": scope,
                "product_key": product_key,
                "signature": signature,
                "retailer": retailer,
                "value": value,
                "status": status,
            },
            on_conflict="id",
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_fix_to_supabase failed: {error}")
        return False


def load_device_profile_from_supabase(device_id):
    client = get_supabase_client()
    if client is None:
        return None

    try:
        rows = (
            client.table("device_profiles")
            .select("id, device_id, comparison_store_ids, onboarding_answers, app_settings")
            .eq("device_id", device_id)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception as error:
        print(f"Supabase load_device_profile_from_supabase failed: {error}")
        return None

    if not rows:
        return None

    row = rows[0]
    settings = row.get("app_settings") or {}
    onboarding_answers = row.get("onboarding_answers") or {}
    return {
        "deviceProfileId": row.get("id"),
        "selectedStoreIds": settings.get("selectedStoreIds") or row.get("comparison_store_ids") or [],
        "likedKeys": settings.get("likedKeys") or [],
        "dislikedKeys": settings.get("dislikedKeys") or [],
        "savedKeys": settings.get("savedKeys") or [],
        "categoryOrderByRetailer": settings.get("categoryOrderByRetailer") or {},
        "newsletterEnabled": bool(settings.get("newsletterEnabled")),
        "newsletterCadence": settings.get("newsletterCadence") or "daily",
        "newsletterOnboardingCompleted": bool(settings.get("newsletterOnboardingCompleted")),
        "newsletterPreferences": settings.get("newsletterPreferences") or {},
        "newsletterEmail": settings.get("newsletterEmail") or "",
        "onboardingAnswers": onboarding_answers if isinstance(onboarding_answers, dict) else {},
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
                "onboarding_answers": profile.get("onboardingAnswers") or {},
                "app_settings": {
                    "selectedStoreIds": profile.get("selectedStoreIds") or [],
                    "likedKeys": profile.get("likedKeys") or [],
                    "dislikedKeys": profile.get("dislikedKeys") or [],
                    "savedKeys": profile.get("savedKeys") or [],
                    "categoryOrderByRetailer": profile.get("categoryOrderByRetailer") or {},
                    "newsletterEnabled": bool(profile.get("newsletterEnabled")),
                    "newsletterCadence": profile.get("newsletterCadence") or "daily",
                    "newsletterOnboardingCompleted": bool(profile.get("newsletterOnboardingCompleted")),
                    "newsletterPreferences": profile.get("newsletterPreferences") or {},
                    "newsletterEmail": profile.get("newsletterEmail") or "",
                },
            },
            on_conflict="device_id",
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_device_profile_to_supabase failed: {error}")
        return False


def _device_profile_row(device_id):
    client = get_supabase_client()
    if client is None:
        return None


def _device_id_for_profile_id(device_profile_id):
    client = get_supabase_client()
    if client is None or not device_profile_id:
        return None
    try:
        rows = (
            client.table("device_profiles")
            .select("device_id")
            .eq("id", device_profile_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0].get("device_id") if rows else None
    except Exception as error:
        print(f"Supabase _device_id_for_profile_id failed: {error}")
        return None
    try:
        rows = (
            client.table("device_profiles")
            .select("id, device_id, comparison_store_ids, onboarding_answers, app_settings")
            .eq("device_id", device_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception as error:
        print(f"Supabase _device_profile_row failed: {error}")
        return None


def ensure_device_profile_id(device_id):
    profile = load_device_profile_from_supabase(device_id)
    if profile and profile.get("deviceProfileId"):
        return profile.get("deviceProfileId")
    if not save_device_profile_to_supabase(device_id, {}):
        return None
    profile = load_device_profile_from_supabase(device_id)
    return (profile or {}).get("deviceProfileId")


def upsert_newsletter_subscriber_to_supabase(*, device_id, email, cadence="daily", timezone="America/New_York", status="active"):
    client = get_supabase_client()
    if client is None:
        return None
    device_profile_id = ensure_device_profile_id(device_id)
    if not device_profile_id:
        return None

    payload = {
        "email": (email or "").strip().lower(),
        "device_profile_id": device_profile_id,
        "status": status,
        "cadence": cadence or "daily",
        "timezone": timezone or "America/New_York",
    }
    if status == "active":
        payload["unsubscribed_at"] = None

    try:
        client.table("newsletter_subscribers").upsert(
            payload,
            on_conflict="email",
        ).execute()
        rows = (
            client.table("newsletter_subscribers")
            .select("*")
            .eq("email", payload["email"])
            .limit(1)
            .execute()
            .data
            or []
        )
        if not rows:
            return None
        row = rows[0]
        row["device_id"] = _device_id_for_profile_id(row.get("device_profile_id"))
        return row
    except Exception as error:
        print(f"Supabase upsert_newsletter_subscriber_to_supabase failed: {error}")
        return None


def load_newsletter_subscriber_from_supabase(device_id):
    client = get_supabase_client()
    if client is None:
        return None
    device_profile_id = ensure_device_profile_id(device_id)
    if not device_profile_id:
        return None
    try:
        rows = (
            client.table("newsletter_subscribers")
            .select("*")
            .eq("device_profile_id", device_profile_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not rows:
            return None
        row = rows[0]
        row["device_id"] = device_id
        return row
    except Exception as error:
        print(f"Supabase load_newsletter_subscriber_from_supabase failed: {error}")
        return None


def save_newsletter_preferences_to_supabase(*, device_id, preferences):
    client = get_supabase_client()
    if client is None:
        return False
    subscriber = load_newsletter_subscriber_from_supabase(device_id)
    if not subscriber:
        return False
    try:
        client.table("newsletter_preferences").upsert(
            {
                "subscriber_id": subscriber.get("id"),
                "preferred_categories": preferences.get("preferredCategories") or [],
                "disliked_categories": preferences.get("dislikedCategories") or [],
                "favorite_brands": preferences.get("favoriteBrands") or [],
                "hidden_brands": preferences.get("hiddenBrands") or [],
                "cadence_settings": preferences.get("cadenceSettings") or {},
                "onboarding_answers": preferences.get("onboardingAnswers") or {},
                "sampled_product_feedback": preferences.get("sampledProductFeedback") or {},
            },
            on_conflict="subscriber_id",
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_newsletter_preferences_to_supabase failed: {error}")
        return False


def load_newsletter_preferences_from_supabase(device_id):
    client = get_supabase_client()
    if client is None:
        return None
    subscriber = load_newsletter_subscriber_from_supabase(device_id)
    if not subscriber:
        return None
    try:
        rows = (
            client.table("newsletter_preferences")
            .select("*")
            .eq("subscriber_id", subscriber.get("id"))
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception as error:
        print(f"Supabase load_newsletter_preferences_from_supabase failed: {error}")
        return None


def list_active_newsletter_subscribers_from_supabase():
    client = get_supabase_client()
    if client is None:
        return []
    try:
        subscribers = (
            client.table("newsletter_subscribers")
            .select("*")
            .eq("status", "active")
            .execute()
            .data
            or []
        )
        if not subscribers:
            return []

        preferences = (
            client.table("newsletter_preferences")
            .select("*")
            .execute()
            .data
            or []
        )
        preferences_by_subscriber = {
            row.get("subscriber_id"): row
            for row in preferences
            if row.get("subscriber_id")
        }
        return [
            {
                **subscriber,
                "device_id": _device_id_for_profile_id(subscriber.get("device_profile_id")),
                "preferences": preferences_by_subscriber.get(subscriber.get("id")) or {},
            }
            for subscriber in subscribers
        ]
    except Exception as error:
        print(f"Supabase list_active_newsletter_subscribers_from_supabase failed: {error}")
        return []


def save_newsletter_feedback_token_to_supabase(*, token, subscriber_id, product_key, action, expires_at, metadata=None):
    client = get_supabase_client()
    if client is None:
        return False
    try:
        client.table("newsletter_feedback_tokens").upsert(
            {
                "token": token,
                "subscriber_id": subscriber_id,
                "product_key": product_key,
                "action": action,
                "expires_at": expires_at,
                "metadata": metadata or {},
            },
            on_conflict="token",
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_newsletter_feedback_token_to_supabase failed: {error}")
        return False


def load_newsletter_feedback_token_from_supabase(token):
    client = get_supabase_client()
    if client is None:
        return None
    try:
        rows = (
            client.table("newsletter_feedback_tokens")
            .select("*")
            .eq("token", token)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception as error:
        print(f"Supabase load_newsletter_feedback_token_from_supabase failed: {error}")
        return None


def mark_newsletter_feedback_token_used_in_supabase(token):
    client = get_supabase_client()
    if client is None:
        return False
    try:
        client.table("newsletter_feedback_tokens").update(
            {
                "used_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("token", token).execute()
        return True
    except Exception as error:
        print(f"Supabase mark_newsletter_feedback_token_used_in_supabase failed: {error}")
        return False


def save_newsletter_delivery_to_supabase(*, subscriber_id, digest_date, status, payload_metadata=None):
    client = get_supabase_client()
    if client is None:
        return False
    try:
        client.table("newsletter_deliveries").insert(
            {
                "subscriber_id": subscriber_id,
                "digest_date": digest_date,
                "status": status,
                "payload_metadata": payload_metadata or {},
            }
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_newsletter_delivery_to_supabase failed: {error}")
        return False


def load_latest_newsletter_delivery_from_supabase(*, subscriber_id):
    client = get_supabase_client()
    if client is None:
        return None
    try:
        rows = (
            client.table("newsletter_deliveries")
            .select("*")
            .eq("subscriber_id", subscriber_id)
            .order("sent_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception as error:
        print(f"Supabase load_latest_newsletter_delivery_from_supabase failed: {error}")
        return None


def save_newsletter_event_to_supabase(*, subscriber_id, event_type, product_key=None, metadata=None):
    client = get_supabase_client()
    if client is None:
        return False
    try:
        client.table("newsletter_events").insert(
            {
                "id": str(uuid4()),
                "subscriber_id": subscriber_id,
                "event_type": event_type,
                "product_key": product_key,
                "metadata": metadata or {},
            }
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_newsletter_event_to_supabase failed: {error}")
        return False


def save_recommendation_snapshot_to_supabase(*, device_id, store_scope, recommendations, metadata=None):
    client = get_supabase_client()
    if client is None:
        return False
    device_profile_id = ensure_device_profile_id(device_id)
    if not device_profile_id:
        return False
    try:
        client.table("recommendation_snapshots").insert(
            {
                "device_profile_id": device_profile_id,
                "store_scope": store_scope or [],
                "recommendations": recommendations or [],
                "metadata": metadata or {},
            }
        ).execute()
        return True
    except Exception as error:
        print(f"Supabase save_recommendation_snapshot_to_supabase failed: {error}")
        return False
