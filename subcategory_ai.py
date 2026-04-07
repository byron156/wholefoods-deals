import json
import os
import pickle
import re
from collections import Counter

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
except ImportError:  # pragma: no cover
    TfidfVectorizer = None
    LogisticRegression = None
    Pipeline = None


MODEL_VERSION = "subcategory-ai-v1"


def sklearn_available():
    return all([TfidfVectorizer, LogisticRegression, Pipeline])


def normalize_text(text):
    if not text:
        return ""

    text = str(text).lower().strip()
    text = text.replace("’", "'")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def flatten_text_list(value):
    if not value:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(item) for item in value if item)
    return str(value)


def build_feature_text(product):
    parts = [
        f"retailer {normalize_text(product.get('retailer'))}",
        f"brand {normalize_text(product.get('brand'))}",
        f"name {normalize_text(product.get('name'))}",
        f"raw {normalize_text(product.get('raw_name'))}",
        f"heuristic_category {normalize_text(product.get('category'))}",
        f"heuristic_subcategory {normalize_text(product.get('subcategory'))}",
        f"variation {normalize_text(product.get('variation'))}",
        f"sources {normalize_text(flatten_text_list(product.get('sources')))}",
        f"source_categories {normalize_text(flatten_text_list(product.get('source_categories')))}",
        f"url {normalize_text(product.get('url'))}",
        f"retail_source_url {normalize_text(product.get('retail_source_url'))}",
        f"tags {normalize_text(flatten_text_list(product.get('tags')))}",
    ]
    return " ".join(part for part in parts if part.strip())


def train_subcategory_model(products, valid_subcategories):
    labels = []
    features = []

    for product in products:
        label = product.get("subcategory")
        if label not in valid_subcategories:
            continue
        feature_text = build_feature_text(product)
        if not feature_text:
            continue
        labels.append(label)
        features.append(feature_text)

    label_counts = Counter(labels)
    metadata = {
        "model_version": MODEL_VERSION,
        "training_examples": len(labels),
        "label_counts": dict(sorted(label_counts.items())),
    }

    if not sklearn_available() or len(label_counts) < 2:
        return None, metadata

    pipeline = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=(1, 2),
                    min_df=1,
                    max_features=40000,
                    sublinear_tf=True,
                ),
            ),
            (
                "clf",
                LogisticRegression(
                    max_iter=2500,
                    class_weight="balanced",
                ),
            ),
        ]
    )
    pipeline.fit(features, labels)
    return pipeline, metadata


def save_model_artifacts(model, metadata, model_path, metadata_path):
    if model is not None:
        with open(model_path, "wb") as model_file:
            pickle.dump(model, model_file)
    with open(metadata_path, "w", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file, indent=2, ensure_ascii=False)


def load_model_artifacts(model_path, metadata_path):
    model = None
    metadata = {
        "model_version": MODEL_VERSION,
        "training_examples": 0,
        "label_counts": {},
        "loaded_from_disk": False,
    }

    if os.path.exists(model_path):
        try:
            with open(model_path, "rb") as model_file:
                model = pickle.load(model_file)
        except Exception:
            model = None

    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r", encoding="utf-8") as metadata_file:
                metadata = json.load(metadata_file)
        except Exception:
            pass

    metadata["loaded_from_disk"] = model is not None
    return model, metadata


def predict_subcategories(model, products):
    if model is None:
        return []

    features = [build_feature_text(product) for product in products]
    predicted_labels = model.predict(features)
    probabilities = model.predict_proba(features)
    predictions = []
    for label, probability_row in zip(predicted_labels, probabilities):
        confidence = float(max(probability_row)) if len(probability_row) else 0.0
        predictions.append(
            {
                "subcategory": label,
                "confidence": round(confidence, 4),
            }
        )
    return predictions


def build_change_report(products):
    changed = []
    changed_pairs = Counter()
    missing_before = 0
    missing_after = 0

    for product in products:
        before = product.get("previous_subcategory")
        after = product.get("subcategory")
        if not before:
            missing_before += 1
        if not after:
            missing_after += 1
        if before != after:
            changed_pairs[(before or "(none)", after or "(none)")] += 1
            changed.append(
                {
                    "asin": product.get("asin"),
                    "retailer": product.get("retailer"),
                    "brand": product.get("brand"),
                    "name": product.get("name"),
                    "raw_name": product.get("raw_name"),
                    "previous_category": product.get("previous_category"),
                    "previous_subcategory": before,
                    "category": product.get("category"),
                    "subcategory": after,
                    "ai_confidence": product.get("ai_confidence"),
                    "ai_label_source": product.get("ai_label_source"),
                }
            )

    changed.sort(
        key=lambda item: (
            item.get("retailer") or "",
            -(item.get("ai_confidence") or 0),
            item.get("name") or "",
        )
    )
    return {
        "model_version": MODEL_VERSION,
        "changed_count": len(changed),
        "missing_before": missing_before,
        "missing_after": missing_after,
        "top_changes": [
            {
                "from": before,
                "to": after,
                "count": count,
            }
            for (before, after), count in changed_pairs.most_common(50)
        ],
        "examples": changed[:250],
    }
