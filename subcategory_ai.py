import json
import math
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


MODEL_VERSION = "subcategory-ai-v2"


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
        f"variation {normalize_text(product.get('variation'))}",
        f"sources {normalize_text(flatten_text_list(product.get('sources')))}",
        f"source_categories {normalize_text(flatten_text_list(product.get('source_categories')))}",
        f"url {normalize_text(product.get('url'))}",
        f"retail_source_url {normalize_text(product.get('retail_source_url'))}",
        f"tags {normalize_text(flatten_text_list(product.get('tags')))}",
    ]
    return " ".join(part for part in parts if part.strip())


def tokenize_feature_text(text):
    normalized = normalize_text(text)
    if not normalized:
        return []
    return [token for token in normalized.split(" ") if token]


def train_token_vote_model(labels, features):
    label_doc_counts = Counter(labels)
    label_token_counts = {}
    label_total_tokens = {}
    vocabulary = set()

    for label, feature_text in zip(labels, features):
        tokens = tokenize_feature_text(feature_text)
        counter = label_token_counts.setdefault(label, Counter())
        counter.update(tokens)
        label_total_tokens[label] = label_total_tokens.get(label, 0) + len(tokens)
        vocabulary.update(tokens)

    return {
        "model_type": "token_vote",
        "labels": sorted(label_doc_counts.keys()),
        "label_doc_counts": dict(label_doc_counts),
        "label_total_tokens": dict(label_total_tokens),
        "label_token_counts": {label: dict(counter) for label, counter in label_token_counts.items()},
        "vocab_size": len(vocabulary),
        "total_docs": sum(label_doc_counts.values()),
    }


def predict_token_vote_model(model, products, allowed_subcategories=None, subcategory_priors=None):
    labels = list(model.get("labels") or [])
    label_doc_counts = model.get("label_doc_counts") or {}
    label_total_tokens = model.get("label_total_tokens") or {}
    raw_label_token_counts = model.get("label_token_counts") or {}
    label_token_counts = {
        label: Counter(raw_label_token_counts.get(label) or {})
        for label in labels
    }
    vocab_size = max(1, int(model.get("vocab_size") or 1))
    total_docs = max(1, int(model.get("total_docs") or 1))

    predictions = []
    for index, product in enumerate(products):
        tokens = tokenize_feature_text(build_feature_text(product))
        allowed = set(allowed_subcategories[index] or []) if allowed_subcategories and index < len(allowed_subcategories) else None
        priors = subcategory_priors[index] if subcategory_priors and index < len(subcategory_priors) else {}
        candidate_labels = [label for label in labels if not allowed or label in allowed]
        if not labels:
            predictions.append({"subcategory": None, "confidence": 0.0})
            continue

        scores = {}
        for label in labels:
            doc_count = max(1, int(label_doc_counts.get(label) or 1))
            total_tokens = int(label_total_tokens.get(label) or 0)
            token_counts = label_token_counts.get(label) or Counter()
            score = math.log(doc_count / total_docs)
            denominator = total_tokens + vocab_size
            for token in tokens:
                score += math.log((token_counts.get(token, 0) + 1) / denominator)
            if priors and label in priors:
                score += float(priors[label]) * 2.5
            scores[label] = score

        if candidate_labels:
            best_label = max(candidate_labels, key=lambda label: scores[label])
        else:
            best_label = max(scores, key=scores.get)
        max_score = max(scores.values())
        exp_scores = {label: math.exp(score - max_score) for label, score in scores.items()}
        total_score = sum(exp_scores.values()) or 1.0
        confidence = exp_scores[best_label] / total_score
        predictions.append(
            {
                "subcategory": best_label,
                "confidence": round(float(confidence), 4),
            }
        )

    return predictions


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

    if len(label_counts) < 2:
        return None, metadata

    if not sklearn_available():
        metadata["model_type"] = "token_vote"
        return train_token_vote_model(labels, features), metadata

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
    metadata["model_type"] = "sklearn_logreg"
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


def predict_subcategories(model, products, allowed_subcategories=None, subcategory_priors=None):
    if model is None:
        return []

    if isinstance(model, dict) and model.get("model_type") == "token_vote":
        return predict_token_vote_model(
            model,
            products,
            allowed_subcategories=allowed_subcategories,
            subcategory_priors=subcategory_priors,
        )

    features = [build_feature_text(product) for product in products]
    probabilities = model.predict_proba(features)
    classes = list(getattr(model, "classes_", []))
    predictions = []
    for index, probability_row in enumerate(probabilities):
        allowed = set(allowed_subcategories[index] or []) if allowed_subcategories and index < len(allowed_subcategories) else None
        ranked = sorted(
            zip(classes, probability_row),
            key=lambda item: item[1],
            reverse=True,
        )
        chosen_label = None
        chosen_confidence = 0.0
        if allowed:
            for label, probability in ranked:
                if label in allowed:
                    chosen_label = label
                    chosen_confidence = float(probability)
                    break
        if chosen_label is None and ranked:
            chosen_label = ranked[0][0]
            chosen_confidence = float(ranked[0][1])
        predictions.append(
            {
                "subcategory": chosen_label,
                "confidence": round(chosen_confidence, 4),
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
