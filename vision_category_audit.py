import argparse
import hashlib
import json
import os
import random
import sys
from io import BytesIO
from pathlib import Path

import requests

from fixed_taxonomy import build_fixed_taxonomy


CATEGORY_DESCRIPTIONS = {
    "Produce": "fresh fruits, vegetables, herbs, mushrooms, and cut fresh produce",
    "Meat & Seafood": "raw or cooked meat, poultry, seafood, sausage, bacon, and meat alternatives",
    "Dairy & Eggs": "milk, cheese, yogurt, eggs, butter, creamers, and dairy alternatives",
    "Bakery": "bread, bagels, tortillas, pastries, cakes, pies, cookies, brownies, and bakery items",
    "Pantry": "shelf-stable pasta, grains, cereal, sauces, condiments, canned foods, oils, and cooking staples",
    "Snacks": "chips, crackers, cookies, candy, bars, popcorn, jerky, fruit snacks, and ready-to-eat snacks",
    "Beverages": "water, juice, soda, coffee, tea, kombucha, sports drinks, drink mixes, and non-alcoholic drinks",
    "Alcohol": "beer, wine, spirits, hard seltzer, cider, cocktails, and non-alcoholic beer or wine",
    "Frozen": "frozen meals, pizza, vegetables, fruit, ice cream, desserts, appetizers, and frozen snacks",
    "Prepared Foods": "ready-to-eat or refrigerated meals, soups, salads, sushi, kimchi, tofu, and deli foods",
    "International": "Asian, Korean, Japanese, Indian, Hispanic, Mediterranean, and other regional grocery foods",
    "Supplements & Wellness": "vitamins, minerals, probiotics, protein, collagen, herbal supplements, wellness shots, and essential oils",
    "Beauty & Personal Care": "skin care, hair care, body care, oral care, deodorant, soap, cosmetics, sunscreen, and grooming products",
    "Household": "cleaning supplies, dishwashing, laundry, paper goods, trash bags, foil, food storage, kitchen supplies, and home goods",
    "Baby": "baby food, formula, diapers, wipes, baby snacks, baby care, and baby wellness products",
}


PAIR_HINTS = {
    ("Beverages", "Coffee Beans & Grounds"): "bags or containers of whole bean coffee, ground coffee, coffee roast, or loose coffee grounds",
    ("Beverages", "Coffee Pods & K-Cups"): "single serve coffee pods, capsules, K-Cups, or Nespresso compatible pods",
    ("Beverages", "Ready-to-Drink Coffee"): "bottled, canned, or carton coffee drinks ready to drink",
    ("Household", "Foil, Wrap & Bags"): "aluminum foil, parchment paper, plastic wrap, wax paper, sandwich bags, or food wrap",
    ("Household", "Kitchen Supplies"): "coffee filters, disposable kitchen tools, food preparation supplies, or kitchen utility items",
    ("Household", "Insect Repellent"): "bug spray, mosquito repellent, insect repellent, or pest repellent",
    ("Supplements & Wellness", "Essential Oils"): "essential oil bottles, aromatherapy oils, diffuser oils, roll-ons, or essential oil kits",
    ("Beauty & Personal Care", "Body Care"): "body lotion, body wash, shower gel, body cream, bath products, or body moisturizer",
    ("Beauty & Personal Care", "Hair Care"): "shampoo, conditioner, hair styling, scalp treatment, or hair care products",
    ("Snacks", "Seaweed Snacks"): "crispy roasted seaweed sheets or packaged seaweed snack products",
}


def require_vision_dependencies():
    missing = []
    try:
        import torch  # noqa: F401
    except Exception:
        missing.append("torch")
    try:
        from PIL import Image  # noqa: F401
    except Exception:
        missing.append("Pillow")
    try:
        from transformers import CLIPModel, CLIPProcessor  # noqa: F401
    except Exception:
        missing.append("transformers")

    if missing:
        print(
            "Missing vision dependencies: "
            + ", ".join(missing)
            + "\nInstall them with:\n"
            + "  .venv/bin/python3 -m pip install torch torchvision pillow transformers tqdm",
            file=sys.stderr,
        )
        raise SystemExit(2)


def load_json(path):
    with open(path) as file:
        return json.load(file)


def save_json(path, value):
    with open(path, "w") as file:
        json.dump(value, file, indent=2, ensure_ascii=False)
        file.write("\n")


def product_id(product, index):
    return (
        product.get("asin")
        or product.get("url")
        or product.get("raw_name")
        or product.get("name")
        or f"product-{index}"
    )


def normalize_label(value):
    return " ".join(str(value or "").lower().split())


def image_cache_path(cache_dir, url):
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return cache_dir / f"{digest}.img"


def load_image(url, cache_dir, timeout=15):
    from PIL import Image

    if not url:
        return None
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = image_cache_path(cache_dir, url)
    try:
        if path.exists():
            content = path.read_bytes()
        else:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            content = response.content
            path.write_bytes(content)
        return Image.open(BytesIO(content)).convert("RGB")
    except Exception:
        return None


def category_prompt(category_name):
    description = CATEGORY_DESCRIPTIONS.get(category_name, category_name)
    return f"a grocery product photo of {category_name}. {description}."


def pair_prompt(category_name, subcategory_name):
    hint = PAIR_HINTS.get((category_name, subcategory_name))
    if not hint:
        hint = f"{subcategory_name} products in the {category_name} grocery category"
    return f"a grocery product photo of {hint}."


def build_labels(taxonomy):
    categories = [category["name"] for category in taxonomy.get("categories", [])]
    pairs = []
    for category in taxonomy.get("categories", []):
        category_name = category["name"]
        for subcategory in category.get("subcategories", []):
            pairs.append((category_name, subcategory["name"]))
    return categories, pairs


def choose_device(torch):
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def encode_texts(model, processor, texts, device, batch_size=64):
    import torch

    vectors = []
    with torch.no_grad():
        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            inputs = processor(text=batch, return_tensors="pt", padding=True, truncation=True).to(device)
            features = model.get_text_features(**inputs)
            features = features / features.norm(dim=-1, keepdim=True)
            vectors.append(features.detach().cpu())
    return torch.cat(vectors, dim=0)


def encode_image(model, processor, image, device):
    import torch

    with torch.no_grad():
        inputs = processor(images=image, return_tensors="pt").to(device)
        features = model.get_image_features(**inputs)
        features = features / features.norm(dim=-1, keepdim=True)
        return features.detach().cpu()[0]


def product_text_prompt(product):
    parts = [
        product.get("brand"),
        product.get("name"),
        product.get("raw_name"),
        " ".join(product.get("source_categories") or []),
    ]
    text = " ".join(str(part) for part in parts if part)
    return f"a grocery product listing for {text}" if text else "a grocery product listing"


def score_labels(image_vector, text_vectors, product_text_vector=None, image_weight=1.0, temperature=100.0):
    import torch

    image_similarities = text_vectors @ image_vector
    if product_text_vector is not None and image_weight < 1.0:
        text_similarities = text_vectors @ product_text_vector
        combined = image_weight * image_similarities + (1.0 - image_weight) * text_similarities
    else:
        text_similarities = None
        combined = image_similarities
    logits = temperature * combined
    probabilities = torch.softmax(logits, dim=0)
    top = torch.argsort(probabilities, descending=True)
    scored = []
    for index in top:
        item = {
            "index": int(index),
            "score": round(float(probabilities[index]), 4),
            "similarity": round(float(combined[index]), 4),
            "image_similarity": round(float(image_similarities[index]), 4),
        }
        if text_similarities is not None:
            item["text_similarity"] = round(float(text_similarities[index]), 4)
        scored.append(item)
    return scored


def compact_product(product, index):
    return {
        "id": product_id(product, index),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "brand": product.get("brand"),
        "retailer": product.get("retailer"),
        "sources": product.get("sources") or [],
        "current_category": product.get("category"),
        "current_subcategory": product.get("subcategory"),
        "ai_confidence": product.get("ai_confidence"),
        "image": product.get("image"),
    }


def current_category_index(product, categories):
    current = normalize_label(product.get("category"))
    for index, category in enumerate(categories):
        if normalize_label(category) == current:
            return index
    return None


def current_pair_index(product, pairs):
    current = (normalize_label(product.get("category")), normalize_label(product.get("subcategory")))
    for index, pair in enumerate(pairs):
        if (normalize_label(pair[0]), normalize_label(pair[1])) == current:
            return index
    return None


def score_for_index(scores, target_index):
    for rank, item in enumerate(scores, start=1):
        if item["index"] == target_index:
            return {**item, "rank": rank}
    return None


def locked_pair_from_clip(*, category_all_scores, pair_all_scores, categories, pairs, top_k):
    best_category_score = category_all_scores[0]
    best_pair_score = pair_all_scores[0]
    best_category = categories[best_category_score["index"]]
    best_pair_category, best_pair_subcategory = pairs[best_pair_score["index"]]

    pair_indices_by_category = {
        category: [index for index, pair in enumerate(pairs) if pair[0] == category]
        for category in categories
    }
    category_index_by_pair = {
        pair: categories.index(pair[0])
        for pair in pairs
    }

    category_confidence = best_category_score["score"]
    pair_confidence = best_pair_score["score"]

    # If CLIP is more certain about the exact subcategory/pair, trust that and derive
    # its parent category. Otherwise lock the subcategory search inside the best
    # category so the reported category/subcategory can never conflict.
    if pair_confidence >= category_confidence:
        locked_category = best_pair_category
        locked_subcategory = best_pair_subcategory
        locked_pair_index = best_pair_score["index"]
        locked_category_index = category_index_by_pair[(locked_category, locked_subcategory)]
        source = "subcategory-first"
        locked_pair_score = best_pair_score
        locked_category_score = score_for_index(category_all_scores, locked_category_index)
    else:
        candidate_pair_indices = pair_indices_by_category.get(best_category) or []
        candidate_scores = [
            item for item in pair_all_scores if item["index"] in candidate_pair_indices
        ]
        locked_pair_score = candidate_scores[0] if candidate_scores else best_pair_score
        locked_pair_index = locked_pair_score["index"]
        locked_category, locked_subcategory = pairs[locked_pair_index]
        locked_category_index = categories.index(locked_category)
        source = "category-first"
        locked_category_score = best_category_score

    top_locked_subcategories = [
        {
            "category": pairs[item["index"]][0],
            "subcategory": pairs[item["index"]][1],
            **{key: value for key, value in item.items() if key != "index"},
        }
        for item in pair_all_scores
        if pairs[item["index"]][0] == locked_category
    ][:top_k]

    return {
        "category": locked_category,
        "subcategory": locked_subcategory,
        "source": source,
        "score": locked_pair_score["score"],
        "category_score": locked_category_score,
        "subcategory_score": locked_pair_score,
        "top_subcategories_in_category": top_locked_subcategories,
    }


def pick_products(products, limit=None, seed=17, only_categories=None):
    selected = products
    if only_categories:
        allowed = {normalize_label(category) for category in only_categories}
        selected = [p for p in selected if normalize_label(p.get("category")) in allowed]
    if limit and limit < len(selected):
        random.seed(seed)
        selected = random.sample(selected, limit)
    return selected


def run_audit(args):
    require_vision_dependencies()

    import torch
    from transformers import CLIPModel, CLIPProcessor

    products = load_json(args.products)
    taxonomy = build_fixed_taxonomy()
    categories, pairs = build_labels(taxonomy)
    category_texts = [category_prompt(category) for category in categories]
    pair_texts = [pair_prompt(category, subcategory) for category, subcategory in pairs]

    selected_products = pick_products(
        products,
        limit=args.limit,
        seed=args.seed,
        only_categories=args.only_category,
    )

    device = choose_device(torch)
    print(f"[vision] loading {args.model} on {device}")
    processor = CLIPProcessor.from_pretrained(args.model)
    model = CLIPModel.from_pretrained(args.model).to(device)
    model.eval()

    print(f"[vision] encoding {len(category_texts)} category prompts and {len(pair_texts)} subcategory prompts")
    category_vectors = encode_texts(model, processor, category_texts, device)
    pair_vectors = encode_texts(model, processor, pair_texts, device)

    cache_dir = Path(args.image_cache)
    reviewed = []
    failures = []
    for offset, product in enumerate(selected_products, start=1):
        name = product.get("name") or product.get("raw_name") or "(unnamed)"
        print(f"[vision] {offset}/{len(selected_products)} {name[:90]}")
        image = load_image(product.get("image"), cache_dir)
        if image is None:
            failures.append({"product": compact_product(product, offset), "error": "image_load_failed"})
            continue

        image_vector = encode_image(model, processor, image, device)
        text_vector = None
        if args.image_weight < 1.0:
            text_vector = encode_texts(model, processor, [product_text_prompt(product)], device)[0]
        category_all_scores = score_labels(
            image_vector,
            category_vectors,
            product_text_vector=text_vector,
            image_weight=args.image_weight,
        )
        pair_all_scores = score_labels(
            image_vector,
            pair_vectors,
            product_text_vector=text_vector,
            image_weight=args.image_weight,
        )
        category_scores = category_all_scores[: args.top_k]
        pair_scores = pair_all_scores[: args.top_k]

        best_category = categories[category_scores[0]["index"]]
        best_pair_category, best_pair_subcategory = pairs[pair_scores[0]["index"]]
        locked_pair = locked_pair_from_clip(
            category_all_scores=category_all_scores,
            pair_all_scores=pair_all_scores,
            categories=categories,
            pairs=pairs,
            top_k=args.top_k,
        )
        current_cat_index = current_category_index(product, categories)
        current_pair_idx = current_pair_index(product, pairs)
        current_cat_score = None
        current_pair_score = None
        if current_cat_index is not None:
            current_cat_score = score_for_index(category_all_scores, current_cat_index)
        if current_pair_idx is not None:
            current_pair_score = score_for_index(pair_all_scores, current_pair_idx)

        category_disagrees = normalize_label(locked_pair["category"]) != normalize_label(product.get("category"))
        pair_disagrees = (
            normalize_label(locked_pair["category"]) != normalize_label(product.get("category"))
            or normalize_label(locked_pair["subcategory"]) != normalize_label(product.get("subcategory"))
        )
        record = {
            "product": compact_product(product, offset),
            "clip_locked_pair": locked_pair,
            "clip_category": {
                "category": best_category,
                "score": category_scores[0]["score"],
                "current_score": current_cat_score,
                "top": [
                    {"category": categories[item["index"]], **{k: v for k, v in item.items() if k != "index"}}
                    for item in category_scores
                ],
            },
            "clip_subcategory": {
                "category": best_pair_category,
                "subcategory": best_pair_subcategory,
                "score": pair_scores[0]["score"],
                "current_score": current_pair_score,
                "top": [
                    {
                        "category": pairs[item["index"]][0],
                        "subcategory": pairs[item["index"]][1],
                        **{k: v for k, v in item.items() if k != "index"},
                    }
                    for item in pair_scores
                ],
            },
            "category_disagrees": category_disagrees,
            "subcategory_disagrees": pair_disagrees,
        }
        if args.disagreements_only and not category_disagrees and not pair_disagrees:
            continue
        reviewed.append(record)

    report = {
        "model": args.model,
        "device": device,
        "image_weight": args.image_weight,
        "products_file": args.products,
        "audited_count": len(selected_products),
        "reported_count": len(reviewed),
        "failure_count": len(failures),
        "category_disagreement_count": sum(1 for item in reviewed if item["category_disagrees"]),
        "subcategory_disagreement_count": sum(1 for item in reviewed if item["subcategory_disagrees"]),
        "results": reviewed,
        "failures": failures,
    }
    save_json(args.output, report)
    print(
        f"[vision] wrote {args.output}: "
        f"{report['reported_count']} reported, "
        f"{report['category_disagreement_count']} category disagreements, "
        f"{report['subcategory_disagreement_count']} subcategory disagreements, "
        f"{report['failure_count']} image failures"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Audit grocery taxonomy labels with local CLIP image classification."
    )
    parser.add_argument("--products", default="combined_products.json")
    parser.add_argument("--output", default="vision_category_audit.json")
    parser.add_argument("--image-cache", default=".cache/vision_images")
    parser.add_argument("--model", default="openai/clip-vit-base-patch32")
    parser.add_argument(
        "--image-weight",
        type=float,
        default=0.65,
        help="Blend CLIP image and product text signals. 1.0 means image-only.",
    )
    parser.add_argument("--limit", type=int, default=80, help="Random sample size. Use 0 for all products.")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--only-category", action="append", default=[])
    parser.add_argument("--disagreements-only", action="store_true")
    args = parser.parse_args()
    if args.limit == 0:
        args.limit = None
    args.image_weight = max(0.0, min(args.image_weight, 1.0))
    return args


if __name__ == "__main__":
    run_audit(parse_args())
