import re
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import List, Optional


def normalize_brand_text(text):
    if not text:
        return ""

    text = text.lower().strip()
    text = text.replace("’", "").replace("'", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def significant_brand_tokens(text, connectors):
    return [
        token
        for token in normalize_brand_text(text).split()
        if token and token not in connectors
    ]


def brand_trigrams(text):
    compact = normalize_brand_text(text).replace(" ", "")
    if not compact:
        return set()
    if len(compact) < 3:
        return {compact}
    return {compact[index:index + 3] for index in range(len(compact) - 2)}


def token_jaccard(left_tokens, right_tokens):
    left = set(left_tokens)
    right = set(right_tokens)
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def trigram_similarity(left, right):
    left_trigrams = brand_trigrams(left)
    right_trigrams = brand_trigrams(right)
    if not left_trigrams or not right_trigrams:
        return 0.0
    return len(left_trigrams & right_trigrams) / len(left_trigrams | right_trigrams)


@dataclass
class BrandObservation:
    brand: str
    canonical_seed: Optional[str]
    normalized_key: str
    significant_tokens: List[str]
    count: int
    samples: List[str]

    @property
    def first_token(self):
        return self.significant_tokens[0] if self.significant_tokens else ""

    @property
    def token_count(self):
        return len(self.significant_tokens)


def _prefixed_by_descriptor(observation, candidate, descriptor_starters, generic_words):
    shorter = observation
    longer = candidate
    if len(shorter.normalized_key) > len(longer.normalized_key):
        shorter, longer = longer, shorter

    if not longer.normalized_key.startswith(shorter.normalized_key + " "):
        return False

    remainder = longer.normalized_key[len(shorter.normalized_key):].strip()
    if not remainder:
        return False

    remainder_tokens = remainder.split()
    if (
        len(shorter.significant_tokens) >= 2
        and 1 <= len(remainder_tokens) <= 2
        and not any(any(character.isdigit() for character in token) for token in remainder_tokens)
    ):
        return True

    return all(token in descriptor_starters or token in generic_words for token in remainder_tokens)


def _brand_similarity_score(left, right):
    token_score = token_jaccard(left.significant_tokens, right.significant_tokens)
    trigram_score = trigram_similarity(left.brand, right.brand)
    sequence_score = SequenceMatcher(None, left.normalized_key, right.normalized_key).ratio()
    return token_score, trigram_score, sequence_score


def _should_merge_brands(left, right, descriptor_starters, generic_words):
    if not left.normalized_key or not right.normalized_key:
        return False

    if left.canonical_seed and right.canonical_seed and left.canonical_seed == right.canonical_seed:
        return True

    if left.first_token and right.first_token and left.first_token != right.first_token:
        if not _prefixed_by_descriptor(left, right, descriptor_starters, generic_words):
            return False

    if _prefixed_by_descriptor(left, right, descriptor_starters, generic_words):
        return True

    token_score, trigram_score, sequence_score = _brand_similarity_score(left, right)
    same_first_token = bool(left.first_token and left.first_token == right.first_token)
    shared_prefix = left.significant_tokens[:2] == right.significant_tokens[:2] and len(left.significant_tokens) >= 2

    if same_first_token and token_score >= 0.66 and trigram_score >= 0.72:
        return True

    if shared_prefix and sequence_score >= 0.8:
        return True

    return False


def _canonical_choice_score(observation, generic_words, descriptor_starters):
    normalized = observation.normalized_key
    generic_penalty = sum(1 for token in observation.significant_tokens if token in generic_words)
    terminal_token = normalized.split()[-1] if normalized.split() else ""
    incomplete_suffix_penalty = 1 if terminal_token in {"of", "the", "and", "&", "by"} else 0
    descriptor_penalty = sum(
        1
        for token in observation.significant_tokens[2:]
        if token in descriptor_starters or token in generic_words
    )
    return (
        1 if observation.canonical_seed else 0,
        -descriptor_penalty,
        -incomplete_suffix_penalty,
        -observation.token_count,
        -generic_penalty,
        observation.count,
        -len(normalized),
    )


def build_brand_family_map(
    products,
    *,
    alias_map,
    connectors,
    generic_words,
    descriptor_starters,
):
    connectors = {token.lower() for token in connectors}
    generic_words = {token.lower() for token in generic_words}
    descriptor_starters = {token.lower() for token in descriptor_starters}

    alias_lookup = {}
    for canonical, aliases in (alias_map or {}).items():
        for alias in aliases:
            alias_lookup[normalize_brand_text(alias)] = canonical
        alias_lookup[normalize_brand_text(canonical)] = canonical

    counts = defaultdict(int)
    samples = defaultdict(list)

    for product in products:
        brand = (product.get("brand") or "").strip()
        if not brand:
            continue
        counts[brand] += 1
        sample_name = (product.get("raw_name") or product.get("name") or "").strip()
        if sample_name and sample_name not in samples[brand] and len(samples[brand]) < 3:
            samples[brand].append(sample_name)

    observations = {}
    for brand, count in counts.items():
        normalized_key = normalize_brand_text(brand)
        observations[brand] = BrandObservation(
            brand=brand,
            canonical_seed=alias_lookup.get(normalized_key),
            normalized_key=normalized_key,
            significant_tokens=significant_brand_tokens(brand, connectors),
            count=count,
            samples=samples.get(brand, []),
        )

    parents = {brand: brand for brand in observations}

    def find(brand):
        while parents[brand] != brand:
            parents[brand] = parents[parents[brand]]
            brand = parents[brand]
        return brand

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        parents[right_root] = left_root

    grouped_by_token = defaultdict(list)
    for brand, observation in observations.items():
        grouped_by_token[observation.first_token or observation.normalized_key[:1]].append(brand)

    for group in grouped_by_token.values():
        for index, left_brand in enumerate(group):
            left = observations[left_brand]
            for right_brand in group[index + 1:]:
                right = observations[right_brand]
                if _should_merge_brands(left, right, descriptor_starters, generic_words):
                    union(left_brand, right_brand)

    clusters = defaultdict(list)
    for brand in observations:
        clusters[find(brand)].append(brand)

    family_map = {}
    cluster_report = []
    for members in clusters.values():
        if len(members) < 2:
            continue

        canonical_brand = max(
            members,
            key=lambda member: _canonical_choice_score(observations[member], generic_words, descriptor_starters),
        )
        for member in members:
            if member != canonical_brand:
                family_map[member] = canonical_brand

        cluster_report.append(
            {
                "canonical": canonical_brand,
                "members": sorted(members),
                "count": sum(observations[member].count for member in members),
                "samples": {member: observations[member].samples for member in members},
            }
        )

    cluster_report.sort(key=lambda item: (-item["count"], item["canonical"].lower()))
    return family_map, cluster_report
