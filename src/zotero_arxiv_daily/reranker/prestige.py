"""Small, explicit preference bonus; not an assessment of scientific quality."""
import math
import re
import unicodedata


def normalize(value):
    value = unicodedata.normalize("NFKD", value).casefold()
    value = "".join(c for c in value if not unicodedata.combining(c))
    return " ".join(re.findall(r"\w+", value))


def apply_prestige_bonus(papers, settings):
    bonus = float(settings.get("bonus", 0))
    if not math.isfinite(bonus) or not 0 <= bonus <= 0.1:
        raise ValueError("Prestige bonus must be between 0 and 0.1")
    institutions = [normalize(x) for x in settings.get("institutions", []) if normalize(x)]
    authors = {normalize(x) for x in settings.get("authors", []) if normalize(x)}
    for paper in papers:
        institution_match = any(
            f" {alias} " in f" {normalize(affiliation)} "
            for affiliation in (paper.affiliations or []) for alias in institutions
        )
        author_match = any(normalize(name) in authors for name in paper.authors)
        if institution_match or author_match:
            paper.score += bonus
    return sorted(papers, key=lambda paper: paper.score, reverse=True)
