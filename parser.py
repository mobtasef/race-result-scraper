from __future__ import annotations

from bs4 import BeautifulSoup

LABEL_MAP = {
    "bib": "bib",
    "name": "name",
    "gender": "gender",
    "event": "event",
    "status": "status",
    "gun time": "gun_time",
    "net time": "net_time",
    "chip time": "net_time",
    "overall gun time rank": "overall_gun_rank",
    "overall chip time rank": "overall_chip_rank",
    "gender gun time rank": "gender_gun_rank",
    "gender chip time rank": "gender_chip_rank",
    "age group gun time rank": "age_group_gun_rank",
    "age group chip time rank": "age_group_chip_rank",
}


def parse_result_html(html: str, bib: int) -> dict | None:
    if not html or len(html.strip()) < 200:
        return None

    soup = BeautifulSoup(html, "lxml")

    # Detect empty result — form still present means no result was returned
    if soup.find("form"):
        return None

    data: dict = {"bib": bib}

    # Extract all label-value row pairs
    for row in soup.find_all(class_="row"):
        cols = row.find_all(class_="col")
        if len(cols) == 2:
            label = cols[0].get_text(strip=True).lower()
            value = cols[1].get_text(strip=True)
            key = LABEL_MAP.get(label)
            if key and key not in data:
                data[key] = value

    if "name" not in data:
        return None

    # Fill missing fields with empty string
    for field in LABEL_MAP.values():
        data.setdefault(field, "")

    return data
