import csv
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from itertools import chain
from pathlib import Path

from sqlalchemy import or_

from app.extensions import db
from app.models.outlet import Outlet


def normalize_py(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def normalize_for_match(s: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (s or "").lower())
    return " ".join(cleaned.split())


@dataclass
class MatchResult:
    brand_name: str | None
    webshop_name: str
    outlet_id: int | None
    outlet_code: str | None
    matched_field: str | None
    score: float | None
    status: str  # "matched" | "review" | "unmatched"


HEADER_TO_FIELD = {
    "brand": "brand_name",
    "merchant": "brand_name",
    "merchant name": "brand_name",
    "nama brand": "brand_name",
    "branch": "webshop_name",
    "outlet": "webshop_name",
    "outlet name": "webshop_name",
    "nama outlet": "webshop_name",
    "nama cabang": "webshop_name",
}
DEFAULT_COLUMN_INDEXES = {
    "brand_name": 1,
    "webshop_name": 2,
}
SOURCE_MATCH_FIELDS = (
    "outlet_name_webshop",
    "outlet_name_qpon",
    "outlet_name_grab",
    "outlet_name_gojek",
)


def _clean_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_header(header: str) -> str:
    return " ".join(_clean_str(header).replace("\ufeff", "").lower().split())


def _resolve_columns(headers: list[str] | None) -> dict:
    resolved = {}
    if headers:
        for idx, header in enumerate(headers):
            field_name = HEADER_TO_FIELD.get(_normalize_header(header))
            if field_name and field_name not in resolved:
                resolved[field_name] = idx

    for field_name, fallback_idx in DEFAULT_COLUMN_INDEXES.items():
        if field_name not in resolved:
            resolved[field_name] = fallback_idx

    return resolved


def _looks_like_header(row: list[str] | None) -> bool:
    if not row:
        return False
    normalized = {_normalize_header(cell) for cell in row}
    known_headers = set(HEADER_TO_FIELD.keys())
    return bool(normalized.intersection(known_headers))


def _resolve_csv_path(csv_path: str) -> Path:
    raw_path = _clean_str(csv_path)
    if raw_path == "":
        raise FileNotFoundError("CSV path is empty.")

    candidate = Path(raw_path).expanduser()
    search_paths = [candidate]

    if not candidate.is_absolute():
        project_root = Path(__file__).resolve().parents[2]
        search_paths.extend(
            [
                project_root / candidate,
                project_root / "scripts" / candidate,
            ]
        )

    checked_paths: list[str] = []
    for path in search_paths:
        resolved = path.resolve()
        checked_paths.append(str(resolved))
        if resolved.is_file():
            return resolved

    raise FileNotFoundError(
        f"Webshop CSV not found: '{csv_path}'. Checked: {', '.join(checked_paths)}"
    )


def parse_webshop_csv_rows(csv_path: str) -> list[tuple[str | None, str]]:
    rows: list[tuple[str | None, str]] = []
    seen: set[tuple[str, str]] = set()
    resolved_csv_path = _resolve_csv_path(csv_path)

    with open(resolved_csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        first_row = next(reader, None)
        if not first_row:
            return rows

        if _looks_like_header(first_row):
            columns = _resolve_columns(first_row)
            csv_rows = reader
        else:
            columns = DEFAULT_COLUMN_INDEXES.copy()
            csv_rows = chain([first_row], reader)

        for row in csv_rows:
            if not row:
                continue

            def _value(field: str) -> str:
                idx = columns.get(field)
                if idx is None or idx < 0 or idx >= len(row):
                    return ""
                return row[idx]

            brand_name = _clean_str(_value("brand_name")).strip('"').strip("'")
            webshop_name = _clean_str(_value("webshop_name")).strip('"').strip("'")
            if webshop_name == "":
                continue

            dedupe_key = (normalize_for_match(brand_name), normalize_for_match(webshop_name))
            if dedupe_key in seen:
                continue

            seen.add(dedupe_key)
            rows.append((brand_name or None, webshop_name))

    return rows


def _score_match(target: str, candidate: str) -> float:
    if not target or not candidate:
        return 0.0
    if target == candidate:
        return 1.0

    score = SequenceMatcher(None, target, candidate).ratio()

    if target in candidate or candidate in target:
        score = max(score, 0.9)

    target_tokens = set(target.split())
    candidate_tokens = set(candidate.split())
    if target_tokens and candidate_tokens:
        token_overlap = len(target_tokens.intersection(candidate_tokens)) / len(
            target_tokens.union(candidate_tokens)
        )
        score = max(score, token_overlap * 0.95)

    return min(1.0, score)


def _get_available_match_fields() -> list[str]:
    return [field for field in SOURCE_MATCH_FIELDS if hasattr(Outlet, field)]


def find_best_outlet_for_webshop_name(
    webshop_name: str,
    outlets: list[Outlet],
    match_fields: list[str],
    brand_name: str | None = None,
):
    target = normalize_for_match(webshop_name)
    if target == "":
        return None, 0.0, None

    brand_target = normalize_for_match(brand_name or "")
    best_outlet = None
    best_score = 0.0
    best_field = None

    for outlet in outlets:
        outlet_brand = normalize_for_match(getattr(outlet, "brand", "") or "")

        for field in match_fields:
            candidate_value = _clean_str(getattr(outlet, field, ""))
            if candidate_value == "":
                continue

            score = _score_match(target, normalize_for_match(candidate_value))

            if brand_target and outlet_brand:
                if brand_target == outlet_brand:
                    score = min(1.0, score + 0.05)
                elif brand_target in outlet_brand or outlet_brand in brand_target:
                    score = min(1.0, score + 0.03)

            if score > best_score:
                best_outlet = outlet
                best_score = score
                best_field = field

    return best_outlet, best_score, best_field


def import_webshop_names_to_outlets(
    csv_path: str,
    threshold_auto: float = 0.82,
    threshold_review: float = 0.65,
    dry_run: bool = True,
) -> list[MatchResult]:
    webshop_rows = parse_webshop_csv_rows(csv_path)
    results: list[MatchResult] = []
    assigned_outlet_ids: dict[int, str] = {}

    match_fields = _get_available_match_fields()
    if not match_fields:
        return [
            MatchResult(brand_name, webshop_name, None, None, None, None, "unmatched")
            for brand_name, webshop_name in webshop_rows
        ]

    outlet_name_filters = [getattr(Outlet, field).isnot(None) for field in match_fields]
    outlets = Outlet.query.filter(or_(*outlet_name_filters)).filter(Outlet.brand == "MP78").all()

    for brand_name, webshop_name in webshop_rows:
        outlet, score, matched_field = find_best_outlet_for_webshop_name(
            webshop_name=webshop_name,
            outlets=outlets,
            match_fields=match_fields,
            brand_name=brand_name,
        )

        if not outlet or score < threshold_review:
            results.append(
                MatchResult(
                    brand_name=brand_name,
                    webshop_name=webshop_name,
                    outlet_id=None,
                    outlet_code=None,
                    matched_field=None,
                    score=score,
                    status="unmatched",
                )
            )
            continue

        status = "matched" if score >= threshold_auto else "review"
        if status == "matched":
            previous_name = assigned_outlet_ids.get(outlet.id)
            if previous_name and previous_name != webshop_name:
                status = "review"
            else:
                assigned_outlet_ids[outlet.id] = webshop_name
                if not dry_run:
                    outlet.outlet_name_webshop = webshop_name

        results.append(
            MatchResult(
                brand_name=brand_name,
                webshop_name=webshop_name,
                outlet_id=outlet.id,
                outlet_code=outlet.outlet_code,
                matched_field=matched_field,
                score=score,
                status=status,
            )
        )

    if not dry_run:
        db.session.commit()

    return results
