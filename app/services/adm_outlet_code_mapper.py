from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable


CSV_HEADER_TO_FIELD = {
    "brand": "brand_name",
    "outlet": "outlet_name",
}
DEFAULT_COLUMN_INDEXES = {
    "brand_name": 0,
    "outlet_name": 1,
}
OUTLET_MATCH_FIELDS = (
    "outlet_name_gojek",
    "outlet_name_grab",
    "outlet_name_webshop",
    "outlet_name_qpon",
    "area",
    "outlet_code",
)
FIELD_BONUSES = {
    "outlet_name_gojek": 0.030,
    "outlet_name_grab": 0.028,
    "outlet_name_webshop": 0.024,
    "outlet_name_qpon": 0.020,
    "area": 0.015,
    "outlet_code": 0.010,
}
GENERIC_BRAND_TOKENS = {"martabak"}
GENERIC_LOCATION_PREFIX_TOKENS = {"martabak", "outlet", "store", "cabang", "gerai", "kedai"}


@dataclass(frozen=True)
class AdmCsvRow:
    row_number: int
    brand_name: str | None
    outlet_name: str
    raw_row: dict[str, str]


@dataclass(frozen=True)
class OutletCandidate:
    id: int | None
    outlet_code: str | None
    brand: str | None
    status: str | None = None
    area: str | None = None
    outlet_name_gojek: str | None = None
    outlet_name_grab: str | None = None
    outlet_name_webshop: str | None = None
    outlet_name_qpon: str | None = None


@dataclass(frozen=True)
class AdmOutletCodeMatchResult:
    brand_name: str | None
    outlet_name: str
    outlet_id: int | None
    outlet_code: str | None
    matched_brand: str | None
    matched_outlet_name: str | None
    matched_field: str | None
    matched_value: str | None
    brand_score: float | None
    name_score: float | None
    score: float | None
    status: str  # "matched" | "review" | "unmatched"


def clean_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_for_match(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", clean_str(value).lower())
    return " ".join(cleaned.split())


def _normalize_header(header: str) -> str:
    return " ".join(clean_str(header).replace("\ufeff", "").lower().split())


def _resolve_csv_path(csv_path: str) -> Path:
    raw_path = clean_str(csv_path)
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
        f"ADM CSV not found: '{csv_path}'. Checked: {', '.join(checked_paths)}"
    )


def _resolve_columns(headers: list[str] | None) -> dict[str, int]:
    resolved: dict[str, int] = {}
    if headers:
        for idx, header in enumerate(headers):
            field_name = CSV_HEADER_TO_FIELD.get(_normalize_header(header))
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
    return bool(normalized.intersection(CSV_HEADER_TO_FIELD.keys()))


def parse_adm_csv_rows(csv_path: str) -> tuple[list[str], list[AdmCsvRow]]:
    resolved_csv_path = _resolve_csv_path(csv_path)
    rows: list[AdmCsvRow] = []

    with resolved_csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file)
        first_row = next(reader, None)
        if not first_row:
            return [], rows

        if _looks_like_header(first_row):
            headers = [clean_str(header) for header in first_row]
            columns = _resolve_columns(headers)
            start_row_number = 2
        else:
            headers = [f"column_{index + 1}" for index in range(len(first_row))]
            columns = DEFAULT_COLUMN_INDEXES.copy()
            reader = iter([first_row, *reader])
            start_row_number = 1

        for row_number, raw_values in enumerate(reader, start=start_row_number):
            if not raw_values or all(clean_str(value) == "" for value in raw_values):
                continue

            row_map = {
                headers[index]: clean_str(raw_values[index]) if index < len(raw_values) else ""
                for index in range(len(headers))
            }

            def _value(field: str) -> str:
                idx = columns.get(field)
                if idx is None or idx < 0 or idx >= len(raw_values):
                    return ""
                return clean_str(raw_values[idx]).strip('"').strip("'")

            outlet_name = _value("outlet_name")
            if outlet_name == "":
                continue

            rows.append(
                AdmCsvRow(
                    row_number=row_number,
                    brand_name=_value("brand_name") or None,
                    outlet_name=outlet_name,
                    raw_row=row_map,
                )
            )

    return headers, rows


def _score_match(target: str, candidate: str) -> float:
    if not target or not candidate:
        return 0.0
    if target == candidate:
        return 1.0

    score = SequenceMatcher(None, target, candidate).ratio()

    if target in candidate or candidate in target:
        score = max(score, 0.90)

    target_tokens = set(target.split())
    candidate_tokens = set(candidate.split())
    if target_tokens and candidate_tokens:
        overlap = target_tokens.intersection(candidate_tokens)
        if overlap:
            jaccard = len(overlap) / len(target_tokens.union(candidate_tokens))
            coverage = len(overlap) / len(target_tokens)
            score = max(score, jaccard * 0.96, coverage * 0.94)
            if target_tokens.issubset(candidate_tokens) or candidate_tokens.issubset(target_tokens):
                score = max(score, 0.92)

    return min(1.0, score)


def _build_brand_aliases(brand_name: str | None) -> set[str]:
    normalized = normalize_for_match(brand_name or "")
    if normalized == "":
        return set()

    tokens = normalized.split()
    aliases = {normalized, "".join(tokens)}

    compact_tokens = [token for token in tokens if token not in GENERIC_BRAND_TOKENS]
    if compact_tokens:
        aliases.add(" ".join(compact_tokens))
        aliases.add("".join(compact_tokens))

    return {alias for alias in aliases if alias}


def _score_brand_match(source_brand: str | None, outlet_brand: str | None) -> float:
    source_aliases = _build_brand_aliases(source_brand)
    outlet_aliases = _build_brand_aliases(outlet_brand)
    if not source_aliases or not outlet_aliases:
        return 0.0

    best_score = 0.0
    for source_alias in source_aliases:
        for outlet_alias in outlet_aliases:
            best_score = max(best_score, _score_match(source_alias, outlet_alias))

    return best_score


def _strip_leading_tokens(text: str, tokens_to_strip: Iterable[str]) -> str:
    stripped = normalize_for_match(text)
    if stripped == "":
        return ""

    current_tokens = stripped.split()
    sorted_groups = sorted(tokens_to_strip, key=lambda value: len(value.split()), reverse=True)

    changed = True
    while changed and current_tokens:
        changed = False

        while current_tokens and current_tokens[0] in GENERIC_LOCATION_PREFIX_TOKENS:
            current_tokens = current_tokens[1:]
            changed = True

        for token_group in sorted_groups:
            group_tokens = token_group.split()
            if group_tokens and current_tokens[: len(group_tokens)] == group_tokens:
                current_tokens = current_tokens[len(group_tokens) :]
                changed = True
                break

    return " ".join(current_tokens).strip()


def _build_location_aliases(value: str | None, brand_name: str | None) -> list[str]:
    raw_value = clean_str(value).strip('"').strip("'")
    if raw_value == "":
        return []

    candidates = [
        raw_value,
        raw_value.replace("_", " "),
    ]
    brand_aliases = _build_brand_aliases(brand_name)
    aliases: list[str] = []
    seen: set[str] = set()

    for candidate in candidates:
        normalized = normalize_for_match(candidate)
        if normalized and normalized not in seen:
            aliases.append(normalized)
            seen.add(normalized)

        stripped = _strip_leading_tokens(candidate, brand_aliases)
        if stripped and stripped not in seen:
            aliases.append(stripped)
            seen.add(stripped)

    return aliases


def _iter_outlet_aliases(outlet: OutletCandidate) -> list[tuple[str, str, str]]:
    aliases: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    for field in OUTLET_MATCH_FIELDS:
        raw_value = clean_str(getattr(outlet, field, None))
        if raw_value == "":
            continue

        for alias in _build_location_aliases(raw_value, outlet.brand):
            key = (field, alias)
            if key in seen:
                continue
            seen.add(key)
            aliases.append((field, raw_value, alias))

    return aliases


def _outlet_display_name(outlet: OutletCandidate) -> str | None:
    for field in ("outlet_name_gojek", "outlet_name_grab", "outlet_name_webshop", "outlet_name_qpon", "area"):
        value = clean_str(getattr(outlet, field, None))
        if value:
            return value
    return clean_str(outlet.outlet_code) or None


def _select_brand_candidates(
    brand_name: str | None, outlets: list[OutletCandidate]
) -> list[tuple[OutletCandidate, float]]:
    if clean_str(brand_name) == "":
        return [(outlet, 0.0) for outlet in outlets]

    scored = [(outlet, _score_brand_match(brand_name, outlet.brand)) for outlet in outlets]
    for threshold in (0.95, 0.80, 0.60):
        selected = [(outlet, score) for outlet, score in scored if score >= threshold]
        if selected:
            return selected

    return scored


def find_best_outlet_for_adm_row(
    row: AdmCsvRow,
    outlets: list[OutletCandidate],
) -> AdmOutletCodeMatchResult:
    target_name = normalize_for_match(row.outlet_name)
    if target_name == "":
        return AdmOutletCodeMatchResult(
            brand_name=row.brand_name,
            outlet_name=row.outlet_name,
            outlet_id=None,
            outlet_code=None,
            matched_brand=None,
            matched_outlet_name=None,
            matched_field=None,
            matched_value=None,
            brand_score=None,
            name_score=None,
            score=None,
            status="unmatched",
        )

    best_outlet: OutletCandidate | None = None
    best_field: str | None = None
    best_value: str | None = None
    best_brand_score = 0.0
    best_name_score = 0.0
    best_score = 0.0

    for outlet, brand_score in _select_brand_candidates(row.brand_name, outlets):
        for field, raw_value, alias in _iter_outlet_aliases(outlet):
            name_score = _score_match(target_name, alias)
            if name_score <= 0.0:
                continue

            combined = name_score
            if clean_str(row.brand_name):
                combined = (name_score * 0.86) + (brand_score * 0.14)
            combined += FIELD_BONUSES.get(field, 0.0)
            if clean_str(outlet.status).lower() == "active":
                combined += 0.01

            combined = min(combined, 1.0)

            if combined > best_score:
                best_outlet = outlet
                best_field = field
                best_value = raw_value
                best_brand_score = brand_score
                best_name_score = name_score
                best_score = combined

    status = "unmatched"
    if best_score >= 0.84:
        status = "matched"
    elif best_score >= 0.65:
        status = "review"

    return AdmOutletCodeMatchResult(
        brand_name=row.brand_name,
        outlet_name=row.outlet_name,
        outlet_id=best_outlet.id if best_outlet else None,
        outlet_code=best_outlet.outlet_code if best_outlet else None,
        matched_brand=best_outlet.brand if best_outlet else None,
        matched_outlet_name=_outlet_display_name(best_outlet) if best_outlet else None,
        matched_field=best_field,
        matched_value=best_value,
        brand_score=best_brand_score if best_outlet else None,
        name_score=best_name_score if best_outlet else None,
        score=best_score if best_outlet else None,
        status=status,
    )


def match_adm_rows(
    rows: list[AdmCsvRow],
    outlets: list[OutletCandidate],
) -> list[AdmOutletCodeMatchResult]:
    return [find_best_outlet_for_adm_row(row, outlets) for row in rows]


def load_outlet_candidates() -> list[OutletCandidate]:
    from app.models.outlet import Outlet

    outlets = Outlet.query.filter(Outlet.outlet_code.isnot(None)).all()
    return [
        OutletCandidate(
            id=outlet.id,
            outlet_code=outlet.outlet_code,
            brand=outlet.brand,
            status=outlet.status,
            area=outlet.area,
            outlet_name_gojek=getattr(outlet, "outlet_name_gojek", None),
            outlet_name_grab=getattr(outlet, "outlet_name_grab", None),
            outlet_name_webshop=getattr(outlet, "outlet_name_webshop", None),
            outlet_name_qpon=getattr(outlet, "outlet_name_qpon", None),
        )
        for outlet in outlets
    ]


def map_adm_outlet_codes(
    csv_path: str,
) -> tuple[list[str], list[AdmCsvRow], list[AdmOutletCodeMatchResult]]:
    headers, rows = parse_adm_csv_rows(csv_path)
    outlets = load_outlet_candidates()
    results = match_adm_rows(rows, outlets)
    return headers, rows, results
