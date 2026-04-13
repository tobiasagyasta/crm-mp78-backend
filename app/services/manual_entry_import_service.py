from __future__ import annotations

import calendar
import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import StringIO

from app.extensions import db
from app.models.expense_category import ExpenseCategory
from app.models.manual_entry import ManualEntry
from app.models.outlet import Outlet


EXPENSE_CATEGORY_COLUMNS = (
    "Admin Kantor",
    "Admin Gudang",
    "Sosmed MP78",
    "Sosmed 777",
    "Fee PIC",
)
CSV_COLUMN_TO_EXPENSE_CATEGORY = {
    "Admin Kantor": "Admin Kantor",
    "Admin Gudang": "Admin Gudang",
    "Sosmed MP78": "Sosmed",
    "Sosmed 777": "Sosmed",
    "Fee PIC": "Fee PIC",
}
DECIMAL_PLACES = Decimal("0.01")


@dataclass(frozen=True)
class ManualEntryImportPayload:
    outlet_code: str
    brand_name: str
    amount: Decimal
    description: str
    start_date: date
    end_date: date
    category_id: int


def _clean_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    return " ".join(_clean_str(value).lower().split())


def parse_uploaded_date(value: str | None) -> date:
    raw_value = _clean_str(value)
    if raw_value == "":
        return date.today()
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("uploaded_date must use YYYY-MM-DD format.") from exc


def _month_last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _month_bounds(target_date: date) -> tuple[date, date]:
    last_day = _month_last_day(target_date.year, target_date.month)
    return date(target_date.year, target_date.month, 1), date(
        target_date.year, target_date.month, last_day
    )


def _previous_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _parse_closing_range(closing_date: str | None) -> tuple[int, int] | None:
    raw_value = _clean_str(closing_date)
    if "-" not in raw_value:
        return None

    start_str, end_str = raw_value.split("-", 1)
    try:
        start_day = int(start_str)
        end_day = int(end_str)
    except ValueError:
        return None

    if not (1 <= start_day <= 31 and 1 <= end_day <= 31):
        return None

    return start_day, end_day


def resolve_manual_entry_date_range(
    closing_date: str | None, uploaded_date: date
) -> tuple[date, date, bool]:
    closing_range = _parse_closing_range(closing_date)
    if not closing_range:
        start_date, end_date = _month_bounds(uploaded_date)
        return start_date, end_date, True

    start_day, end_day = closing_range

    if start_day > end_day:
        start_year, start_month = _previous_month(uploaded_date.year, uploaded_date.month)
        start_date = date(
            start_year,
            start_month,
            min(start_day, _month_last_day(start_year, start_month)),
        )
        end_date = date(
            uploaded_date.year,
            uploaded_date.month,
            min(end_day, _month_last_day(uploaded_date.year, uploaded_date.month)),
        )
        return start_date, end_date, False

    start_date = date(
        uploaded_date.year,
        uploaded_date.month,
        min(start_day, _month_last_day(uploaded_date.year, uploaded_date.month)),
    )
    end_date = date(
        uploaded_date.year,
        uploaded_date.month,
        min(end_day, _month_last_day(uploaded_date.year, uploaded_date.month)),
    )
    return start_date, end_date, False


def _build_description(expense_category: str, start_date: date, end_date: date) -> str:
    return f"{expense_category} {start_date.isoformat()} to {end_date.isoformat()}"


def _parse_amount(value: str, category_name: str, row_number: int) -> Decimal | None:
    raw_value = _clean_str(value)
    if raw_value in {"", "-"}:
        return None

    normalized = raw_value.replace(",", "")
    try:
        amount = Decimal(normalized).quantize(DECIMAL_PLACES)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"Invalid amount '{raw_value}' for category '{category_name}' at CSV row {row_number}."
        ) from exc

    if amount == Decimal("0.00"):
        return None

    return amount


def _load_expense_categories() -> dict[str, ExpenseCategory]:
    categories = ExpenseCategory.query.all()
    return {_normalize_key(category.name): category for category in categories}


def _load_outlets(outlet_codes: set[str]) -> dict[str, Outlet]:
    if not outlet_codes:
        return {}

    outlets = Outlet.query.filter(Outlet.outlet_code.in_(sorted(outlet_codes))).all()
    return {outlet.outlet_code: outlet for outlet in outlets}


def _payload_key(payload: ManualEntryImportPayload) -> tuple[str, int, date, date, Decimal, str]:
    return (
        payload.outlet_code,
        payload.category_id,
        payload.start_date,
        payload.end_date,
        payload.amount,
        payload.description,
    )


def _entry_key(entry: ManualEntry) -> tuple[str, int, date, date, Decimal, str]:
    return (
        entry.outlet_code,
        entry.category_id,
        entry.start_date,
        entry.end_date,
        Decimal(str(entry.amount)).quantize(DECIMAL_PLACES),
        _clean_str(entry.description),
    )


def _load_existing_manual_entry_keys(
    payloads: list[ManualEntryImportPayload],
) -> set[tuple[str, int, date, date, Decimal, str]]:
    if not payloads:
        return set()

    outlet_codes = sorted({payload.outlet_code for payload in payloads})
    category_ids = sorted({payload.category_id for payload in payloads})
    min_start_date = min(payload.start_date for payload in payloads)
    max_end_date = max(payload.end_date for payload in payloads)

    existing_entries = (
        ManualEntry.query.filter(
            ManualEntry.entry_type == "expense",
            ManualEntry.outlet_code.in_(outlet_codes),
            ManualEntry.category_id.in_(category_ids),
            ManualEntry.start_date >= min_start_date,
            ManualEntry.end_date <= max_end_date,
        ).all()
    )

    return {_entry_key(entry) for entry in existing_entries}


def _read_adm_csv_rows(file_contents: str) -> list[dict[str, str]]:
    csv_file = StringIO(file_contents)
    reader = csv.DictReader(csv_file)
    if not reader.fieldnames:
        raise ValueError("ADM CSV is empty.")

    required_columns = {"Brand", "Outlet", "outlet_code", *EXPENSE_CATEGORY_COLUMNS}
    missing_columns = [
        column_name for column_name in required_columns if column_name not in reader.fieldnames
    ]
    if missing_columns:
        missing_str = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required CSV columns: {missing_str}")

    return list(reader)


def import_manual_entries_from_adm_csv_content(
    file_contents: str,
    uploaded_date: date,
    source_name: str | None = None,
) -> dict[str, object]:
    expense_categories = _load_expense_categories()

    missing_categories = [
        expense_category_name
        for expense_category_name in dict.fromkeys(CSV_COLUMN_TO_EXPENSE_CATEGORY.values())
        if _normalize_key(expense_category_name) not in expense_categories
    ]
    if missing_categories:
        missing_str = ", ".join(missing_categories)
        raise ValueError(f"Missing expense categories for CSV columns: {missing_str}")

    rows = _read_adm_csv_rows(file_contents)
    outlets = _load_outlets({_clean_str(row.get("outlet_code")) for row in rows})

    payloads: list[ManualEntryImportPayload] = []
    missing_outlet_codes: set[str] = set()
    skipped_missing_brand = 0
    fallback_calendar_ranges = 0
    processed_rows = 0

    for row_number, row in enumerate(rows, start=2):
        outlet_code = _clean_str(row.get("outlet_code"))
        if outlet_code == "":
            continue

        outlet = outlets.get(outlet_code)
        if outlet is None:
            missing_outlet_codes.add(outlet_code)
            continue

        processed_rows += 1
        brand_name = _clean_str(row.get("Brand")) or _clean_str(outlet.brand)
        if brand_name == "":
            skipped_missing_brand += 1
            continue

        start_date, end_date, used_fallback = resolve_manual_entry_date_range(
            outlet.closing_date, uploaded_date
        )
        if used_fallback:
            fallback_calendar_ranges += 1

        for category_name in EXPENSE_CATEGORY_COLUMNS:
            amount = _parse_amount(row.get(category_name, ""), category_name, row_number)
            if amount is None:
                continue

            expense_category_name = CSV_COLUMN_TO_EXPENSE_CATEGORY[category_name]
            expense_category = expense_categories[_normalize_key(expense_category_name)]
            payloads.append(
                ManualEntryImportPayload(
                    outlet_code=outlet_code,
                    brand_name=brand_name,
                    amount=amount,
                    description=_build_description(expense_category_name, start_date, end_date),
                    start_date=start_date,
                    end_date=end_date,
                    category_id=expense_category.id,
                )
            )

    existing_keys = _load_existing_manual_entry_keys(payloads)

    created_entries = 0
    skipped_duplicates = 0
    for payload in payloads:
        payload_key = _payload_key(payload)
        if payload_key in existing_keys:
            skipped_duplicates += 1
            continue

        db.session.add(
            ManualEntry(
                outlet_code=payload.outlet_code,
                brand_name=payload.brand_name,
                entry_type="expense",
                amount=payload.amount,
                description=payload.description,
                start_date=payload.start_date,
                end_date=payload.end_date,
                category_id=payload.category_id,
            )
        )
        existing_keys.add(payload_key)
        created_entries += 1

    db.session.commit()

    return {
        "status": "ok",
        "source_name": source_name or "uploaded_file",
        "uploaded_date": uploaded_date.isoformat(),
        "rows_in_csv": len(rows),
        "processed_rows": processed_rows,
        "created_entries": created_entries,
        "skipped_duplicates": skipped_duplicates,
        "skipped_missing_brand": skipped_missing_brand,
        "fallback_calendar_ranges": fallback_calendar_ranges,
        "missing_outlet_codes": sorted(missing_outlet_codes),
    }
