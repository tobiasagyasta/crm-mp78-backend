"""Build installment entries using the outlet's accounting periods."""

import calendar
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from app.models.manual_entry import ManualEntry
from app.models.outlet import Outlet
from app.services.manual_entry_import_service import _parse_closing_range


INSTALLMENT_CATEGORY_NAME = "Pembayaran Termin"
MAX_INSTALLMENTS = 600
MAX_ENTRY_AMOUNT = Decimal("9999999999999999.99")


def _month_offset(year, month, offset):
    index = year * 12 + month - 1 + offset
    new_year, month_index = divmod(index, 12)
    return new_year, month_index + 1


def _clamped_date(year, month, day):
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


def _period(closing_range, year, month):
    if closing_range is None:
        return date(year, month, 1), _clamped_date(year, month, 31)
    start_day, end_day = closing_range
    start = _clamped_date(year, month, start_day)
    end_year, end_month = _month_offset(year, month, int(start_day > end_day))
    return start, _clamped_date(end_year, end_month, end_day)


def build_installment_entries(data):
    """Return rows for the current accounting period and subsequent periods."""
    count = data.get("installment_count")
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_INSTALLMENTS:
        raise ValueError(f"installment_count must be an integer between 1 and {MAX_INSTALLMENTS}")
    try:
        amount = Decimal(str(data["amount"]))
        if not amount.is_finite() or amount <= 0 or amount != amount.quantize(Decimal("0.01")):
            raise ValueError
    except (InvalidOperation, ValueError):
        raise ValueError("amount must be a positive number with at most two decimal places")

    cents = int(amount * 100)
    base_cents, remainder = divmod(cents, count)
    if base_cents == 0:
        raise ValueError("amount must be at least 0.01 per installment")
    if Decimal(base_cents + bool(remainder)) / 100 > MAX_ENTRY_AMOUNT:
        raise ValueError("installment amount exceeds the supported maximum")

    start_date = date.today()
    outlet = Outlet.query.filter_by(outlet_code=data["outlet_code"]).first()
    if not outlet:
        raise ValueError("Outlet not found")

    closing_range = _parse_closing_range(outlet.closing_date)
    year, month = start_date.year, start_date.month
    try:
        first_start, first_end = _period(closing_range, year, month)
        if closing_range and closing_range[0] > closing_range[1] and start_date < first_start:
            year, month = _month_offset(year, month, -1)
            first_start, first_end = _period(closing_range, year, month)
    except (OverflowError, ValueError) as exc:
        raise ValueError("installment schedule exceeds the supported date range") from exc
    if not first_start <= start_date <= first_end:
        raise ValueError("Today is outside the outlet's configured closing periods")
    if year * 12 + month - 1 + count - 1 >= 10000 * 12:
        raise ValueError("installment schedule exceeds the supported date range")

    group_id = str(uuid4())
    entries = []
    try:
        for index in range(count):
            period_year, period_month = _month_offset(year, month, index)
            period_start, period_end = _period(closing_range, period_year, period_month)
            entries.append(ManualEntry(
                outlet_code=data["outlet_code"],
                brand_name=data["brand_name"],
                entry_type="expense",
                amount=Decimal(base_cents + (index < remainder)) / 100,
                description=data.get("description"),
                start_date=period_start,
                end_date=period_end,
                category_id=data["category_id"],
                installment_group_id=group_id,
                installment_number=index + 1,
                installment_count=count,
            ))
    except (OverflowError, ValueError) as exc:
        raise ValueError("installment schedule exceeds the supported date range") from exc
    return entries
