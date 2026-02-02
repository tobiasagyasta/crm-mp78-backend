from datetime import date

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.outlet import Outlet


def _parse_opening_day(closing_date_str: str) -> int | None:
    """Parses an opening day from a string like '25-24'."""
    if not closing_date_str or '-' not in closing_date_str:
        return None
    try:
        start_str = closing_date_str.split('-')[0]
        start_day = int(start_str)
        if 1 <= start_day <= 31:
            return start_day
    except (ValueError, TypeError, IndexError):
        return None
    return None


def generate_monthly_net_income_data(
    brand_name: str,
    year: int,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    """
    Generates aggregated monthly net income data for all active outlets of a specific brand,
    handling custom financial month ranges.
    """
    outlets = Outlet.query.filter_by(brand=brand_name, status='Active').all()
    if not outlets:
        return {}

    def _month_range(start: date, end: date) -> list[tuple[int, int]]:
        periods: list[tuple[int, int]] = []
        year_val = start.year
        month_val = start.month
        while (year_val, month_val) <= (end.year, end.month):
            periods.append((year_val, month_val))
            if month_val == 12:
                year_val += 1
                month_val = 1
            else:
                month_val += 1
        return periods

    outlet_ids = [outlet.outlet_code for outlet in outlets]

    query = db.session.query(DailyMerchantTotal).filter(
        # outlet_id column is varchar in DB, so compare with strings
        DailyMerchantTotal.outlet_id.in_([str(i) for i in outlet_ids]),
    )

    range_mode = start_date is not None and end_date is not None
    if range_mode:
        query = query.filter(
            DailyMerchantTotal.date >= start_date,
            DailyMerchantTotal.date <= end_date,
        )
    else:
        # To get all transactions for the financial year `year`, we need to query
        # calendar dates from the beginning of `year` up to the end of January of `year + 1`.
        # This ensures we capture transactions that belong to the December financial month,
        # which can extend into January of the next calendar year.
        query_start_date = date(year, 1, 1)
        query_end_date = date(year + 1, 1, 31)
        query = query.filter(
            DailyMerchantTotal.date >= query_start_date,
            DailyMerchantTotal.date <= query_end_date,
        )

    daily_totals = query.all()

    data = {}
    periods_set: set[tuple[int, int]] | None = None
    if range_mode:
        periods_set = set(_month_range(start_date, end_date))

    for outlet in outlets:
        opening_day = _parse_opening_day(outlet.closing_date)
        closing_day_display = outlet.closing_date if opening_day else 'Calendar'
        data[outlet.outlet_code] = {
            'name': outlet.outlet_name_gojek,
            'closing_day': closing_day_display,
            'monthly_totals': {} if range_mode else {i: 0 for i in range(1, 13)},
            'total': 0,
        }

    for daily_total in daily_totals:
        outlet = next((o for o in outlets if o.outlet_code == daily_total.outlet_id), None)
        if not outlet:
            continue

        transaction_date = daily_total.date
        opening_day = _parse_opening_day(outlet.closing_date)

        financial_year = transaction_date.year
        financial_month = transaction_date.month

        if opening_day:
            if transaction_date.day < opening_day:
                # This transaction belongs to the financial period that started in the previous calendar month.
                financial_month -= 1
                if financial_month == 0:
                    financial_month = 12
                    financial_year -= 1

        if range_mode:
            period_key = (financial_year, financial_month)
            if periods_set is not None:
                periods_set.add(period_key)
            outlet_totals = data[outlet.outlet_code]['monthly_totals']
            outlet_totals[period_key] = outlet_totals.get(period_key, 0) + daily_total.total_net
            data[outlet.outlet_code]['total'] += daily_total.total_net
        else:
            # Only include totals for the requested financial year.
            if financial_year == year:
                if 1 <= financial_month <= 12:
                    data[outlet.outlet_code]['monthly_totals'][financial_month] += daily_total.total_net
                    data[outlet.outlet_code]['total'] += daily_total.total_net

    if range_mode and periods_set is not None:
        periods = sorted(periods_set)
        for outlet_data in data.values():
            for period in periods:
                outlet_data['monthly_totals'].setdefault(period, 0)
        return {
            'periods': periods,
            'outlets': data,
        }

    return data
