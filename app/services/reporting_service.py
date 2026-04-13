from collections import defaultdict
from datetime import date

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.outlet import Outlet
from app.models.shopee_reports import ShopeeReport


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


def _resolve_financial_period(
    transaction_date: date,
    closing_date_str: str | None,
) -> tuple[int, int]:
    opening_day = _parse_opening_day(closing_date_str)
    financial_year = transaction_date.year
    financial_month = transaction_date.month

    if opening_day and transaction_date.day < opening_day:
        financial_month -= 1
        if financial_month == 0:
            financial_month = 12
            financial_year -= 1

    return financial_year, financial_month


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
        financial_year, financial_month = _resolve_financial_period(
            transaction_date,
            outlet.closing_date,
        )

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


def generate_monthly_mpr_commission_data(
    year: int,
    start_date: date | None = None,
    end_date: date | None = None,
    commission_rate: float = 0.08,
) -> dict:
    """
    Generates aggregated monthly MPR commission data across all MPR outlets.

    Data is sourced directly from the Gojek, Grab, and Shopee platform report tables
    by filtering rows where brand_name == 'MPR' and grouping by calendar month.
    """
    outlets = Outlet.query.filter_by(brand='MPR', status='Active').all()
    outlet_name_map = {
        outlet.outlet_code: (outlet.outlet_name_gojek or outlet.outlet_name_grab or outlet.outlet_code)
        for outlet in outlets
    }

    range_mode = start_date is not None and end_date is not None
    periods_set: set[tuple[int, int]] | None = None
    if range_mode:
        periods_set = set(_month_range(start_date, end_date))

    data = {}
    for outlet_code, outlet_name in outlet_name_map.items():
        data[outlet_code] = {
            'name': outlet_name,
            'monthly_totals': {} if range_mode else {
                i: {'net_total': 0, 'commission_total': 0} for i in range(1, 13)
            },
            'total_commission': 0,
        }

    def _ensure_outlet_data(outlet_code: str) -> dict:
        if outlet_code not in data:
            data[outlet_code] = {
                'name': outlet_name_map.get(outlet_code, outlet_code),
                'monthly_totals': {} if range_mode else {
                    i: {'net_total': 0, 'commission_total': 0} for i in range(1, 13)
                },
                'total_commission': 0,
            }
        return data[outlet_code]

    def _accumulate(outlet_code: str, transaction_date: date, amount: float):
        outlet_data = _ensure_outlet_data(outlet_code)
        if range_mode:
            period_key = (transaction_date.year, transaction_date.month)
            if periods_set is not None:
                periods_set.add(period_key)
            period_totals = outlet_data['monthly_totals'].setdefault(
                period_key,
                {'net_total': 0, 'commission_total': 0},
            )
            period_totals['net_total'] += amount
            return

        if transaction_date.year != year or not (1 <= transaction_date.month <= 12):
            return
        outlet_data['monthly_totals'][transaction_date.month]['net_total'] += amount

    gojek_query = GojekReport.query.filter(GojekReport.brand_name == 'MPR')
    grab_query = GrabFoodReport.query.filter(GrabFoodReport.brand_name == 'MPR')
    shopee_query = ShopeeReport.query.filter(ShopeeReport.brand_name == 'MPR')

    if range_mode:
        gojek_query = gojek_query.filter(
            GojekReport.transaction_date >= start_date,
            GojekReport.transaction_date <= end_date,
        )
        grab_query = grab_query.filter(
            db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) >= start_date,
            db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) <= end_date,
        )
        shopee_query = shopee_query.filter(
            db.func.cast(ShopeeReport.order_create_time, db.Date) >= start_date,
            db.func.cast(ShopeeReport.order_create_time, db.Date) <= end_date,
        )
    else:
        query_start_date = date(year, 1, 1)
        query_end_date = date(year, 12, 31)
        gojek_query = gojek_query.filter(
            GojekReport.transaction_date >= query_start_date,
            GojekReport.transaction_date <= query_end_date,
        )
        grab_query = grab_query.filter(
            db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) >= query_start_date,
            db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) <= query_end_date,
        )
        shopee_query = shopee_query.filter(
            db.func.cast(ShopeeReport.order_create_time, db.Date) >= query_start_date,
            db.func.cast(ShopeeReport.order_create_time, db.Date) <= query_end_date,
        )

    for report in gojek_query.all():
        _accumulate(report.outlet_code, report.transaction_date, float(report.nett_amount or 0))

    for report in grab_query.all():
        if not report.tanggal_dibuat:
            continue
        _accumulate(report.outlet_code, report.tanggal_dibuat.date(), float(report.total or 0))

    for report in shopee_query.all():
        if report.order_status == "Cancelled" or not report.order_create_time:
            continue
        _accumulate(report.outlet_code, report.order_create_time.date(), float(report.net_income or 0))

    if range_mode and periods_set is not None:
        periods = sorted(periods_set)
        for outlet_data in data.values():
            for period in periods:
                outlet_data['monthly_totals'].setdefault(
                    period,
                    {'net_total': 0, 'commission_total': 0},
                )
            total_commission = 0
            for period in periods:
                period_totals = outlet_data['monthly_totals'][period]
                period_totals['commission_total'] = (
                    period_totals['net_total'] * commission_rate
                )
                total_commission += period_totals['commission_total']
            outlet_data['total_commission'] = total_commission

        return {
            'periods': periods,
            'outlets': data,
            'commission_rate': commission_rate,
        }

    for outlet_data in data.values():
        total_commission = 0
        for month in range(1, 13):
            period_totals = outlet_data['monthly_totals'][month]
            period_totals['commission_total'] = period_totals['net_total'] * commission_rate
            total_commission += period_totals['commission_total']
        outlet_data['total_commission'] = total_commission

    return data
