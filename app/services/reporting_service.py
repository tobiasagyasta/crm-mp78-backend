from collections import defaultdict
from datetime import date, timedelta

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.outlet import Outlet
from app.models.shopee_reports import ShopeeReport
from app.models.shopeepay_reports import ShopeepayReport
from app.services.excel_export import mpr_calculations as mpr_calc


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


def _next_month(period_year: int, period_month: int) -> tuple[int, int]:
    if period_month == 12:
        return period_year + 1, 1
    return period_year, period_month + 1


def _build_period_window(
    period_year: int,
    period_month: int,
    closing_date_str: str | None,
) -> tuple[date, date]:
    opening_day = _parse_opening_day(closing_date_str)
    if not opening_day:
        period_start = date(period_year, period_month, 1)
        next_year, next_month = _next_month(period_year, period_month)
        period_end = date(next_year, next_month, 1) - timedelta(days=1)
        return period_start, period_end

    next_year, next_month = _next_month(period_year, period_month)
    period_start = date(period_year, period_month, 1) + timedelta(days=opening_day - 1)
    period_end = date(next_year, next_month, 1) + timedelta(days=opening_day - 2)
    return period_start, period_end


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


def generate_monthly_net_income_data_from_closing_anchor(
    brand_name: str,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Generates monthly Grab net total data for calendar months between ``start_date``
    and ``end_date``, but expands each month into the outlet's financial closing
    window.
    """
    outlets = Outlet.query.filter_by(brand=brand_name, status='Active').all()
    if not outlets:
        return {}

    periods = _month_range(start_date, end_date)
    period_set = set(periods)
    outlet_map = {str(outlet.outlet_code): outlet for outlet in outlets}

    query_start_date: date | None = None
    query_end_date: date | None = None
    for outlet in outlets:
        first_period_start, _ = _build_period_window(
            periods[0][0],
            periods[0][1],
            outlet.closing_date,
        )
        _, last_period_end = _build_period_window(
            periods[-1][0],
            periods[-1][1],
            outlet.closing_date,
        )
        if query_start_date is None or first_period_start < query_start_date:
            query_start_date = first_period_start
        if query_end_date is None or last_period_end > query_end_date:
            query_end_date = last_period_end

    grab_reports = GrabFoodReport.query.filter(
        GrabFoodReport.brand_name == brand_name,
        db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) >= query_start_date,
        db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) <= query_end_date,
    ).all()

    data = {}
    for outlet in outlets:
        opening_day = _parse_opening_day(outlet.closing_date)
        closing_day_display = outlet.closing_date if opening_day else 'Calendar'
        data[outlet.outlet_code] = {
            'name': outlet.outlet_name_gojek,
            'closing_day': closing_day_display,
            'monthly_totals': {period: 0 for period in periods},
            'total': 0,
        }

    for report in grab_reports:
        if not report.tanggal_dibuat:
            continue

        outlet = outlet_map.get(str(report.outlet_code))
        if not outlet:
            continue

        financial_year, financial_month = _resolve_financial_period(
            report.tanggal_dibuat.date(),
            outlet.closing_date,
        )
        period_key = (financial_year, financial_month)
        if period_key not in period_set:
            continue

        net_total = float(report.total or 0)
        data[outlet.outlet_code]['monthly_totals'][period_key] += net_total
        data[outlet.outlet_code]['total'] += net_total

    return {
        'periods': periods,
        'outlets': data,
    }


def generate_monthly_grab_net_income_data(
    brand_name: str,
    year: int,
) -> dict:
    """
    Generates aggregated monthly Grab net totals for all active outlets of a brand.

    Financial month grouping follows each outlet's ``closing_date`` and falls back
    to regular calendar months when the closing range is not configured.
    """
    outlets = Outlet.query.filter_by(brand=brand_name, status='Active').all()
    if not outlets:
        return {}

    outlet_map = {outlet.outlet_code: outlet for outlet in outlets}
    data = {}
    for outlet in outlets:
        opening_day = _parse_opening_day(outlet.closing_date)
        closing_day_display = outlet.closing_date if opening_day else 'Calendar'
        data[outlet.outlet_code] = {
            'name': outlet.outlet_name_gojek or outlet.outlet_name_grab or outlet.outlet_code,
            'closing_day': closing_day_display,
            'monthly_totals': {i: 0 for i in range(1, 13)},
            'total': 0,
        }

    query_start_date = date(year, 1, 1)
    query_end_date = date(year + 1, 1, 31)
    grab_reports = GrabFoodReport.query.filter(
        GrabFoodReport.brand_name == brand_name,
        db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) >= query_start_date,
        db.func.cast(GrabFoodReport.tanggal_dibuat, db.Date) <= query_end_date,
    ).all()

    for report in grab_reports:
        if not report.tanggal_dibuat:
            continue

        outlet = outlet_map.get(report.outlet_code)
        if not outlet:
            continue

        transaction_date = report.tanggal_dibuat.date()
        financial_year, financial_month = _resolve_financial_period(
            transaction_date,
            outlet.closing_date,
        )
        if financial_year != year or not (1 <= financial_month <= 12):
            continue

        net_total = float(report.total or 0)
        data[outlet.outlet_code]['monthly_totals'][financial_month] += net_total
        data[outlet.outlet_code]['total'] += net_total

    return data


def generate_monthly_mpr_commission_data(
    year: int,
    start_date: date | None = None,
    end_date: date | None = None,
    commission_rate: float = 0.08,
) -> dict:
    """
    Generates aggregated monthly MPR commission data across all MPR outlets.

    Data is sourced directly from the Gojek, Grab, Shopee, and ShopeePay platform report tables
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

    def _accumulate(outlet_code: str, transaction_date: date, amount: float, commission: float):
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
            period_totals['commission_total'] += commission
            return

        if transaction_date.year != year or not (1 <= transaction_date.month <= 12):
            return
        outlet_data['monthly_totals'][transaction_date.month]['net_total'] += amount
        outlet_data['monthly_totals'][transaction_date.month]['commission_total'] += commission

    def _standard_commission(amount: float) -> float:
        return amount * (1 - mpr_calc.MPR_STANDARD_NET_RATE)

    def _qris_ovo_commission(amount: float) -> float:
        return amount * (1 - mpr_calc.MPR_QRIS_OVO_NET_RATE)

    gojek_query = GojekReport.query.filter(GojekReport.brand_name == 'MPR')
    grab_query = GrabFoodReport.query.filter(GrabFoodReport.brand_name == 'MPR')
    shopee_query = ShopeeReport.query.filter(ShopeeReport.brand_name == 'MPR')
    shopeepay_query = ShopeepayReport.query.filter(ShopeepayReport.brand_name == 'MPR')

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
        shopeepay_query = shopeepay_query.filter(
            db.func.cast(ShopeepayReport.create_time, db.Date) >= start_date,
            db.func.cast(ShopeepayReport.create_time, db.Date) <= end_date,
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
        shopeepay_query = shopeepay_query.filter(
            db.func.cast(ShopeepayReport.create_time, db.Date) >= query_start_date,
            db.func.cast(ShopeepayReport.create_time, db.Date) <= query_end_date,
        )

    for report in gojek_query.all():
        amount = float(report.nett_amount or 0)
        commission = (
            _qris_ovo_commission(amount)
            if report.payment_type == 'QRIS'
            else _standard_commission(amount)
        )
        _accumulate(report.outlet_code, report.transaction_date, amount, commission)

    for report in grab_query.all():
        if not report.tanggal_dibuat:
            continue
        amount = float(report.total or 0)
        commission = (
            _qris_ovo_commission(amount)
            if getattr(report, 'jenis', None) == 'OVO'
            else _standard_commission(amount)
        )
        _accumulate(report.outlet_code, report.tanggal_dibuat.date(), amount, commission)

    for report in shopee_query.all():
        if report.order_status == "Cancelled" or not report.order_create_time:
            continue
        amount = float(report.net_income or 0)
        _accumulate(
            report.outlet_code,
            report.order_create_time.date(),
            amount,
            _standard_commission(amount),
        )

    for report in shopeepay_query.all():
        if report.transaction_type == "Withdrawal" or not report.create_time:
            continue
        amount = float(report.settlement_amount or 0)
        _accumulate(
            report.outlet_code,
            report.create_time.date(),
            amount,
            _qris_ovo_commission(amount),
        )

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
                total_commission += period_totals['commission_total']
            outlet_data['total_commission'] = total_commission

        return {
            'periods': periods,
            'outlets': data,
            'commission_rate': commission_rate,
            'commission_rates': {
                'standard': 1 - mpr_calc.MPR_STANDARD_NET_RATE,
                'qris_ovo': 1 - mpr_calc.MPR_QRIS_OVO_NET_RATE,
            },
        }

    for outlet_data in data.values():
        total_commission = 0
        for month in range(1, 13):
            period_totals = outlet_data['monthly_totals'][month]
            total_commission += period_totals['commission_total']
        outlet_data['total_commission'] = total_commission

    return data


def generate_monthly_management_commission_data(
    brand_name: str,
    year: int,
    start_date: date | None = None,
    end_date: date | None = None,
    commission_divisor: float = 74,
) -> dict:
    """
    Generates monthly management commission data for a non-MPR brand.

    Net totals follow each outlet's financial month based on ``closing_date`` and
    fall back to calendar months when no valid closing range is configured.
    """
    normalized_brand_name = (brand_name or "").strip()
    if not normalized_brand_name or normalized_brand_name.upper() == "MPR":
        return {}

    if start_date is None and end_date is None:
        monthly_income_data = generate_monthly_grab_net_income_data(
            normalized_brand_name,
            year,
        )
    else:
        monthly_income_data = generate_monthly_net_income_data_from_closing_anchor(
            normalized_brand_name,
            start_date,
            end_date,
        )
    if not monthly_income_data:
        return {}

    if isinstance(monthly_income_data, dict) and "periods" in monthly_income_data:
        periods = monthly_income_data.get("periods", [])
        outlets = monthly_income_data.get("outlets", {})
    else:
        periods = list(range(1, 13))
        outlets = monthly_income_data

    transformed_outlets = {}
    for outlet_code, outlet_data in outlets.items():
        transformed_monthly_totals = {}
        total_commission = 0
        total_after_commission = 0

        for period in periods:
            raw_net_total = outlet_data.get("monthly_totals", {}).get(period, 0)
            net_total = float(raw_net_total or 0)
            commission_total = net_total / commission_divisor if commission_divisor else 0
            net_after_commission = net_total - commission_total
            transformed_monthly_totals[period] = {
                "net_total": net_total,
                "commission_total": commission_total,
                "net_after_commission": net_after_commission,
            }
            total_commission += commission_total
            total_after_commission += net_after_commission

        transformed_outlets[outlet_code] = {
            "name": outlet_data.get("name", outlet_code),
            "closing_day": outlet_data.get("closing_day", "Calendar"),
            "monthly_totals": transformed_monthly_totals,
            "total_commission": total_commission,
            "total_after_commission": total_after_commission,
        }

    return {
        "brand_name": normalized_brand_name,
        "periods": periods,
        "outlets": transformed_outlets,
        "commission_divisor": commission_divisor,
        "commission_rate": (1 / commission_divisor) if commission_divisor else 0,
    }
