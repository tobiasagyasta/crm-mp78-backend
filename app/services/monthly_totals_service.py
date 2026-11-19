from datetime import date, datetime

from sqlalchemy import func

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.monthly_totals import MonthlyTotal
from app.models.outlet import Outlet
from app.services.consolidation_service import REBUILD_PLATFORMS


def next_month(month):
    return date(month.year + (month.month == 12), month.month % 12 + 1, 1)


def rebuild_monthly_totals(start_date, end_date, platforms=REBUILD_PLATFORMS, progress=None):
    """Rebuild whole calendar months intersecting the range from daily totals.

    The caller owns the transaction. Rows with no remaining daily totals are
    zeroed, so reruns also repair stale values.
    """
    if not isinstance(start_date, date) or not isinstance(end_date, date) or start_date > end_date:
        raise ValueError('Invalid date range')
    platforms = tuple(dict.fromkeys(platforms))
    if not platforms or any(platform not in REBUILD_PLATFORMS for platform in platforms):
        raise ValueError('Unsupported rebuild platform')

    month = start_date.replace(day=1)
    last_month = end_date.replace(day=1)
    counts = {platform: 0 for platform in platforms}
    while month <= last_month:
        end = next_month(month)
        rows = (db.session.query(
            DailyMerchantTotal.outlet_id, DailyMerchantTotal.report_type,
            Outlet.brand, func.sum(DailyMerchantTotal.total_gross),
            func.sum(DailyMerchantTotal.total_net),
        ).outerjoin(Outlet, Outlet.outlet_code == DailyMerchantTotal.outlet_id)
            .filter(DailyMerchantTotal.date >= month, DailyMerchantTotal.date < end,
                    DailyMerchantTotal.report_type.in_(platforms))
            .group_by(DailyMerchantTotal.outlet_id, DailyMerchantTotal.report_type, Outlet.brand)
            .all())
        existing = {
            (row.outlet_id, row.report_type): row
            for row in MonthlyTotal.query.filter(
                MonthlyTotal.month == month, MonthlyTotal.report_type.in_(platforms)
            ).all()
        }
        seen = set()
        for outlet_id, platform, brand, gross, net in rows:
            key = (outlet_id, platform)
            seen.add(key)
            monthly = existing.get(key)
            if monthly is None:
                monthly = MonthlyTotal(outlet_id=outlet_id, month=month, report_type=platform)
                db.session.add(monthly)
            monthly.brand = brand
            monthly.total_gross = gross or 0
            monthly.total_net = net or 0
            monthly.updated_at = datetime.utcnow()
            counts[platform] += 1
        for key, monthly in existing.items():
            if key not in seen:
                monthly.total_gross = 0
                monthly.total_net = 0
                monthly.updated_at = datetime.utcnow()
                counts[monthly.report_type] += 1
        if progress:
            progress(month, len(rows), len(existing.keys() - seen))
        month = end
    return counts
