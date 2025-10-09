from datetime import datetime, timedelta

from app.models import db
from app.models.daily_merchant_total import DailyMerchantTotal
from app.models.outlet import Outlet


def generate_monthly_net_income_data(brand_name: str, year: int) -> dict:
    """
    Generates aggregated monthly net income data for all active outlets of a specific brand.

    Args:
        brand_name: The name of the brand to generate the report for.
        year: The year to generate the report for.

    Returns:
        A dictionary containing the processed data.
    """
    outlets = Outlet.query.filter_by(brand=brand_name, status='Active').all()
    if not outlets:
        return {}

    outlet_ids = [outlet.id for outlet in outlets]
    start_date = datetime(year - 1, 12, 1)
    end_date = datetime(year, 12, 31)

    daily_totals = (
        db.session.query(DailyMerchantTotal)
        .filter(
            DailyMerchantTotal.outlet_id.in_(outlet_ids),
            DailyMerchantTotal.transaction_date >= start_date,
            DailyMerchantTotal.transaction_date <= end_date,
        )
        .all()
    )

    data = {}
    for outlet in outlets:
        data[outlet.outlet_code] = {
            'name': outlet.name,
            'closing_day': outlet.closing_date if outlet.closing_date else 'Calendar',
            'monthly_totals': {i: 0 for i in range(1, 13)},
        }

    for daily_total in daily_totals:
        outlet = next((o for o in outlets if o.id == daily_total.outlet_id), None)
        if not outlet:
            continue

        transaction_date = daily_total.transaction_date
        closing_day = outlet.closing_date

        financial_month = transaction_date.month
        financial_year = transaction_date.year

        if closing_day and 1 <= closing_day <= 31:
            if transaction_date.day > closing_day:
                # Move to the first day of the current month, add 32 days to guarantee we are in the next month,
                # then get the first day of that next month.
                next_month_date = (transaction_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                financial_month = next_month_date.month
                financial_year = next_month_date.year

        if financial_year == year:
            data[outlet.outlet_code]['monthly_totals'][financial_month] += daily_total.total_net

    return data