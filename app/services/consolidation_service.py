from sqlalchemy import func
from datetime import datetime

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopee_reports import ShopeeReport
from app.models.shopeepay_reports import ShopeepayReport
REPORT_CONFIG = {
    'gojek': {
        'model': GojekReport,
        'date_col': GojekReport.transaction_date,
        'gross_col': GojekReport.amount,
        'net_col': GojekReport.nett_amount,
        'filters': [],
    },
    'grab': {
        'model': GrabFoodReport,
        'date_col': func.cast(GrabFoodReport.tanggal_dibuat, db.Date),
        'gross_col': GrabFoodReport.amount,
        'net_col': GrabFoodReport.total,
        'filters': [],
    },
    'shopee': {
        'model': ShopeeReport,
        'date_col': func.cast(ShopeeReport.order_create_time, db.Date),
        'gross_col': ShopeeReport.order_amount,
        'net_col': ShopeeReport.net_income,
        'filters': [ShopeeReport.order_status != "Cancelled"],
    },
    'shopeepay': {
        'model': ShopeepayReport,
        'date_col': func.cast(ShopeepayReport.create_time, db.Date),
        'gross_col': ShopeepayReport.transaction_amount,
        'net_col': ShopeepayReport.settlement_amount,
        'filters': [ShopeepayReport.transaction_type != "Withdrawal"],
    },
}

def update_daily_total_for_outlet(outlet_id: str, date: datetime.date, report_type: str):
    """
    Calculates and updates the daily total for a specific outlet, date, and report type.

    Args:
        outlet_id: The code of the outlet.
        date: The specific date to calculate the total for.
        report_type: The type of report ('gojek', 'grab', 'shopee', 'shopeepay').
    """
    if report_type not in REPORT_CONFIG:
        raise ValueError(f"Invalid report type: {report_type}")

    config = REPORT_CONFIG[report_type]
    model = config['model']
    date_col = config['date_col']
    gross_col = config['gross_col']
    net_col = config['net_col']
    filters = config['filters']

    # Query to get the sum of gross and net amounts for the given outlet and date
    query = db.session.query(
        func.sum(gross_col).label('total_gross'),
        func.sum(net_col).label('total_net')
    ).filter(
        model.outlet_code == outlet_id,
        date_col == date,
        *filters
    )

    daily_total = query.one()

    total_gross = daily_total.total_gross or 0
    total_net = daily_total.total_net or 0

    # Find the existing DailyMerchantTotal record
    existing_total = DailyMerchantTotal.query.filter_by(
        outlet_id=outlet_id,
        date=date,
        report_type=report_type
    ).first()

    if existing_total:
        # Update the existing record if the totals have changed
        if existing_total.total_gross != total_gross or existing_total.total_net != total_net:
            existing_total.total_gross = total_gross
            existing_total.total_net = total_net
            existing_total.updated_at = datetime.utcnow()
    else:
        # Create a new record if it doesn't exist
        new_total = DailyMerchantTotal(
            outlet_id=outlet_id,
            date=date,
            report_type=report_type,
            total_gross=total_gross,
            total_net=total_net
        )
        db.session.add(new_total)