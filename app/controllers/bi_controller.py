from flask import Blueprint, jsonify, request
from datetime import datetime
from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.outlet import Outlet
from collections import defaultdict

bi_bp = Blueprint('bi', __name__, url_prefix='/bi')

def _parse_opening_day(closing_date_str: str) -> int | None:
    """Parses an opening day from a string like '12-11'."""
    if not closing_date_str or '-' not in closing_date_str:
        return None
    try:
        start_day = int(closing_date_str.split('-')[0])
        return start_day if 1 <= start_day <= 31 else None
    except (ValueError, TypeError, IndexError):
        return None

@bi_bp.route('/brand-performance', methods=['POST'])
def get_brand_performance_by_partner():
    """
    Provides aggregated monthly net income for a brand, nested by partner and outlet.
    """
    json_data = request.get_json()
    if not json_data:
        return jsonify({"error": "Invalid JSON"}), 400

    brand_name = json_data.get("brand_name")
    year = json_data.get("year", datetime.now().year)

    if not brand_name:
        return jsonify({"error": "brand_name is required"}), 400

    # 1. Fetch all active outlets for the brand
    outlets = Outlet.query.filter_by(brand=brand_name, status='Active').all()
    if not outlets:
        return jsonify({"data": {}}), 200

    outlet_ids = [outlet.outlet_code for outlet in outlets]

    # 2. Fetch all relevant daily totals in a single query
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 31) # Fetch into Jan of next year for financial month calculations
    
    daily_totals = db.session.query(DailyMerchantTotal).filter(
        DailyMerchantTotal.outlet_id.in_(outlet_ids),
        DailyMerchantTotal.date.between(start_date, end_date)
    ).all()

    # 3. Pre-process the daily totals into a per-outlet monthly structure
    processed_outlets = {}
    outlet_map = {o.outlet_code: o for o in outlets}

    for daily_total in daily_totals:
        outlet = outlet_map.get(daily_total.outlet_id)
        if not outlet:
            continue
        
        # Initialize the outlet's data structure if not present
        if outlet.outlet_code not in processed_outlets:
            opening_day = _parse_opening_day(outlet.closing_date)
            closing_day_display = outlet.closing_date if opening_day else 'Calendar'
            processed_outlets[outlet.outlet_code] = {
                'name': outlet.outlet_name_gojek,
                'closing_day': closing_day_display,
                'monthly_totals': defaultdict(float) # Use defaultdict for easier summation
            }

        # Calculate the financial month for the transaction
        transaction_date = daily_total.date
        opening_day = _parse_opening_day(outlet.closing_date)
        
        financial_year = transaction_date.year
        financial_month = transaction_date.month

        if opening_day and transaction_date.day < opening_day:
            # Belongs to the financial period of the previous calendar month
            financial_month -= 1
            if financial_month == 0:
                financial_month = 12
                financial_year -= 1
        
        # Add the total to the correct financial month if it's in the requested year
        if financial_year == year:
            processed_outlets[outlet.outlet_code]['monthly_totals'][financial_month] += float(daily_total.total_net)

    # 4. Group the processed outlets by partner name
    partner_data = defaultdict(dict)
    for outlet in outlets:
        partner_name = outlet.pic_partner_name or "Unassigned"
        if outlet.outlet_code in processed_outlets:
            # Convert defaultdict back to a regular dict for JSON serialization
            processed_outlets[outlet.outlet_code]['monthly_totals'] = dict(processed_outlets[outlet.outlet_code]['monthly_totals'])
            partner_data[partner_name][outlet.outlet_code] = processed_outlets[outlet.outlet_code]

    return jsonify({"data": partner_data})