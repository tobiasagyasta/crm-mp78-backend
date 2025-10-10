from flask import Blueprint, jsonify, request
from app.extensions import db
from app.models.outlet import Outlet
from app.models.daily_merchant_totals import DailyMerchantTotal
from sqlalchemy import func
from datetime import datetime, timedelta

bi_bp = Blueprint('bi', __name__, url_prefix='/bi')

@bi_bp.route('/partner-performance', methods=['POST'])
def partner_performance():
    data = request.get_json()
    if not data or 'partner_name' not in data:
        return jsonify({'error': 'partner_name is required'}), 400

    partner_name = data.get('partner_name')
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    else:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

    # Step A: Find Partner's Outlets
    outlets = Outlet.query.filter_by(pic_partner_name=partner_name).all()
    if not outlets:
        return jsonify({'error': 'Partner not found or has no outlets'}), 404

    outlet_codes = [outlet.outlet_code for outlet in outlets]
    outlet_name_map = {outlet.outlet_code: outlet.outlet_name_gojek for outlet in outlets}

    # Step B: Aggregate Earnings
    results = db.session.query(
        DailyMerchantTotal.outlet_id,
        DailyMerchantTotal.report_type,
        func.sum(DailyMerchantTotal.total_net).label('total_net')
    ).filter(
        DailyMerchantTotal.outlet_id.in_(outlet_codes),
        DailyMerchantTotal.date.between(start_date, end_date)
    ).group_by(
        DailyMerchantTotal.outlet_id,
        DailyMerchantTotal.report_type
    ).all()

    # Format the response
    response_data = {}
    for outlet_id, report_type, total_net in results:
        if outlet_id not in response_data:
            response_data[outlet_id] = {
                "name": outlet_name_map.get(outlet_id, "Unknown"),
                "total": 0
            }

        total_net_float = float(total_net) if total_net is not None else 0.0

        response_data[outlet_id][report_type.lower()] = total_net_float
        response_data[outlet_id]["total"] += total_net_float

    return jsonify({"data": response_data})