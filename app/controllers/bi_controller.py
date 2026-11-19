from flask import Blueprint, jsonify, request
from datetime import date, datetime
from decimal import Decimal
from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.outlet import Outlet
from collections import defaultdict

bi_bp = Blueprint('bi', __name__, url_prefix='/bi')

DAILY_TOTAL_PLATFORMS = ('gojek', 'grab', 'shopee')


@bi_bp.route('/daily-totals', methods=['GET'])
def get_daily_totals():
    """Return transaction-date totals by brand, date and platform."""
    try:
        start = date.fromisoformat(request.args.get('start_date', ''))
        end = date.fromisoformat(request.args.get('end_date', ''))
        if start > end:
            raise ValueError('start_date cannot be after end_date')
        platform_args = request.args.getlist('platform')
        platforms = list(dict.fromkeys(value.strip().lower() for value in platform_args))
        if any(platform not in DAILY_TOTAL_PLATFORMS for platform in platforms):
            raise ValueError('platform must be gojek, grab or shopee')
        platforms = platforms or list(DAILY_TOTAL_PLATFORMS)
        brand = request.args.get('brand')
        if brand is not None:
            brand = brand.strip()
            if not brand:
                raise ValueError('brand cannot be empty')
        include_outlets = request.args.get('include_outlets', 'false').lower()
        if include_outlets not in ('true', 'false'):
            raise ValueError('include_outlets must be true or false')
        page = int(request.args.get('page', '1'))
        per_page = int(request.args.get('per_page', '25'))
        if page < 1 or not 1 <= per_page <= 100:
            raise ValueError('page must be positive and per_page must be between 1 and 100')
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    filters = [
        DailyMerchantTotal.date >= start,
        DailyMerchantTotal.date <= end,
        DailyMerchantTotal.report_type.in_(platforms),
    ]
    if brand is not None:
        filters.append(Outlet.brand == brand)

    def base_query(*columns):
        return (db.session.query(*columns)
                .select_from(DailyMerchantTotal)
                .outerjoin(Outlet, Outlet.outlet_code == DailyMerchantTotal.outlet_id)
                .filter(*filters))

    gross = db.func.coalesce(db.func.sum(DailyMerchantTotal.total_gross), 0)
    net = db.func.coalesce(db.func.sum(DailyMerchantTotal.total_net), 0)
    rows = (base_query(Outlet.brand, DailyMerchantTotal.date,
                       DailyMerchantTotal.report_type, gross, net)
            .group_by(Outlet.brand, DailyMerchantTotal.date, DailyMerchantTotal.report_type)
            .order_by(Outlet.brand, DailyMerchantTotal.date, DailyMerchantTotal.report_type)
            .all())
    overall_gross = sum((Decimal(str(row[3])) for row in rows), Decimal(0))
    overall_net = sum((Decimal(str(row[4])) for row in rows), Decimal(0))
    brand_totals = {}
    for row_brand, _, platform, row_gross, row_net in rows:
        item = brand_totals.setdefault(row_brand, {'gross': Decimal(0), 'net': Decimal(0),
                                                   'platforms': {}})
        platform_total = item['platforms'].setdefault(platform, {'gross': Decimal(0), 'net': Decimal(0)})
        for metric, value in (('gross', row_gross), ('net', row_net)):
            amount = Decimal(str(value))
            item[metric] += amount
            platform_total[metric] += amount
    response = {
        'filters': {'start_date': start.isoformat(), 'end_date': end.isoformat(),
                    'brand': brand, 'platforms': platforms, 'date_basis': 'transaction_date'},
        'totals': {'gross': str(overall_gross), 'net': str(overall_net)},
        'brands': [
            {'brand': name, 'gross': str(item['gross']), 'net': str(item['net']),
             'platforms': [{'platform': platform, 'gross': str(amounts['gross']),
                            'net': str(amounts['net'])}
                           for platform, amounts in sorted(item['platforms'].items())]}
            for name, item in brand_totals.items()
        ],
        'daily': [
            {'brand': row_brand, 'date': day.isoformat(), 'platform': platform,
             'gross': str(row_gross), 'net': str(row_net)}
            for row_brand, day, platform, row_gross, row_net in rows
        ],
    }
    if include_outlets == 'true':
        name = db.func.coalesce(
            db.func.nullif(db.func.trim(Outlet.outlet_name_gojek), ''),
            db.func.nullif(db.func.trim(Outlet.outlet_name_grab), ''),
            db.func.nullif(db.func.trim(Outlet.outlet_name_webshop), ''),
            db.func.nullif(db.func.trim(Outlet.outlet_name_qpon), ''),
            DailyMerchantTotal.outlet_id,
        )
        outlet_query = (base_query(DailyMerchantTotal.outlet_id.label('outlet_code'),
                                   Outlet.brand.label('brand'), name.label('outlet_name'),
                                   gross.label('gross'), net.label('net'))
                        .group_by(DailyMerchantTotal.outlet_id, Outlet.brand, name))
        total_outlets = outlet_query.order_by(None).count()
        outlet_rows = (outlet_query.order_by(db.func.lower(name), DailyMerchantTotal.outlet_id)
                       .offset((page - 1) * per_page).limit(per_page).all())
        outlet_platforms = defaultdict(list)
        outlet_codes = [row.outlet_code for row in outlet_rows]
        if outlet_codes:
            platform_rows = (base_query(DailyMerchantTotal.outlet_id, DailyMerchantTotal.report_type,
                                        gross, net)
                             .filter(DailyMerchantTotal.outlet_id.in_(outlet_codes))
                             .group_by(DailyMerchantTotal.outlet_id, DailyMerchantTotal.report_type)
                             .order_by(DailyMerchantTotal.report_type).all())
            for code, platform, platform_gross, platform_net in platform_rows:
                outlet_platforms[code].append({'platform': platform, 'gross': str(platform_gross),
                                               'net': str(platform_net)})
        response['outlets'] = {
            'data': [{'outlet_code': row.outlet_code, 'outlet_name': row.outlet_name,
                      'brand': row.brand, 'gross': str(row.gross), 'net': str(row.net),
                      'platforms': outlet_platforms[row.outlet_code]}
                     for row in outlet_rows],
            'pagination': {'page': page, 'per_page': per_page, 'total_records': total_outlets,
                           'total_pages': (total_outlets + per_page - 1) // per_page},
        }
    return jsonify(response)

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
