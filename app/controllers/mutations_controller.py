from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.gojek_reports import GojekReport
from app.models.outlet import Outlet
from datetime import datetime, timedelta
from sqlalchemy import func, cast, Date

mutations_bp = Blueprint('mutations', __name__)

def create_standardized_match(platform_data, platform_name):
    """Helper function to create standardized match structure"""
    amount = platform_data.get('total_amount') or platform_data.get('total_net_income') or platform_data.get('total_settlement')
    return {
        'platform_report': {
            'merchant_id': platform_data.get('merchant_id') or platform_data.get('store_id') or platform_data.get('entity_id'),
            'merchant_name': platform_data.get('merchant_name') or platform_data.get('store_name'),
            'transaction_date': platform_data.get('transaction_date'),
            'amount': float(amount) if amount is not None else 0.0,
            'platform': platform_name
        },
        'mutation_match': None
    }

def create_standardized_mutation(mutation):
    """Helper function to create standardized mutation structure"""
    return {
        'transaction_id': mutation.transaction_id,
        'transaction_amount': float(mutation.transaction_amount) if mutation.transaction_amount is not None else 0.0,
        'rekening_number': mutation.rekening_number,
        'transaction_date': mutation.tanggal,
        'platform_code': getattr(mutation, 'platform_code', None)
    }

def create_standardized_unmatched(platform_data, platform_name):
    """Helper function to create standardized unmatched structure"""
    amount = platform_data.get('amount') or platform_data.get('total_amount')
    return {
        'merchant_id': platform_data.get('merchant_id') or platform_data.get('store_id') or platform_data.get('entity_id'),
        'merchant_name': platform_data.get('merchant_name') or platform_data.get('store_name'),
        'transaction_date': platform_data.get('transaction_date'),
        'amount': float(amount) if amount is not None else 0.0,
        'platform': platform_name
    }

@mutations_bp.route('/match/gojek', methods=['GET'])
def match_gojek_transactions():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)

        # Get aggregated Gojek reports with merchant_name
        gojek_aggregated = db.session.query(
            GojekReport.merchant_id,
            GojekReport.merchant_name,
            cast(GojekReport.transaction_date, Date).label('order_date'),
            func.sum(GojekReport.nett_amount).label('total_amount')
        ).filter(
            GojekReport.transaction_date >= start_date,
            GojekReport.transaction_date <= end_date
        ).group_by(
            GojekReport.merchant_id,
            GojekReport.merchant_name,
            cast(GojekReport.transaction_date, Date)
        ).order_by(
            cast(GojekReport.transaction_date, Date)
        )

        # Calculate total pages
        total_records = gojek_aggregated.count()
        total_pages = (total_records + per_page - 1) // per_page
        
        # Apply pagination for response data
        gojek_aggregated = gojek_aggregated.offset((page - 1) * per_page).limit(per_page).all()

        # Get mutations with specific fields
        mutations = db.session.query(
            BankMutation.transaction_id,
            BankMutation.transaction_amount,
            BankMutation.rekening_number,
            BankMutation.tanggal,
            BankMutation.platform_code
        ).filter(
            BankMutation.platform_name == 'Gojek',
            BankMutation.tanggal >= start_date + timedelta(days=1),
            BankMutation.tanggal <= end_date + timedelta(days=1)
        ).all()

        matches = []
        unmatched_merchants = []
        for agg in gojek_aggregated:
            platform_data = {
                'merchant_id': agg.merchant_id,
                'merchant_name': agg.merchant_name,
                'transaction_date': agg.order_date,
                'total_amount': float(agg.total_amount)
            }
            
            match = create_standardized_match(platform_data, 'Gojek')

            mutation = next(
                (m for m in mutations 
                 if m.transaction_id and m.platform_code == agg.merchant_id 
                 and m.tanggal == agg.order_date + timedelta(days=1)
                 and abs(round(float(m.transaction_amount or 0), -1) - round(float(agg.total_amount), -1)) < 10),
                None
            )

            if mutation:
                match['mutation_match'] = create_standardized_mutation(mutation)
                matches.append(match)
            else:
                unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Gojek'))

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(gojek_aggregated),
                'page_matched': len(matches),
                'page_unmatched': len(unmatched_merchants),
                'unmatched_merchants': unmatched_merchants
            },
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records
            }
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@mutations_bp.route('/match/grab', methods=['GET'])
def match_grab_transactions():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)

        # Get aggregated Grab reports
        grab_aggregated = db.session.query(
            GrabFoodReport.id_toko,
            GrabFoodReport.nama_toko,
            cast(GrabFoodReport.tanggal_dibuat, Date).label('order_date'),
            func.sum(GrabFoodReport.total).label('total_amount')
        ).filter(
            GrabFoodReport.tanggal_dibuat >= start_date,
            GrabFoodReport.tanggal_dibuat <= end_date
            # GrabFoodReport.jenis == 'GrabFood'
        ).group_by(
            GrabFoodReport.id_toko,
            GrabFoodReport.nama_toko,
            cast(GrabFoodReport.tanggal_dibuat, Date)
        ).order_by(
            cast(GrabFoodReport.tanggal_dibuat, Date)
        )

        # Calculate total pages
        total_records = grab_aggregated.count()
        total_pages = (total_records + per_page - 1) // per_page
        
        # Apply pagination
        grab_aggregated = grab_aggregated.offset((page - 1) * per_page).limit(per_page).all()

        # Get mutations with specific fields
        mutations = db.session.query(
            BankMutation.transaction_id,
            BankMutation.transaction_amount,
            BankMutation.rekening_number,
            BankMutation.tanggal
        ).filter(
            BankMutation.platform_name == 'Grab',
            BankMutation.tanggal >= start_date + timedelta(days=1),
            BankMutation.tanggal <= end_date + timedelta(days=1)
        ).all()

        matches = []
        unmatched_merchants = []
        for agg in grab_aggregated:
            platform_data = {
                'store_id': agg.id_toko,
                'store_name': agg.nama_toko,
                'transaction_date': agg.order_date,
                'total_amount': float(agg.total_amount)
            }
            
            match = create_standardized_match(platform_data, 'Grab')

            mutation = next(
                (m for m in mutations 
                 if m.transaction_id
                 and m.tanggal == agg.order_date + timedelta(days=1)
                 and abs(round(float(m.transaction_amount or 0), -1) - round(float(agg.total_amount), -1)) < 10),
                None
            )

            if mutation:
                match['mutation_match'] = create_standardized_mutation(mutation)
                matches.append(match)
            else:
                unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Grab'))

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(grab_aggregated),
                'page_matched': len(matches),
                'page_unmatched': len(unmatched_merchants),
                'unmatched_merchants': unmatched_merchants
            },
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records
            }
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@mutations_bp.route('/match/shopee', methods=['GET'])
def match_shopee_transactions():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)

        # Get aggregated Shopee reports
        shopee_aggregated = db.session.query(
            ShopeeReport.store_id,
            ShopeeReport.store_name,
            cast(ShopeeReport.order_create_time, Date).label('order_date'),
            func.sum(ShopeeReport.net_income).label('total_amount')
        ).filter(
            ShopeeReport.order_create_time >= start_date,
            ShopeeReport.order_create_time <= end_date,
            ShopeeReport.order_status == 'Settled'
        ).group_by(
            ShopeeReport.store_id,
            ShopeeReport.store_name,
            cast(ShopeeReport.order_create_time, Date)
        ).order_by(
            cast(ShopeeReport.order_create_time, Date),
            ShopeeReport.store_id
        )

        # Calculate total pages
        total_records = shopee_aggregated.count()
        total_pages = (total_records + per_page - 1) // per_page
        
        # Apply pagination
        shopee_aggregated = shopee_aggregated.offset((page - 1) * per_page).limit(per_page).all()

        # Get mutations with specific fields
        mutations = db.session.query(
            BankMutation.transaction_id,
            BankMutation.transaction_amount,
            BankMutation.rekening_number,
            BankMutation.tanggal,
            BankMutation.platform_code
        ).filter(
            BankMutation.platform_name == 'ShopeeFood',
            BankMutation.tanggal >= start_date + timedelta(days=1),
            BankMutation.tanggal <= end_date + timedelta(days=1)
        ).all()

        matches = []
        unmatched_merchants = []
        for agg in shopee_aggregated:
            # Extract last 5 digits of store_id for platform_code matching
            store_code = agg.store_id[-5:] if agg.store_id else None
            
            platform_data = {
                'store_id': agg.store_id,
                'store_name': agg.store_name,
                'transaction_date': agg.order_date,
                'total_amount': float(agg.total_amount)
            }
            
            match = create_standardized_match(platform_data, 'Shopee')

            mutation = next(
                (m for m in mutations 
                 if m.transaction_id
                 and m.platform_code == store_code
                 and m.tanggal == agg.order_date + timedelta(days=1)
                 and abs(round(float(m.transaction_amount or 0), -1) - round(float(agg.total_amount), -1)) < 10),
                None
            )

            if mutation:
                match['mutation_match'] = create_standardized_mutation(mutation)
                matches.append(match)
            else:
                unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Shopee'))

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(shopee_aggregated),
                'page_matched': len(matches),
                'page_unmatched': len(unmatched_merchants),
                'unmatched_merchants': unmatched_merchants
            },
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records
            }
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@mutations_bp.route('/match/shopeepay', methods=['GET'])
def match_shopeepay_transactions():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)

        # Get aggregated ShopeePay reports
        shopeepay_aggregated = db.session.query(
            ShopeepayReport.entity_id,
            ShopeepayReport.merchant_store_name,
            cast(ShopeepayReport.created_at, Date).label('order_date'),
            func.sum(ShopeepayReport.settlement_amount).label('total_amount')
        ).filter(
            ShopeepayReport.created_at >= start_date,
            ShopeepayReport.created_at <= end_date
        ).group_by(
            ShopeepayReport.entity_id,
            ShopeepayReport.merchant_store_name,
            cast(ShopeepayReport.created_at, Date)
        ).order_by(
            cast(ShopeepayReport.created_at, Date),
            ShopeepayReport.entity_id
        )

        # Calculate total pages
        total_records = shopeepay_aggregated.count()
        total_pages = (total_records + per_page - 1) // per_page
        
        # Apply pagination
        shopeepay_aggregated = shopeepay_aggregated.offset((page - 1) * per_page).limit(per_page).all()

        # Get mutations with specific fields
        mutations = db.session.query(
            BankMutation.transaction_id,
            BankMutation.transaction_amount,
            BankMutation.rekening_number,
            BankMutation.tanggal
        ).filter(
            BankMutation.platform_name == 'Shopee',
            BankMutation.tanggal >= start_date + timedelta(days=1),
            BankMutation.tanggal <= end_date + timedelta(days=1)
        ).all()

        matches = []
        unmatched_merchants = []
        for agg in shopeepay_aggregated:
            platform_data = {
                'entity_id': agg.entity_id,
                'store_name': agg.merchant_store_name,
                'transaction_date': agg.order_date,
                'total_amount': float(agg.total_amount)
            }
            
            match = create_standardized_match(platform_data, 'ShopeePay')

            mutation = next(
                (m for m in mutations 
                 if m.transaction_id
                 and m.tanggal == agg.order_date + timedelta(days=1)
                 and abs(round(float(m.transaction_amount or 0), -1) - round(float(agg.total_amount), -1)) < 10),
                None
            )

            if mutation:
                match['mutation_match'] = create_standardized_mutation(mutation)
                matches.append(match)
            else:
                unmatched_merchants.append(create_standardized_unmatched(platform_data, 'ShopeePay'))

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(shopeepay_aggregated),
                'page_matched': len(matches),
                'page_unmatched': len(unmatched_merchants),
                'unmatched_merchants': unmatched_merchants
            },
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records
            }
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@mutations_bp.route('/match/summary', methods=['GET'])
def get_matching_summary():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        platform = request.args.get('platform', '').lower()

        platform_queries = {
            'gojek': {
                'model': GojekReport,
                'amount_field': GojekReport.nett_amount,
                'date_field': GojekReport.transaction_date,
                'group_by_fields': [GojekReport.merchant_id, GojekReport.merchant_name],
                'mutation_name': 'Gojek',
                'extra_filters': []
            },
            'grab': {
                'model': GrabFoodReport,
                'amount_field': GrabFoodReport.total,
                'date_field': GrabFoodReport.tanggal_dibuat,
                'group_by_fields': [GrabFoodReport.id_toko, GrabFoodReport.nama_toko],
                'mutation_name': 'Grab',
                'extra_filters': []
            },
            'shopee': {
                'model': ShopeeReport,
                'amount_field': ShopeeReport.net_income,
                'date_field': ShopeeReport.order_create_time,
                'group_by_fields': [ShopeeReport.store_id, ShopeeReport.store_name],
                'mutation_name': 'ShopeeFood',
                'extra_filters': [ShopeeReport.order_status == 'Settled']
            },
            'shopeepay': {
                'model': ShopeepayReport,
                'amount_field': ShopeepayReport.settlement_amount,
                'date_field': ShopeepayReport.created_at,
                'group_by_fields': [ShopeepayReport.entity_id, ShopeepayReport.merchant_store_name],
                'mutation_name': 'Shopee',
                'extra_filters': []
            }
        }

        if platform and platform not in platform_queries:
            return jsonify({'error': 'Invalid platform specified'}), 400

        results = {}
        platforms_to_query = [platform] if platform else platform_queries.keys()

        for p in platforms_to_query:
            query_config = platform_queries[p]
            
            # Get aggregated platform reports
            platform_aggregated = db.session.query(
                *query_config['group_by_fields'],
                cast(query_config['date_field'], Date).label('order_date'),
                func.sum(query_config['amount_field']).label('total_amount')
            ).filter(
                query_config['date_field'] >= start_date,
                query_config['date_field'] <= end_date,
                *query_config['extra_filters']
            ).group_by(
                *query_config['group_by_fields'],
                cast(query_config['date_field'], Date)
            ).all()

            # Get mutations
            mutations = db.session.query(
                BankMutation
            ).filter(
                BankMutation.platform_name == query_config['mutation_name'],
                BankMutation.tanggal >= start_date + timedelta(days=1),
                BankMutation.tanggal <= end_date + timedelta(days=1)
            ).all()

            matches = 0
            unmatched_merchants = len(platform_aggregated)

            for agg in platform_aggregated:
                for mut in mutations:
                    if (mut.tanggal == agg.order_date + timedelta(days=1) and 
                        abs(round(float(mut.transaction_amount or 0), -1) - round(float(agg.total_amount), -1)) < 10):
                        matches += 1
                        unmatched_merchants -= 1
                        break

            platform_total = sum(float(agg.total_amount or 0) for agg in platform_aggregated)
            mutation_total = sum(float(mut.transaction_amount or 0) for mut in mutations)

            results[p] = {
                'platform_transactions': {
                    'total_amount': platform_total
                },
                'bank_mutations': {
                    'total_amount': mutation_total
                },
                'comparison': {
                    'difference': abs(platform_total - mutation_total),
                    'difference_percentage': (
                        abs(platform_total - mutation_total) / platform_total * 100 
                        if platform_total > 0 else 0
                    ),
                    'matched_count': matches,
                    'unmatched_count': unmatched_merchants
                }
            }

        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500



