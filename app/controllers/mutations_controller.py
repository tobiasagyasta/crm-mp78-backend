from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.gojek_reports import GojekReport
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.manual_entry import ManualEntry
from app.models.outlet import Outlet
from datetime import datetime, timedelta
from sqlalchemy import func, cast, Date, distinct
from app.utils.transaction_matcher import TransactionMatcher
from app.models.income_category import IncomeCategory
from app.models.expense_category import ExpenseCategory
mutations_bp = Blueprint('mutations', __name__)

UNASSIGNED_PLATFORM_CODE_EXCLUDED_PLATFORMS = ('PKB', 'Grab', 'Gojek', 'Shopee', 'ShopeeFood')


def parse_date_param(param_name):
    value = request.args.get(param_name)
    if not value:
        raise ValueError(f'{param_name} is required')

    return datetime.strptime(value, '%Y-%m-%d').date()


def serialize_bank_mutation(mutation):
    return {
        'id': mutation.id,
        'rekening_number': mutation.rekening_number,
        'tanggal': mutation.tanggal.isoformat() if mutation.tanggal else None,
        'transaksi': mutation.transaksi,
        'transaction_type': mutation.transaction_type,
        'transaction_id': mutation.transaction_id,
        'transaction_amount': float(mutation.transaction_amount) if mutation.transaction_amount is not None else None,
        'platform_code': mutation.platform_code,
        'platform_name': mutation.platform_name,
        'created_at': mutation.created_at.isoformat() if mutation.created_at else None,
    }


def get_mutation_query_options():
    return {
        'start_date': parse_date_param('start_date'),
        'end_date': parse_date_param('end_date'),
        'page': request.args.get('page', 1, type=int),
        'per_page': request.args.get('per_page', 50, type=int),
    }


def build_unassigned_platform_code_mutations_query(options):
    return (
        BankMutation.query
        .filter(
            BankMutation.platform_code.is_(None),
            BankMutation.platform_name.notin_(UNASSIGNED_PLATFORM_CODE_EXCLUDED_PLATFORMS),
            BankMutation.tanggal >= options['start_date'],
            BankMutation.tanggal <= options['end_date'],
        )
    )


@mutations_bp.route('/mutations/unassigned', methods=['GET'])
def get_mutations():
    try:
        options = get_mutation_query_options()
        if options['start_date'] > options['end_date']:
            return jsonify({'error': 'start_date cannot be after end_date'}), 400

        page = max(options['page'], 1)
        per_page = min(max(options['per_page'], 1), 100)
        query = build_unassigned_platform_code_mutations_query(options)
        total_records = query.count()
        total_pages = (total_records + per_page - 1) // per_page

        mutations = (
            query
            .order_by(BankMutation.tanggal.desc(), BankMutation.id.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )

        return jsonify({
            'data': [serialize_bank_mutation(mutation) for mutation in mutations],
            'filters': {
                'start_date': options['start_date'].isoformat(),
                'end_date': options['end_date'].isoformat(),
                'platform_code': None,
                'excluded_platform_names': list(UNASSIGNED_PLATFORM_CODE_EXCLUDED_PLATFORMS),
            },
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records,
            },
        }), 200
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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

def build_match_response(platform, platform_label, start_date, end_date, page, per_page, platform_code_filter=None):
    matcher = TransactionMatcher(platform)
    batch_result = matcher.safe_rebuild_matches(start_date, end_date, platform_code_filter)

    total_records = len(batch_result['daily_totals'])
    total_pages = (total_records + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_results = batch_result['results'][start_idx:end_idx]
    total_unmatched_merchants = sum(
        1 for result in batch_result['results']
        if result['platform_data'] and not result['mutation_data']
    )

    matches = []
    unmatched_merchants = []
    for result in page_results:
        platform_data = result['platform_data']
        mutation_data = result['mutation_data']
        if not platform_data:
            continue
        if mutation_data:
            match = create_standardized_match(platform_data, platform_label)
            match['mutation_match'] = mutation_data
            matches.append(match)
        else:
            unmatched_merchants.append(create_standardized_unmatched(platform_data, platform_label))

    unmatched_mutations = [
        {
            'transaction_id': m.transaction_id,
            'platform_code': m.platform_code,
            'transaction_date': m.tanggal,
            'transaction_amount': float(m.transaction_amount or 0.0)
        }
        for m in batch_result['mutations']
        if (m.platform_code, m.tanggal) not in batch_result['matched_mutation_keys']
    ]

    return {
        'matches': matches,
        'unmatched_merchants': unmatched_merchants,
        'unmatched_mutations': unmatched_mutations,
        'total_unmatched_merchants': total_unmatched_merchants,
        'pagination': {
            'current_page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_records': total_records
        },
        'page_total': len(page_results),
    }

@mutations_bp.route('/match/summary', methods=['GET'])
def match_summary():
    """Summary route for transaction matching statistics across platforms"""
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        platform = request.args.get('platform', '').lower()
        platform_code = request.args.get('platform_code')
        
        # Validate platform parameter
        valid_platforms = ['gojek', 'grab', 'shopee']
        if platform and platform not in valid_platforms:
            return jsonify({'error': f'Invalid platform. Must be one of: {valid_platforms}'}), 400
        
        # Initialize results dictionary
        results = {}
        
        # Process each platform or just the requested one
        platforms_to_process = [platform] if platform else valid_platforms
        
        for current_platform in platforms_to_process:
            # Skip platform_code for Grab as it doesn't use it
            current_platform_code = None if current_platform == 'grab' else platform_code
            
            matcher = TransactionMatcher(current_platform)
            batch_result = matcher.safe_rebuild_matches(start_date, end_date, current_platform_code)
            
            # Calculate statistics
            total_merchants = len(batch_result['daily_totals'])
            total_matched = sum(1 for result in batch_result['results'] if result['mutation_data'])
            total_unmatched_merchants = total_merchants - total_matched
            total_unmatched_mutations = sum(
                1 for mutation in batch_result['mutations']
                if (mutation.platform_code, mutation.tanggal) not in batch_result['matched_mutation_keys']
            )
            
            # Calculate matching percentage
            matching_percentage = (total_matched / total_merchants * 100) if total_merchants > 0 else 0
            
            # Store results for this platform
            results[current_platform] = {
                'total_merchants': total_merchants,
                'total_matched': total_matched,
                'total_unmatched_merchants': total_unmatched_merchants,
                'total_unmatched_mutations': total_unmatched_mutations,
                'matching_percentage': round(matching_percentage, 2)
            }
        
        # Calculate overall statistics if multiple platforms were processed
        if len(platforms_to_process) > 1:
            total_merchants_all = sum(results[p]['total_merchants'] for p in platforms_to_process)
            total_matched_all = sum(results[p]['total_matched'] for p in platforms_to_process)
            total_unmatched_merchants_all = sum(results[p]['total_unmatched_merchants'] for p in platforms_to_process)
            total_unmatched_mutations_all = sum(results[p]['total_unmatched_mutations'] for p in platforms_to_process)
            
            overall_matching_percentage = (total_matched_all / total_merchants_all * 100) if total_merchants_all > 0 else 0
            
            results['overall'] = {
                'total_merchants': total_merchants_all,
                'total_matched': total_matched_all,
                'total_unmatched_merchants': total_unmatched_merchants_all,
                'total_unmatched_mutations': total_unmatched_mutations_all,
                'matching_percentage': round(overall_matching_percentage, 2)
            }
        
        return jsonify({
            'summary': results,
            'date_range': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
        }), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@mutations_bp.route('/match/gojek', methods=['GET'])
def match_gojek_transactions():
    try:
        start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        platform_code_filter = request.args.get('platform_code')

        response = build_match_response('gojek', 'Gojek', start_date, end_date, page, per_page, platform_code_filter)
        paginated_unmatched_mutations = response['unmatched_mutations'][(page - 1) * per_page:page * per_page]

        return jsonify({
            'matches': response['matches'],
            'statistics': {
                'page_total': response['page_total'],
                'page_matched': len(response['matches']),
                'page_unmatched': len(response['unmatched_merchants']),
                'total_unmatched': response['total_unmatched_merchants'],
                'total_unmatched_mutations': len(response['unmatched_mutations']),
                'unmatched_merchants': response['unmatched_merchants'],
                'unmatched_mutations': paginated_unmatched_mutations
            },
            'pagination': response['pagination']
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
        platform_code_filter = request.args.get('platform_code')

        response = build_match_response('grab', 'Grab', start_date, end_date, page, per_page, platform_code_filter)

        return jsonify({
            'matches': response['matches'],
            'statistics': {
                'page_total': response['page_total'],
                'page_matched': len(response['matches']),
                'page_unmatched': len(response['unmatched_merchants']),
                'unmatched_merchants': response['unmatched_merchants']
            },
            'pagination': response['pagination']
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
        platform_code_filter = request.args.get('platform_code')

        response = build_match_response('shopee', 'Shopee', start_date, end_date, page, per_page, platform_code_filter)
        paginated_unmatched_mutations = response['unmatched_mutations'][(page - 1) * per_page:page * per_page]

        return jsonify({
            'matches': response['matches'],
            'statistics': {
                'page_total': response['page_total'],
                'page_matched': len(response['matches']),
                'page_unmatched': len(response['unmatched_merchants']),
                'total_unmatched': response['total_unmatched_merchants'],
                'total_unmatched_mutations': len(response['unmatched_mutations']),
                'unmatched_merchants': response['unmatched_merchants'],
                'unmatched_mutations': paginated_unmatched_mutations
            },
            'pagination': response['pagination']
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
        platform_code_filter = request.args.get('platform_code')

        response = build_match_response('shopeepay', 'ShopeePay', start_date, end_date, page, per_page, platform_code_filter)
        paginated_unmatched_mutations = response['unmatched_mutations'][(page - 1) * per_page:page * per_page]

        return jsonify({
            'matches': response['matches'],
            'statistics': {
                'page_total': response['page_total'],
                'page_matched': len(response['matches']),
                'page_unmatched': len(response['unmatched_merchants']),
                'total_unmatched': response['total_unmatched_merchants'],
                'total_unmatched_mutations': len(response['unmatched_mutations']),
                'unmatched_merchants': response['unmatched_merchants'],
                'unmatched_mutations': paginated_unmatched_mutations
            },
            'pagination': response['pagination']
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@mutations_bp.route('/convert-pkb', methods=['GET'])
def convert_mutation_to_manual_entry():
    number_of_skips = 0
    try:
        # Fetch all mutations within the date range and platform code
        mutations = BankMutation.query.filter(
          BankMutation.platform_name == 'PKB'
        ).all()

        if not mutations:
            return jsonify({'message': 'No mutations found for the specified criteria.'}), 404

        manual_entries = []
        unmapped_pkb_codes = set()
        outlets_map = {o.pkb_code: o.outlet_code for o in Outlet.query.all()}
        categories_map = {
            'income': next((c.id for c in IncomeCategory.query.filter_by(name='PKB').all()), None),
            'expense': next((c.id for c in ExpenseCategory.query.filter_by(name='PKB').all()), None)
        }

        for mutation in mutations:
            # Skip if mutation.tanggal is None or not a date/datetime
            if not mutation.tanggal or not hasattr(mutation.tanggal, 'strftime'):
                number_of_skips += 1
                continue

            # Find the outlet with matching pkb_code
            outlet_code = outlets_map.get(mutation.platform_code)
            if not outlet_code:
                unmapped_pkb_codes.add(mutation.platform_code)
                continue  # or handle as needed (e.g., log, skip, etc.)

            # Determine entry type first
            entry_type = 'income' if mutation.transaction_type == 'CR' else 'expense'
            
            # Check for existing manual entry to avoid duplicates
            exists = ManualEntry.query.filter_by(
                outlet_code=outlet_code,
                amount=abs(mutation.transaction_amount) if mutation.transaction_amount is not None else 0,
                start_date=mutation.tanggal,
                end_date=mutation.tanggal,
                description=mutation.transaksi or '',
                entry_type=entry_type,
                brand_name='Pukis & Martabak Kota Baru',
                category_id=categories_map[entry_type]
            ).first()
            if exists:
                number_of_skips += 1
                continue  # Skip if already exists

            manual_entry = ManualEntry(
                outlet_code=outlet_code,  # Use the mapped outlet_code
                brand_name='Pukis & Martabak Kota Baru',
                entry_type=entry_type,
                amount=abs(mutation.transaction_amount) if mutation.transaction_amount is not None else 0,
                description=mutation.transaksi or '',
                start_date=mutation.tanggal,
                end_date=mutation.tanggal,
                category_id=categories_map[entry_type]
            )
            db.session.add(manual_entry)
          
        db.session.commit()
        return jsonify({
            'count': len(manual_entries),
            'unmapped_pkb_codes': sorted(unmapped_pkb_codes),
            'skipped_entries': number_of_skips,
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# @mutations_bp.route('/update-grab-platform-codes', methods=['POST'])
# def update_grab_platform_codes():
#     """Update platform_code for ALL matched Grab transactions in date range"""
#     try:
#         start_date = datetime.strptime(request.json.get('start_date'), '%Y-%m-%d').date()
#         end_date = datetime.strptime(request.json.get('end_date'), '%Y-%m-%d').date()
#         platform_code_filter = request.json.get('platform_code')  # Optional

#         matcher = TransactionMatcher('grab')

#         daily_totals_query = matcher.get_daily_totals_query(start_date, end_date, platform_code_filter)
#         all_daily_totals = daily_totals_query.all()
#         mutations = matcher.get_mutations_query(start_date, end_date).all()

#         updates_made = 0
#         matched_count = 0
#         mutations_to_update = []

#         for daily_total in all_daily_totals:
#             platform_data, mutation_data = matcher.match_transactions(daily_total, mutations)

#             if platform_data and mutation_data:
#                 matched_count += 1

#                 # 👇 Replace lookup by matching date + amount instead of transaction_id
#                 mutation_obj = next(
#                     (m for m in mutations
#                      if m.tanggal.date() == mutation_data['tanggal'].date()
#                      and float(m.amount) == float(mutation_data['amount'])),
#                     None
#                 )

#                 if mutation_obj and mutation_obj.platform_code != daily_total.outlet_id:
#                     mutation_obj.platform_code = daily_total.outlet_id
#                     mutations_to_update.append(mutation_obj)
#                     updates_made += 1

#         if mutations_to_update:
#             try:
#                 db.session.bulk_save_objects(mutations_to_update)
#                 db.session.commit()
#                 return jsonify({
#                     'success': True,
#                     'message': f'Successfully updated {updates_made} platform codes',
#                     'statistics': {
#                         'total_matched': matched_count,
#                         'platform_codes_updated': updates_made,
#                         'date_range': f'{start_date} to {end_date}'
#                     }
#                 }), 200
#             except Exception as e:
#                 db.session.rollback()
#                 return jsonify({'success': False, 'error': f'Database error: {str(e)}'}), 500
#         else:
#             return jsonify({
#                 'success': True,
#                 'message': 'No platform code updates needed',
#                 'statistics': {
#                     'total_matched': matched_count,
#                     'platform_codes_updated': 0,
#                     'date_range': f'{start_date} to {end_date}'
#                 }
#             }), 200

#     except Exception as e:
#         return jsonify({'success': False, 'error': str(e)}), 500






