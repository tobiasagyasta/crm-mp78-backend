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
            
            # Initialize matcher for current platform
            matcher = TransactionMatcher(current_platform)
            
            # Get daily totals
            daily_totals = matcher.get_daily_totals_query(start_date, end_date, current_platform_code).all()
            
            # Get mutations
            mutations = matcher.get_mutations_query(start_date, end_date).all()
            
            # Process matching
            matches = []
            unmatched_merchants = []
            matched_mutations = set()
            
            # Process each daily total
            for total in daily_totals:
                platform_data, mutation_data = matcher.match_transactions(total, mutations)
                
                if platform_data:
                    if mutation_data:
                        matches.append(1)  # Just count, don't store details
                        matched_mutations.add((mutation_data.get('platform_code', ''), mutation_data.get('transaction_date')))
                    else:
                        unmatched_merchants.append(1)  # Just count
            
            # Find unmatched mutations
            unmatched_mutations = [
                1  # Just count
                for m in mutations
                if (getattr(m, 'platform_code', ''), m.tanggal) not in matched_mutations
            ]
            
            # Calculate statistics
            total_merchants = len(daily_totals)
            total_matched = len(matches)
            total_unmatched_merchants = len(unmatched_merchants)
            total_unmatched_mutations = len(unmatched_mutations)
            
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

        matcher = TransactionMatcher('gojek')

       
        # Get daily totals with pagination
        daily_totals_query = matcher.get_daily_totals_query(start_date, end_date, platform_code_filter)
        total_records = daily_totals_query.count()
        total_pages = (total_records + per_page - 1) // per_page

        daily_totals = daily_totals_query.order_by(DailyMerchantTotal.date)\
                                       .offset((page - 1) * per_page)\
                                       .limit(per_page)\
                                       .all()

        # Get mutations
        mutations = matcher.get_mutations_query(start_date, end_date).all()

        matches = []
        unmatched_merchants = []
        matched_mutations = set()

        # Process each daily total
        for total in daily_totals:
            platform_data, mutation_data = matcher.match_transactions(total, mutations)
            
            if platform_data:
                if mutation_data:
                    match = create_standardized_match(platform_data, 'Gojek')
                    match['mutation_match'] = mutation_data
                    matches.append(match)
                    matched_mutations.add((mutation_data['platform_code'], mutation_data['transaction_date']))
                else:
                    unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Gojek'))

        # Find unmatched mutations
        unmatched_mutations = [
            {
                'transaction_id': m.transaction_id,
                'platform_code': m.platform_code,
                'transaction_date': m.tanggal,
                'transaction_amount': float(m.transaction_amount or 0.0)
            }
            for m in mutations
            if (m.platform_code, m.tanggal) not in matched_mutations
        ]

        # Paginate unmatched lists
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_unmatched_merchants = unmatched_merchants[start_idx:end_idx]
        paginated_unmatched_mutations = unmatched_mutations[start_idx:end_idx]

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(daily_totals),
                'page_matched': len(matches),
                'page_unmatched': len(paginated_unmatched_merchants),
                'total_unmatched': len(unmatched_merchants),
                'total_unmatched_mutations': len(unmatched_mutations),
                'unmatched_merchants': paginated_unmatched_merchants,
                'unmatched_mutations': paginated_unmatched_mutations
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
        platform_code_filter = request.args.get('platform_code')

        # Initialize TransactionMatcher for Grab
        matcher = TransactionMatcher('grab')

        # Get daily totals query
        daily_totals_query = matcher.get_daily_totals_query(start_date, end_date,platform_code_filter)

        # Calculate total pages
        total_records = daily_totals_query.count()
        total_pages = (total_records + per_page - 1) // per_page

        # Apply pagination
        daily_totals = daily_totals_query.offset((page - 1) * per_page).limit(per_page).all()

        # Get mutations
        mutations = matcher.get_mutations_query(start_date, end_date).all()

        matches = []
        unmatched_merchants = []
        matched_mutations = set()

        for daily_total in daily_totals:
            platform_data, mutation_data = matcher.match_transactions(daily_total, mutations)
            
            if platform_data:
                match = create_standardized_match(platform_data, 'Grab')
                if mutation_data:
                    match['mutation_match'] = mutation_data
                    matches.append(match)
                    matched_mutations.add((mutation_data['platform_code'], mutation_data['transaction_date']))
                else:
                    unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Grab'))

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(daily_totals),
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
        platform_code_filter = request.args.get('platform_code')

        matcher = TransactionMatcher('shopee')
        
        # Get daily totals with pagination
        daily_totals_query = matcher.get_daily_totals_query(start_date, end_date, platform_code_filter)
        total_records = daily_totals_query.count()
        total_pages = (total_records + per_page - 1) // per_page

        daily_totals = daily_totals_query.order_by(DailyMerchantTotal.date)\
                                       .offset((page - 1) * per_page)\
                                       .limit(per_page)\
                                       .all()

        # Get mutations
        mutations = matcher.get_mutations_query(start_date, end_date).all()

        matches = []
        unmatched_merchants = []
        matched_mutations = set()

        # Process each daily total
        for total in daily_totals:
            platform_data, mutation_data = matcher.match_transactions(total, mutations)
            
            if platform_data:
                if mutation_data:
                    match = create_standardized_match(platform_data, 'Shopee')
                    match['mutation_match'] = mutation_data
                    matches.append(match)
                    matched_mutations.add((mutation_data['platform_code'], mutation_data['transaction_date']))
                else:
                    unmatched_merchants.append(create_standardized_unmatched(platform_data, 'Shopee'))

        # Find unmatched mutations
        unmatched_mutations = [
            {
                'transaction_id': m.transaction_id,
                'platform_code': m.platform_code,
                'transaction_date': m.tanggal,
                'transaction_amount': float(m.transaction_amount or 0.0)
            }
            for m in mutations
            if (m.platform_code, m.tanggal) not in matched_mutations
        ]

        # Paginate unmatched lists
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_unmatched_merchants = unmatched_merchants[start_idx:end_idx]
        paginated_unmatched_mutations = unmatched_mutations[start_idx:end_idx]

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(daily_totals),
                'page_matched': len(matches),
                'page_unmatched': len(paginated_unmatched_merchants),
                'total_unmatched': len(unmatched_merchants),
                'total_unmatched_mutations': len(unmatched_mutations),
                'unmatched_merchants': paginated_unmatched_merchants,
                'unmatched_mutations': paginated_unmatched_mutations
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
        platform_code_filter = request.args.get('platform_code')

        matcher = TransactionMatcher('shopeepay')
        
        # Get daily totals with pagination
        daily_totals_query = matcher.get_daily_totals_query(start_date, end_date, platform_code_filter)
        total_records = daily_totals_query.count()
        total_pages = (total_records + per_page - 1) // per_page

        daily_totals = daily_totals_query.order_by(DailyMerchantTotal.date)\
                                       .offset((page - 1) * per_page)\
                                       .limit(per_page)\
                                       .all()

        # Get mutations
        mutations = matcher.get_mutations_query(start_date, end_date).all()

        matches = []
        unmatched_merchants = []
        matched_mutations = set()

        # Process each daily total
        for total in daily_totals:
            platform_data, mutation_data = matcher.match_transactions(total, mutations)
            
            if platform_data:
                if mutation_data:
                    match = create_standardized_match(platform_data, 'ShopeePay')
                    match['mutation_match'] = mutation_data
                    matches.append(match)
                    matched_mutations.add((mutation_data['platform_code'], mutation_data['transaction_date']))
                else:
                    unmatched_merchants.append(create_standardized_unmatched(platform_data, 'ShopeePay'))

        # Find unmatched mutations
        unmatched_mutations = [
            {
                'transaction_id': m.transaction_id,
                'platform_code': m.platform_code,
                'transaction_date': m.tanggal,
                'transaction_amount': float(m.transaction_amount or 0.0)
            }
            for m in mutations
            if (m.platform_code, m.tanggal) not in matched_mutations
        ]

        # Paginate unmatched lists
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_unmatched_merchants = unmatched_merchants[start_idx:end_idx]
        paginated_unmatched_mutations = unmatched_mutations[start_idx:end_idx]

        return jsonify({
            'matches': matches,
            'statistics': {
                'page_total': len(daily_totals),
                'page_matched': len(matches),
                'page_unmatched': len(paginated_unmatched_merchants),
                'total_unmatched': len(unmatched_merchants),
                'total_unmatched_mutations': len(unmatched_mutations),
                'unmatched_merchants': paginated_unmatched_merchants,
                'unmatched_mutations': paginated_unmatched_mutations
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
        for mutation in mutations:
            # Skip if mutation.tanggal is None or not a date/datetime
            if not mutation.tanggal or not hasattr(mutation.tanggal, 'strftime'):
                number_of_skips += 1
                continue

            # Find the outlet with matching pkb_code
            outlet = Outlet.query.filter_by(pkb_code=mutation.platform_code).first()
            if not outlet:
                unmapped_pkb_codes.add(mutation.platform_code)
                continue  # or handle as needed (e.g., log, skip, etc.)

            # Check for existing manual entry to avoid duplicates
            exists = ManualEntry.query.filter_by(
                outlet_code=outlet.outlet_code,
                amount=abs(mutation.transaction_amount) if mutation.transaction_amount is not None else 0,
                start_date=mutation.tanggal,
                end_date=mutation.tanggal,
                description=mutation.transaksi or '',
                # category_id=9,
                entry_type='expense' or 'income',
                brand_name='Pukis & Martabak Kota Baru'
            ).first()
            if exists:
                number_of_skips += 1
                continue  # Skip if already exists

            entry_type = 'income' if mutation.transaction_type == 'CR' else 'expense'
            if entry_type == 'income':
                from app.models.income_category import IncomeCategory
                category = IncomeCategory.query.filter_by(name='PKB').first()
            else:
                from app.models.expense_category import ExpenseCategory
                category = ExpenseCategory.query.filter_by(name='PKB').first()
            manual_entry = ManualEntry(
                outlet_code=outlet.outlet_code,  # Use the mapped outlet_code
                brand_name='Pukis & Martabak Kota Baru',
                entry_type=entry_type,
                amount=abs(mutation.transaction_amount) if mutation.transaction_amount is not None else 0,
                description=mutation.transaksi or '',
                start_date=mutation.tanggal,
                end_date=mutation.tanggal,
                category_id=category.id if category else None
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






