from flask import Blueprint, jsonify
from app.extensions import db
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopee_reports import ShopeeReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.daily_merchant_totals import DailyMerchantTotal
from sqlalchemy import func
from datetime import datetime

daily_merchant_totals_bp = Blueprint('daily_merchant_totals', __name__)

@daily_merchant_totals_bp.route('/consolidate/grab', methods=['POST'])
def consolidate_grab_totals():
    try:
        # Get daily totals from Grab reports
        daily_totals = db.session.query(
            GrabFoodReport.outlet_code,
            func.cast(GrabFoodReport.tanggal_dibuat, db.Date).label('transaction_date'),
            func.sum(GrabFoodReport.amount).label('total_gross'),
            func.sum(GrabFoodReport.total).label('total_net')
        ).group_by(
            GrabFoodReport.outlet_code,
            func.cast(GrabFoodReport.tanggal_dibuat, db.Date)
        ).all()

        total_processed = 0
        total_updated = 0
        total_created = 0

        # Update or insert daily totals
        for total in daily_totals:
            existing_total = DailyMerchantTotal.query.filter_by(
                outlet_id=total.outlet_code,
                date=total.transaction_date,
                report_type='grab'
            ).first()

            if existing_total:
                existing_total.total_gross = total.total_gross
                existing_total.total_net = total.total_net
                existing_total.updated_at = datetime.utcnow()
                total_updated += 1
            else:
                new_total = DailyMerchantTotal(
                    outlet_id=total.outlet_code,
                    date=total.transaction_date,
                    report_type='grab',
                    total_gross=total.total_gross,
                    total_net=total.total_net
                )
                db.session.add(new_total)
                total_created += 1

            total_processed += 1

        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Grab daily totals consolidated successfully',
            'statistics': {
                'total_processed': total_processed,
                'total_updated': total_updated,
                'total_created': total_created
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@daily_merchant_totals_bp.route('/consolidate/shopee', methods=['POST'])
def consolidate_shopee_totals():
    try:
        # Get daily totals from Grab reports
        daily_totals = db.session.query(
            ShopeeReport.outlet_code,
            func.cast(ShopeeReport.order_create_time, db.Date).label('transaction_date'),
            func.sum(ShopeeReport.order_amount).label('total_gross'),
            func.sum(ShopeeReport.net_income).label('total_net')
        ).filter(ShopeeReport.order_status != "Cancelled").group_by(
            ShopeeReport.outlet_code,
            func.cast(ShopeeReport.order_create_time, db.Date)
        ).all()

        total_processed = 0
        total_updated = 0
        total_created = 0

        # Update or insert daily totals
        for total in daily_totals:
            existing_total = DailyMerchantTotal.query.filter_by(
                outlet_id=total.outlet_code,
                date=total.transaction_date,
                report_type='shopee'
            ).first()

            if existing_total:
                existing_total.total_gross = total.total_gross
                existing_total.total_net = total.total_net
                existing_total.updated_at = datetime.utcnow()
                total_updated += 1
            else:
                new_total = DailyMerchantTotal(
                    outlet_id=total.outlet_code,
                    date=total.transaction_date,
                    report_type='shopee',
                    total_gross=total.total_gross,
                    total_net=total.total_net
                )
                db.session.add(new_total)
                total_created += 1

            total_processed += 1

        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Shopee daily totals consolidated successfully',
            'statistics': {
                'total_processed': total_processed,
                'total_updated': total_updated,
                'total_created': total_created
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@daily_merchant_totals_bp.route('/consolidate/shopeepay', methods=['POST'])
def consolidate_shopeepay_totals():
    try:
        # Get daily totals from Grab reports
        daily_totals = db.session.query(
            ShopeepayReport.outlet_code,
            func.cast(ShopeepayReport.create_time, db.Date).label('transaction_date'),
            func.sum(ShopeepayReport.transaction_amount).label('total_gross'),
            func.sum(ShopeepayReport.settlement_amount).label('total_net')
        ).filter(ShopeepayReport.transaction_type != "Withdrawal").group_by(
            ShopeepayReport.outlet_code,
            func.cast(ShopeepayReport.create_time, db.Date)
        ).all()

        total_processed = 0
        total_updated = 0
        total_created = 0

        # Update or insert daily totals
        for total in daily_totals:
            existing_total = DailyMerchantTotal.query.filter_by(
                outlet_id=total.outlet_code,
                date=total.transaction_date,
                report_type='shopeepay'
            ).first()

            if existing_total:
                existing_total.total_gross = total.total_gross
                existing_total.total_net = total.total_net
                existing_total.updated_at = datetime.utcnow()
                total_updated += 1
            else:
                new_total = DailyMerchantTotal(
                    outlet_id=total.outlet_code,
                    date=total.transaction_date,
                    report_type='shopeepay',
                    total_gross=total.total_gross,
                    total_net=total.total_net
                )
                db.session.add(new_total)
                total_created += 1

            total_processed += 1

        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'ShopeePay daily totals consolidated successfully',
            'statistics': {
                'total_processed': total_processed,
                'total_updated': total_updated,
                'total_created': total_created
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500




@daily_merchant_totals_bp.route('/consolidate/gojek', methods=['POST'])
def consolidate_gojek_totals():
    try:
        # Get daily totals from Gojek reports
        daily_totals = db.session.query(
            GojekReport.outlet_code,
            GojekReport.transaction_date,
            func.sum(GojekReport.amount).label('total_gross'),
            func.sum(GojekReport.nett_amount).label('total_net')
        ).group_by(
            GojekReport.outlet_code,
            GojekReport.transaction_date
        ).all()

        total_processed = 0
        total_updated = 0
        total_created = 0

        # Update or insert daily totals
        for total in daily_totals:
            existing_total = DailyMerchantTotal.query.filter_by(
                outlet_id=total.outlet_code,
                date=total.transaction_date,
                report_type='gojek'
            ).first()

            if existing_total:
                existing_total.total_gross = total.total_gross
                existing_total.total_net = total.total_net
                existing_total.updated_at = datetime.utcnow()
                total_updated += 1
            else:
                new_total = DailyMerchantTotal(
                    outlet_id=total.outlet_code,
                    date=total.transaction_date,
                    report_type='gojek',
                    total_gross=total.total_gross,
                    total_net=total.total_net
                )
                db.session.add(new_total)
                total_created += 1

            total_processed += 1

        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Gojek daily totals consolidated successfully',
            'statistics': {
                'total_processed': total_processed,
                'total_updated': total_updated,
                'total_created': total_created
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500