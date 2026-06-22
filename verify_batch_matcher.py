import argparse
from datetime import datetime


def parse_date(value):
    return datetime.strptime(value, '%Y-%m-%d').date()


def cache_counts(db, TransactionMatch, platform, start_date, end_date):
    rows = db.session.query(
        TransactionMatch.status,
        db.func.count(TransactionMatch.id),
    ).filter(
        TransactionMatch.platform == platform,
        TransactionMatch.report_date >= start_date,
        TransactionMatch.report_date <= end_date,
    ).group_by(TransactionMatch.status).all()
    return {status: count for status, count in rows}


def duplicate_counts(db, TransactionMatch, platform, start_date, end_date):
    daily_duplicates = db.session.query(
        TransactionMatch.daily_total_outlet_id,
        TransactionMatch.daily_total_date,
        TransactionMatch.daily_total_report_type,
        db.func.count(TransactionMatch.id),
    ).filter(
        TransactionMatch.platform == platform,
        TransactionMatch.daily_total_date >= start_date,
        TransactionMatch.daily_total_date <= end_date,
        TransactionMatch.status != 'ignored',
        TransactionMatch.daily_total_outlet_id.isnot(None),
    ).group_by(
        TransactionMatch.daily_total_outlet_id,
        TransactionMatch.daily_total_date,
        TransactionMatch.daily_total_report_type,
    ).having(db.func.count(TransactionMatch.id) > 1).count()

    mutation_duplicates = db.session.query(
        TransactionMatch.mutation_id,
        db.func.count(TransactionMatch.id),
    ).filter(
        TransactionMatch.platform == platform,
        TransactionMatch.report_date >= start_date,
        TransactionMatch.report_date <= end_date,
        TransactionMatch.status != 'ignored',
        TransactionMatch.mutation_id.isnot(None),
    ).group_by(TransactionMatch.mutation_id).having(db.func.count(TransactionMatch.id) > 1).count()

    return {'daily_total_duplicates': daily_duplicates, 'mutation_duplicates': mutation_duplicates}


def protected_cache_snapshot(db, or_, TransactionMatch, platform, start_date, end_date, excluded_outlet_ids):
    rows = db.session.query(TransactionMatch).filter(
        TransactionMatch.platform == platform,
        TransactionMatch.report_date >= start_date,
        TransactionMatch.report_date <= end_date,
        or_(
            TransactionMatch.daily_total_outlet_id.is_(None),
            TransactionMatch.daily_total_outlet_id.notin_(excluded_outlet_ids),
        ),
    ).order_by(TransactionMatch.id).all()
    return [
        (
            row.id,
            row.status,
            row.daily_total_outlet_id,
            row.daily_total_date,
            row.daily_total_report_type,
            row.mutation_id,
            row.platform_code,
            row.report_date,
        )
        for row in rows
    ]


def main():
    parser = argparse.ArgumentParser(description='Verify optimized batch transaction matcher parity and persistence safety.')
    parser.add_argument('--platform', required=True, choices=['gojek', 'grab', 'shopee', 'shopeepay'])
    parser.add_argument('--start-date', required=True, type=parse_date)
    parser.add_argument('--end-date', required=True, type=parse_date)
    parser.add_argument('--platform-code')
    parser.add_argument('--persist-check', action='store_true', help='Run persist twice and verify cache row count is stable.')
    parser.add_argument(
        '--filtered-safety-check',
        action='store_true',
        help='Run a full rebuild, then a platform-code filtered rebuild and verify unrelated cached rows are unchanged.',
    )
    args = parser.parse_args()

    from sqlalchemy import or_

    from app import create_app
    from app.extensions import db
    from app.models.transaction_match import TransactionMatch
    from app.utils.transaction_matcher import TransactionMatcher

    app = create_app()
    with app.app_context():
        matcher = TransactionMatcher(args.platform)
        parity = matcher.verify_batch_parity(args.start_date, args.end_date, args.platform_code)
        print(f"parity checked={parity['checked']} mismatches={parity['mismatch_count']}")
        if parity['mismatches']:
            for mismatch in parity['mismatches'][:10]:
                print(mismatch)
            raise SystemExit(1)

        if args.persist_check:
            matcher.safe_rebuild_matches(args.start_date, args.end_date, args.platform_code)
            first_counts = cache_counts(db, TransactionMatch, args.platform, args.start_date, args.end_date)

            matcher.safe_rebuild_matches(args.start_date, args.end_date, args.platform_code)
            second_counts = cache_counts(db, TransactionMatch, args.platform, args.start_date, args.end_date)

            print(f'first_cache_counts={first_counts}')
            print(f'second_cache_counts={second_counts}')
            if first_counts != second_counts:
                raise SystemExit('Persistence is not idempotent: cache counts changed on repeated persist.')

            duplicates = duplicate_counts(db, TransactionMatch, args.platform, args.start_date, args.end_date)
            print(f'duplicate_counts={duplicates}')
            if duplicates['daily_total_duplicates'] or duplicates['mutation_duplicates']:
                raise SystemExit('Duplicate active transaction_matches rows detected.')

        if args.filtered_safety_check:
            if not args.platform_code:
                raise SystemExit('--filtered-safety-check requires --platform-code')

            matcher.safe_rebuild_matches(args.start_date, args.end_date)

            filtered_batch = matcher.match_batch(args.start_date, args.end_date, args.platform_code)
            filtered_outlet_ids = [total.outlet_id for total in filtered_batch['daily_totals'] if total.outlet_id]
            before = protected_cache_snapshot(
                db,
                or_,
                TransactionMatch,
                args.platform,
                args.start_date,
                args.end_date,
                filtered_outlet_ids,
            )
            matcher.safe_rebuild_matches(args.start_date, args.end_date, args.platform_code)
            after = protected_cache_snapshot(
                db,
                or_,
                TransactionMatch,
                args.platform,
                args.start_date,
                args.end_date,
                filtered_outlet_ids,
            )
            print(f'filtered_safety_protected_rows={len(before)}')
            if before != after:
                raise SystemExit('Filtered rebuild changed cached rows outside the filtered outlet scope.')


if __name__ == '__main__':
    main()
