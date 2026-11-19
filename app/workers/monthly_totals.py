"""Backfill monthly totals from existing daily totals."""

import argparse
import json
import sys
from datetime import date

from app.extensions import db
from app.services.consolidation_service import REBUILD_PLATFORMS
from app.services.monthly_totals_service import rebuild_monthly_totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', type=date.fromisoformat, required=True)
    parser.add_argument('--end-date', type=date.fromisoformat, required=True)
    parser.add_argument('--platform', action='append', choices=REBUILD_PLATFORMS)
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error('--start-date cannot be after --end-date')

    from app import create_app

    def progress(month, found, zeroed):
        print(f'{month}: {found} monthly partitions, {zeroed} stale partitions zeroed '
              '(not yet committed)', file=sys.stderr, flush=True)

    app = create_app()
    with app.app_context():
        try:
            counts = rebuild_monthly_totals(args.start_date, args.end_date,
                                            args.platform or REBUILD_PLATFORMS, progress)
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
    print(json.dumps({'start_date': args.start_date.isoformat(),
                      'end_date': args.end_date.isoformat(), 'monthly_partitions': counts}))


if __name__ == '__main__':
    main()
