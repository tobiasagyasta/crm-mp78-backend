"""Backfill daily mutation totals without rebuilding platform report totals."""

import argparse
import json
import sys
from datetime import date

from app.extensions import db
from app.services.consolidation_service import REBUILD_PLATFORMS
from app.services.mutation_totals_service import rebuild_daily_mutation_totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', type=date.fromisoformat, required=True)
    parser.add_argument('--end-date', type=date.fromisoformat, required=True)
    parser.add_argument('--platform', action='append', choices=REBUILD_PLATFORMS)
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error('--start-date cannot be after --end-date')
    from app import create_app

    print('Rebuilding mutation totals; changes are not committed yet.', file=sys.stderr, flush=True)
    with create_app().app_context():
        try:
            result = rebuild_daily_mutation_totals(
                args.start_date, args.end_date, args.platform or REBUILD_PLATFORMS)
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
    print(json.dumps(result))


if __name__ == '__main__':
    main()
