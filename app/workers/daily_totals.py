"""Rebuild daily platform totals; invoke from a daily external scheduler."""

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from app.extensions import db
from app.services.consolidation_service import REBUILD_PLATFORMS, rebuild_daily_totals


def run_daily_totals(start_date, end_date, platforms=REBUILD_PLATFORMS, progress=None):
    """Run one transactional rebuild inside a Flask application context."""
    from app import create_app

    app = create_app()
    with app.app_context():
        try:
            counts = rebuild_daily_totals(start_date, end_date, platforms, progress=progress)
            if progress:
                progress('committing', None, None, None)
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
    return {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'partitions': counts,
    }


def terminal_progress():
    """Write human-readable progress to stderr, keeping stdout JSON-only."""
    last_update = time.monotonic()

    def report(event, platform, processed, total):
        nonlocal last_update
        now = time.monotonic()
        if event == 'platform_start':
            message = f'{platform}: finding partitions...'
        elif event == 'partitions_found':
            message = f'{platform}: found {total} partitions'
        elif event == 'partition_processed':
            if processed != total and processed % 100 != 0 and now - last_update < 5:
                return
            message = f'{platform}: processed {processed}/{total} partitions (not yet committed)'
        elif event == 'platform_complete':
            message = f'{platform}: processed {processed}/{total} partitions'
        elif event == 'committing':
            message = 'Committing all changes...'
        else:
            return
        print(message, file=sys.stderr, flush=True)
        last_update = now

    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', type=date.fromisoformat)
    parser.add_argument('--end-date', type=date.fromisoformat)
    parser.add_argument('--lookback-days', type=int, default=30,
                        help='Days through yesterday in Asia/Jakarta (default: 30)')
    parser.add_argument('--platform', action='append', choices=REBUILD_PLATFORMS,
                        help='Limit to a platform; may be repeated')
    args = parser.parse_args()
    if (args.start_date is None) != (args.end_date is None):
        parser.error('--start-date and --end-date must be provided together')
    if args.lookback_days < 1:
        parser.error('--lookback-days must be positive')
    end = args.end_date or (datetime.now(ZoneInfo('Asia/Jakarta')).date() - timedelta(days=1))
    start = args.start_date or (end - timedelta(days=args.lookback_days - 1))
    if start > end:
        parser.error('--start-date cannot be after --end-date')
    platforms = args.platform or REBUILD_PLATFORMS
    started = time.monotonic()
    print(f'Rebuilding {start} through {end}; platforms: {", ".join(dict.fromkeys(platforms))}',
          file=sys.stderr, flush=True)
    try:
        result = run_daily_totals(start, end, platforms, progress=terminal_progress())
    except Exception:
        print('Rebuild failed; changes were rolled back.', file=sys.stderr, flush=True)
        raise
    print(f'Rebuild committed in {time.monotonic() - started:.1f}s. '
          f'Partitions: {result["partitions"]}', file=sys.stderr, flush=True)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
