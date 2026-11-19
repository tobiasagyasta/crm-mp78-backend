from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, or_

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.daily_mutation_totals import DailyMutationTotal
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.grab_disbursement import GrabDisbursement
from app.models.outlet import Outlet
from app.models.transaction_match import TransactionMatch
from app.services.consolidation_service import REBUILD_PLATFORMS

PLATFORM_NAMES = {'gojek': 'Gojek', 'grab': 'Grab', 'shopee': 'ShopeeFood'}


def rebuild_daily_mutation_totals(start_date, end_date, platforms=REBUILD_PLATFORMS):
    """Rebuild from manual matches, unique SID/H+1 candidates, and Grab links.

    No match records are written. Ambiguous candidates remain unattributed.
    """
    if not isinstance(start_date, date) or not isinstance(end_date, date) or start_date > end_date:
        raise ValueError('Invalid date range')
    platforms = tuple(dict.fromkeys(platforms))
    if not platforms or any(platform not in REBUILD_PLATFORMS for platform in platforms):
        raise ValueError('Unsupported rebuild platform')
    bank_start, bank_end = start_date + timedelta(days=1), end_date + timedelta(days=1)
    reverse = {PLATFORM_NAMES[p]: p for p in platforms}
    mutations = (BankMutation.query.filter(
        BankMutation.platform_name.in_(reverse),
        BankMutation.tanggal >= bank_start, BankMutation.tanggal <= bank_end,
        or_(BankMutation.transaction_type.is_(None),
            func.trim(BankMutation.transaction_type) == '',
            func.upper(func.trim(BankMutation.transaction_type)) == 'CR'),
    ).all())
    links = defaultdict(set)
    ids = [mutation.id for mutation in mutations]
    id_set = set(ids)
    manual_query = db.session.query(
        TransactionMatch.mutation_id, TransactionMatch.platform,
        TransactionMatch.daily_total_outlet_id, TransactionMatch.daily_total_date,
        TransactionMatch.daily_total_report_type,
    ).filter(
        TransactionMatch.platform.in_([p for p in platforms if p != 'grab']),
        TransactionMatch.status == 'manual_matched',
    )
    manual_rows = list(manual_query.filter(
        TransactionMatch.daily_total_date >= start_date,
        TransactionMatch.daily_total_date <= end_date,
    ).all())
    for start in range(0, len(ids), 500):
        manual_rows.extend(manual_query.filter(TransactionMatch.mutation_id.in_(ids[start:start + 500])).all())
    manual_ids = {row[0] for row in manual_rows if row[0] is not None}
    for mutation_id, platform, outlet, day, report_type in manual_rows:
        if outlet and day and platform == report_type:
            if mutation_id in id_set:
                links[mutation_id].add((platform, outlet, day, 'manual'))
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        payouts = (db.session.query(
            GrabDisbursement.mutation_id, GrabDisbursement.transfer_date, Outlet.outlet_code,
        ).outerjoin(Outlet, Outlet.store_id_grab == GrabDisbursement.store_id)
            .filter(GrabDisbursement.mutation_id.in_(chunk),
                    func.lower(func.trim(GrabDisbursement.status)) == 'completed').all())
        for mutation_id, transfer, outlet in payouts:
            if outlet and transfer:
                links[mutation_id].add(('grab', outlet, transfer.date() - timedelta(days=1), 'disbursement'))

    code_index = defaultdict(set)
    daily_rows = (db.session.query(
        DailyMerchantTotal.outlet_id, DailyMerchantTotal.date, DailyMerchantTotal.report_type,
        Outlet.store_id_gojek, Outlet.store_id_shopee,
    ).join(Outlet, Outlet.outlet_code == DailyMerchantTotal.outlet_id)
        .filter(DailyMerchantTotal.date >= start_date, DailyMerchantTotal.date <= end_date,
                DailyMerchantTotal.report_type.in_([p for p in platforms if p != 'grab'])).all())
    for outlet, day, platform, gojek_id, shopee_id in daily_rows:
        store_id = (gojek_id if platform == 'gojek' else shopee_id) or ''
        store_id = store_id.strip()
        if not store_id:
            continue
        codes = {store_id} if platform == 'gojek' else {store_id[-4:], store_id[-5:]}
        for code in codes:
            code_index[(platform, day + timedelta(days=1), code)].add((platform, outlet, day))
    candidates = {}
    for mutation in mutations:
        platform = reverse[mutation.platform_name]
        code = (mutation.platform_code or '').strip()
        if platform == 'grab' or not code or mutation.id in manual_ids:
            continue
        choices = code_index.get((platform, mutation.tanggal, code), set())
        candidates[mutation.id] = choices
    for mutation_id, choices in candidates.items():
        if len(choices) == 1:
            choice = next(iter(choices))
            links[mutation_id].add((*choice, 'sid_h1'))

    outlet_codes = sorted({link[1] for entries in links.values() for link in entries})
    brands = {}
    for offset in range(0, len(outlet_codes), 500):
        codes = outlet_codes[offset:offset + 500]
        brands.update(db.session.query(Outlet.outlet_code, Outlet.brand)
                      .filter(Outlet.outlet_code.in_(codes)).all())

    groups = {}
    for mutation in mutations:
        platform = reverse[mutation.platform_name]
        candidates = links.get(mutation.id, set())
        # Conflicting evidence is not safe to attribute to any outlet.
        link = next(iter(candidates)) if len(candidates) == 1 else None
        if link and link[0] == platform:
            _, outlet, transaction_day, attribution = link
            key = attribution + ':' + outlet
            brand = brands.get(outlet)
        else:
            outlet, brand = None, None
            transaction_day = mutation.tanggal - timedelta(days=1)
            attribution, key = 'unattributed', 'unattributed'
        group_key = (mutation.tanggal, platform, transaction_day, key)
        group = groups.setdefault(group_key, {
            'attribution': attribution, 'outlet_code': outlet, 'brand': brand,
            'amount': Decimal(0), 'mutation_count': 0, 'missing_amount_count': 0,
        })
        group['mutation_count'] += 1
        if mutation.transaction_amount is None:
            group['missing_amount_count'] += 1
        else:
            group['amount'] += Decimal(str(mutation.transaction_amount))

    (DailyMutationTotal.query.filter(
        DailyMutationTotal.bank_date >= bank_start,
        DailyMutationTotal.bank_date <= bank_end,
        DailyMutationTotal.platform.in_(platforms),
    ).delete(synchronize_session=False))
    for (bank_date, platform, transaction_day, key), values in groups.items():
        db.session.add(DailyMutationTotal(
            bank_date=bank_date, platform=platform, transaction_date=transaction_day,
            attribution_key=key, updated_at=datetime.utcnow(), **values))
    return {'bank_start_date': bank_start.isoformat(), 'bank_end_date': bank_end.isoformat(),
            'mutations': len(mutations), 'partitions': len(groups)}
