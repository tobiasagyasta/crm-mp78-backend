from datetime import datetime, timedelta
from collections import defaultdict
from app.extensions import db
from app.models.outlet import Outlet
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.grab_disbursement import GrabDisbursement
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.models.mp78_mutations import MP78Mutation
from app.models.shopeepay_reports import ShopeepayReport
from app.models.tiktok_reports import TiktokReport
from app.models.qpon_reports import QponReport
from app.models.bank_mutations import BankMutation
from app.models.rekening import Rekening
from app.models.webshop_report import WebshopReport
from app.models.maxim_report import MaximReport
from app.models.pukis import Pukis
from app.models.ultra_voucher import VoucherReport
from app.models.income_category import IncomeCategory
from app.models.expense_category import ExpenseCategory
from app.services.excel_export import mpr_calculations as mpr_calc
from app.utils.transaction_matcher import TransactionMatcher
from app.utils.pkb_mutation import get_minus_manual_entries
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased

GRAB_REPORTS_TRANSFERRED_ONLY = False
GRAB_TRANSFERRED_STATUSES = ('Transferred', 'Ditransfer')
EXCLUDE_COMPLETED_GRAB_REPORTS = False
COMPLETED_GRAB_STATUSES = ('Selesai', 'Completed')
SHOW_MUTATIONS_WITHOUT_PLATFORM_DATA = True
# True: match Grab bank mutations only against completed disbursements.
# False: fall back to the legacy matcher on dates without a disbursement.
GRAB_DISBURSEMENT_FIRST_MATCHING = False
GRAB_DISBURSEMENT_MATCH_TOLERANCE = 5

def _grab_report_datetime_col():
    return func.coalesce(GrabFoodReport.diperbarui_pada, GrabFoodReport.tanggal_dibuat)

def _grab_report_date(report):
    return report.diperbarui_pada or report.tanggal_dibuat

def _is_transferred_grab_report(report):
    return (report.status or '').strip() in GRAB_TRANSFERRED_STATUSES

def _is_completed_grab_report(report):
    return (report.status or '').strip() in COMPLETED_GRAB_STATUSES

def _matching_completed_grab_report_ids(reports, daily_totals):
    transferred_totals_by_date = defaultdict(lambda: defaultdict(int))

    for report in reports:
        report_datetime = _grab_report_date(report)
        if not report_datetime:
            continue

        date = report_datetime.date()
        if date not in daily_totals:
            continue

        total = float(report.total or 0)
        if _is_transferred_grab_report(report):
            transferred_totals_by_date[date][round(total, 2)] += 1

    completed_report_ids = set()
    for report in reports:
        report_datetime = _grab_report_date(report)
        if not report_datetime:
            continue

        date = report_datetime.date()
        if date not in daily_totals:
            continue
        if not _is_completed_grab_report(report):
            continue

        total = round(float(report.total or 0), 2)
        if transferred_totals_by_date[date][total] > 0:
            completed_report_ids.add(id(report))
            transferred_totals_by_date[date][total] -= 1

    return completed_report_ids


def _get_grab_disbursements(store_id, start_date, end_date):
    if not store_id:
        return []
    transfer_start = datetime.combine(start_date.date() + timedelta(days=1), datetime.min.time())
    transfer_end = datetime.combine(end_date.date() + timedelta(days=2), datetime.min.time())
    return GrabDisbursement.query.filter(
        GrabDisbursement.store_id == store_id,
        GrabDisbursement.transfer_date >= transfer_start,
        GrabDisbursement.transfer_date < transfer_end,
        func.lower(func.trim(GrabDisbursement.status)) == 'completed',
    ).all()


def _match_grab_disbursement_mutations(daily_totals, reports, commit=True):
    """Persist closest one-to-one matches across shared accounts within Rp5."""
    if not reports:
        return 0
    accounts = {report.rekening_id: report.rekening.rekening_number for report in reports}
    first = min(report.transfer_date.date() for report in reports)
    last = max(report.transfer_date.date() for report in reports)
    # Already-linked reports need no candidate or competing-payout search.
    if all(report.mutation_id is not None for report in reports):
        linked = BankMutation.query.filter(
            BankMutation.id.in_([report.mutation_id for report in reports]),
            BankMutation.platform_name == 'Grab',
            BankMutation.tanggal >= first,
            BankMutation.tanggal <= last,
            or_(BankMutation.transaction_type.is_(None), func.upper(func.trim(BankMutation.transaction_type)) == 'CR'),
            BankMutation.transaction_amount.isnot(None),
        ).all()
        matched = {report.id: mutation for report in reports for mutation in linked
                   if report.mutation_id == mutation.id}
        return _apply_grab_disbursement_matches(daily_totals, reports, matched)
    competitors = (GrabDisbursement.query.filter(
        GrabDisbursement.rekening_id.in_(accounts),
        GrabDisbursement.transfer_date >= datetime.combine(first, datetime.min.time()),
        GrabDisbursement.transfer_date < datetime.combine(last + timedelta(days=1), datetime.min.time()),
        func.lower(func.trim(GrabDisbursement.status)) == 'completed',
    ).order_by(GrabDisbursement.id).with_for_update().all())
    mutations = BankMutation.query.filter(
        BankMutation.platform_name == 'Grab',
        BankMutation.rekening_number.in_(list(accounts.values())),
        BankMutation.tanggal >= first,
        BankMutation.tanggal <= last,
        or_(BankMutation.transaction_type.is_(None), func.upper(func.trim(BankMutation.transaction_type)) == 'CR'),
        BankMutation.transaction_amount.isnot(None),
    ).order_by(BankMutation.id).with_for_update().all()
    # Links can belong to payouts outside this account/date selection. Exclude
    # them regardless of the linked payout's status or the mutation direction.
    candidate_ids = [mutation.id for mutation in mutations]
    globally_linked_ids = set()
    for offset in range(0, len(candidate_ids), 500):
        globally_linked_ids.update(
            value for (value,) in db.session.query(GrabDisbursement.mutation_id)
            .filter(GrabDisbursement.mutation_id.in_(candidate_ids[offset:offset + 500])).all()
        )

    groups = defaultdict(lambda: [[], []])
    for report in competitors:
        groups[(accounts[report.rekening_id], report.transfer_date.date())][0].append(report)
    for mutation in mutations:
        groups[(mutation.rekening_number, mutation.tanggal)][1].append(mutation)

    # Existing cross-account links must remain visible in mixed linked/unlinked
    # batches too, not only in the all-linked fast path.
    missing_link_ids = {p.mutation_id for p in competitors if p.mutation_id is not None}
    missing_link_ids.difference_update(mutation.id for mutation in mutations)
    if missing_link_ids:
        mutations.extend(BankMutation.query.filter(
            BankMutation.id.in_(missing_link_ids), BankMutation.platform_name == 'Grab',
            BankMutation.tanggal >= first, BankMutation.tanggal <= last,
            or_(BankMutation.transaction_type.is_(None),
                func.upper(func.trim(BankMutation.transaction_type)) == 'CR'),
            BankMutation.transaction_amount.isnot(None),
        ).all())
    mutation_by_id = {mutation.id: mutation for mutation in mutations}
    matched = {p.id: mutation_by_id[p.mutation_id] for p in competitors
               if p.mutation_id in mutation_by_id}
    linked_ids = globally_linked_ids | {
        p.mutation_id for p in competitors if p.mutation_id is not None
    }
    changed = False
    changed_bank_dates = set()
    for payouts, credits in groups.values():
        candidates = []
        for payout in payouts:
            if payout.mutation_id is not None or payout.net_amount is None:
                continue
            for credit in credits:
                if credit.id in linked_ids or (credit.platform_code or '').strip():
                    continue
                difference = abs(payout.net_amount - credit.transaction_amount)
                if difference <= GRAB_DISBURSEMENT_MATCH_TOLERANCE:
                    candidates.append((difference, payout.id, credit.id, payout, credit))
        # Closest amounts win; IDs make ties deterministic.
        for _, _, _, payout, credit in sorted(candidates, key=lambda candidate: candidate[:3]):
            if payout.mutation_id is not None or credit.id in linked_ids:
                continue
            payout.mutation_id = credit.id
            credit.platform_code = payout.store_id
            linked_ids.add(credit.id)
            matched[payout.id] = credit
            changed = True
            changed_bank_dates.add(credit.tanggal)

    # Only unmatched payouts enter the fallback. Historical coded Grab credits
    # establish which alternate full account numbers have been used by a store.
    unmatched = [payout for payout in competitors if payout.mutation_id is None]
    if unmatched:
        stores = {payout.store_id for payout in unmatched if payout.store_id}
        history = (db.session.query(BankMutation.platform_code, BankMutation.rekening_number)
                   .filter(BankMutation.platform_name == 'Grab',
                           BankMutation.platform_code.in_(stores),
                           BankMutation.rekening_number.isnot(None))
                   .distinct().all())
        store_accounts = defaultdict(set)
        for store_id, number in history:
            if number and number.strip():
                store_accounts[store_id].add(number)
        alternate_accounts = {
            number for payout in unmatched for number in store_accounts[payout.store_id]
            if number != accounts[payout.rekening_id]
        }
        if alternate_accounts:
            alternate_credits = (BankMutation.query.filter(
                BankMutation.platform_name == 'Grab',
                BankMutation.rekening_number.in_(alternate_accounts),
                BankMutation.tanggal >= first, BankMutation.tanggal <= last,
                or_(BankMutation.transaction_type.is_(None),
                    func.upper(func.trim(BankMutation.transaction_type)) == 'CR'),
                BankMutation.transaction_amount.isnot(None),
                or_(BankMutation.platform_code.is_(None),
                    func.trim(BankMutation.platform_code) == ''),
            ).order_by(BankMutation.id).with_for_update().all())
            # A credit may already be linked to a payout outside the original
            # account group; never attempt to use it again.
            alternate_ids = [credit.id for credit in alternate_credits]
            already_linked = set()
            for offset in range(0, len(alternate_ids), 500):
                already_linked.update(
                    value for (value,) in db.session.query(GrabDisbursement.mutation_id)
                    .filter(GrabDisbursement.mutation_id.in_(alternate_ids[offset:offset + 500])).all()
                )
            candidate_payouts = defaultdict(list)
            candidate_credits = defaultdict(list)
            for payout in unmatched:
                if payout.net_amount is None:
                    continue
                for credit in alternate_credits:
                    if (credit.id in linked_ids or credit.id in already_linked
                            or credit.rekening_number == accounts[payout.rekening_id]
                            or credit.rekening_number not in store_accounts[payout.store_id]
                            or credit.tanggal != payout.transfer_date.date()):
                        continue
                    if abs(payout.net_amount - credit.transaction_amount) <= GRAB_DISBURSEMENT_MATCH_TOLERANCE:
                        candidate_payouts[credit.id].append(payout)
                        candidate_credits[payout.id].append(credit)
            for payout in unmatched:
                choices = candidate_credits[payout.id]
                if len(choices) != 1:
                    continue
                credit = choices[0]
                if len(candidate_payouts[credit.id]) != 1:
                    continue
                payout.mutation_id = credit.id
                credit.platform_code = payout.store_id
                linked_ids.add(credit.id)
                matched[payout.id] = credit
                changed = True
                changed_bank_dates.add(credit.tanggal)

    if changed:
        from app.services.mutation_totals_service import rebuild_daily_mutation_totals
        db.session.flush()
        for bank_date in sorted(changed_bank_dates):
            transaction_date = bank_date - timedelta(days=1)
            rebuild_daily_mutation_totals(transaction_date, transaction_date, ['grab'])
        if commit:
            db.session.commit()

    return _apply_grab_disbursement_matches(daily_totals, reports, matched)


def _apply_grab_disbursement_matches(daily_totals, reports, matched):
    count = 0
    for report in reports:
        mutation = matched.get(report.id)
        if mutation:
            count += 1
        day = report.transfer_date.date() - timedelta(days=1)
        if mutation and day in daily_totals:
            totals = daily_totals[day]
            totals['Grab_Mutation'] = (totals['Grab_Mutation'] or 0) + float(mutation.transaction_amount)
    return count

def get_report_data(outlet_code: str, start_date: datetime, end_date: datetime) -> dict:
    """
    Fetches and prepares all data required for the Excel report.
    """
    end_date_inclusive = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)

    outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
    if not outlet:
        raise ValueError("Outlet not found")

    # Initialize daily totals
    daily_totals = {}
    all_dates = []
    date_iter = start_date.date()
    while date_iter <= end_date.date():
        all_dates.append(date_iter)
        daily_totals[date_iter] = _init_daily_total()
        date_iter += timedelta(days=1)

    # Fetch all reports
    gojek_reports = GojekReport.query.filter(GojekReport.outlet_code == outlet_code, GojekReport.transaction_date >= start_date, GojekReport.transaction_date <= end_date_inclusive).all()
    grab_date_col = _grab_report_datetime_col()
    grab_query = GrabFoodReport.query.filter(
        GrabFoodReport.outlet_code == outlet_code,
        grab_date_col >= start_date,
        grab_date_col <= end_date_inclusive,
    )
    if GRAB_REPORTS_TRANSFERRED_ONLY:
        grab_query = grab_query.filter(GrabFoodReport.status.in_(GRAB_TRANSFERRED_STATUSES))
    if EXCLUDE_COMPLETED_GRAB_REPORTS:
        grab_query = grab_query.filter(or_(GrabFoodReport.status.is_(None), ~GrabFoodReport.status.in_(COMPLETED_GRAB_STATUSES)))
    grab_reports = grab_query.all()
    grab_disbursements = _get_grab_disbursements(outlet.store_id_grab, start_date, end_date)
    shopee_reports = ShopeeReport.query.filter(ShopeeReport.outlet_code == outlet_code, ShopeeReport.order_create_time >= start_date, ShopeeReport.order_create_time <= end_date_inclusive).all()
    shopeepay_reports = ShopeepayReport.query.filter(ShopeepayReport.outlet_code == outlet_code, ShopeepayReport.create_time >= start_date, ShopeepayReport.create_time <= end_date_inclusive).all()
    tiktok_reports = TiktokReport.query.filter(TiktokReport.outlet_code == outlet_code, TiktokReport.order_time >= start_date, TiktokReport.order_time <= end_date_inclusive).all()
    tiktok_closing_reports = []
    closing_shift_days = 7 if (outlet.brand or '').strip().upper() in ('MP78', 'MPR') else 0
    if mpr_calc.ENABLE_CLOSING_TIKTOK_SETTLEMENT_SHIFT:
        if closing_shift_days:
            tiktok_closing_reports = TiktokReport.query.filter(TiktokReport.outlet_code == outlet_code, TiktokReport.order_time >= start_date - timedelta(days=closing_shift_days), TiktokReport.order_time <= end_date_inclusive - timedelta(days=closing_shift_days)).all()
        else:
            tiktok_closing_reports = tiktok_reports
    qpon_reports = QponReport.query.filter(QponReport.outlet_code == outlet_code, QponReport.bill_created_at >= start_date, QponReport.bill_created_at <= end_date_inclusive).all()
    qpon_closing_shift = closing_shift_days
    qpon_closing_reports = qpon_reports
    if qpon_closing_shift:
        qpon_closing_reports = QponReport.query.filter(QponReport.outlet_code == outlet_code, QponReport.bill_created_at >= start_date - timedelta(days=qpon_closing_shift), QponReport.bill_created_at <= end_date_inclusive - timedelta(days=qpon_closing_shift)).all()
    webshop_reports = WebshopReport.query.filter(WebshopReport.outlet_code == outlet_code, WebshopReport.created_at >= start_date, WebshopReport.created_at <= end_date_inclusive).all()
    maxim_reports = MaximReport.query.filter(MaximReport.outlet_code == outlet_code, MaximReport.created_at >= start_date, MaximReport.created_at <= end_date_inclusive).all()
    uv_reports = VoucherReport.query.filter(VoucherReport.outlet_code == outlet_code, VoucherReport.order_date >= start_date, VoucherReport.order_date <= end_date_inclusive).all()
    cash_income_reports = CashReport.query.filter(CashReport.outlet_code == outlet_code, CashReport.type == 'income', CashReport.tanggal >= start_date, CashReport.tanggal <= end_date_inclusive).all()
    cash_expense_reports = CashReport.query.filter(CashReport.outlet_code == outlet_code, CashReport.type == 'expense', CashReport.tanggal >= start_date, CashReport.tanggal <= end_date_inclusive).all()

    pukis_reports = []
    if outlet.brand == "Pukis & Martabak Kota Baru":
        pukis_reports = Pukis.query.filter(Pukis.outlet_code == outlet_code, Pukis.tanggal >= start_date, Pukis.tanggal <= end_date_inclusive).order_by(Pukis.tanggal.asc()).all()

    # Aggregate data
    _aggregate_gojek(daily_totals, gojek_reports)
    _aggregate_uv(daily_totals, uv_reports)
    grab_totals = _aggregate_grab(daily_totals, grab_reports, outlet.brand)
    _aggregate_grab_disbursements(daily_totals, grab_disbursements)
    _match_grab_disbursement_mutations(daily_totals, grab_disbursements)
    _aggregate_shopee(daily_totals, shopee_reports)
    _aggregate_shopeepay(daily_totals, shopeepay_reports)
    _aggregate_tiktok(daily_totals, tiktok_reports)
    _aggregate_tiktok_closing(daily_totals, tiktok_closing_reports, shift_days=closing_shift_days)
    _aggregate_qpon(daily_totals, qpon_reports)
    _aggregate_qpon_mutations(daily_totals, qpon_reports)
    _aggregate_qpon_closing(daily_totals, qpon_closing_reports, shift_days=qpon_closing_shift)
    _aggregate_qpon_mutations(daily_totals, qpon_closing_reports, closing=True, shift_days=qpon_closing_shift)
    _aggregate_webshop(daily_totals, webshop_reports)
    _aggregate_webshop(daily_totals, maxim_reports, platform='Maxim')
    _aggregate_cash(daily_totals, cash_income_reports, cash_expense_reports)

    # Match mutations
    _match_mutations(daily_totals, outlet_code, start_date, end_date)

    # Get minusan entries
    minusan_entries = get_minus_manual_entries(outlet_code, start_date.date(), end_date_inclusive.date())
    minusan_by_date = defaultdict(float)
    for entry in minusan_entries:
        d = getattr(entry, 'minus_date', None)
        if d:
            minusan_by_date[d] += float(entry.amount or 0) * -1

    # Fetch manual entries
    IncomeCat = aliased(IncomeCategory)
    ExpenseCat = aliased(ExpenseCategory)
    manual_entries = (
        ManualEntry.query
        .filter(
            ManualEntry.outlet_code == outlet_code,
            ManualEntry.start_date <= end_date,
            ManualEntry.end_date >= start_date,
            # ~ManualEntry.description.ilike('%minus%')
        )
        .outerjoin(IncomeCat, (ManualEntry.category_id == IncomeCat.id) & (ManualEntry.entry_type == 'income'))
        .outerjoin(ExpenseCat, (ManualEntry.category_id == ExpenseCat.id) & (ManualEntry.entry_type == 'expense'))
        .add_entity(IncomeCat)
        .add_entity(ExpenseCat)
        .all()
    )

    mp78_mutations = (
        MP78Mutation.query
        .filter(
            MP78Mutation.outlet_code == outlet_code,
            MP78Mutation.tanggal >= start_date.date(),
            MP78Mutation.tanggal <= end_date_inclusive.date(),
        )
        .order_by(MP78Mutation.tanggal.asc(), MP78Mutation.id.asc())
        .all()
    )

    grand_totals = _calculate_grand_totals(daily_totals)

    return {
        "outlet": outlet,
        "start_date": start_date,
        "end_date": end_date,
        "all_dates": all_dates,
        "daily_totals": daily_totals,
        "grand_totals": grand_totals,
        "manual_entries": manual_entries,
        "mp78_mutations": mp78_mutations,
        "pukis_reports": pukis_reports,
        "minusan_by_date": minusan_by_date,
        **grab_totals
    }

def _init_daily_total():
    return {
        'Gojek_QRIS' : 0,'Gojek_Gross': 0, 'Gojek_Net': 0, 'Grab_Gross': 0, 'Grab_Net': 0, 'GrabOVO_Gross': 0, 'GrabOVO_Net': 0,
        'ShopeePay_Gross': 0, 'ShopeePay_Net': 0, 'Shopee_Gross': 0, 'Shopee_Net': 0,
        'Tiktok_Gross': 0, 'Tiktok_Net': 0, 'Tiktok_Settlement_Time': set(),
        'Tiktok_Closing_Gross': 0, 'Tiktok_Closing_Net': 0,
        'Qpon_Gross': 0, 'Qpon_Net': 0, 'Qpon_Mutation': None, 'Qpon_Difference': None,
        'Qpon_Closing_Gross': 0, 'Qpon_Closing_Net': 0, 'Qpon_Closing_Mutation': None,
        'Webshop_Gross': 0, 'Webshop_Net': 0,
        'Maxim_Gross': 0, 'Maxim_Net': 0,
        'Cash_Income': 0, 'Cash_Expense': 0,
        'Gojek_Mutation': None, 'Gojek_Difference': 0, 'Grab_Difference': 0, 'Grab_Disbursement': None, 'Grab_Mutation': None,
        'Grab_Commission': 0, 'Shopee_Mutation': None, 'Shopee_Difference': 0,
        'ShopeePay_Mutation': None, 'ShopeePay_Difference': 0, 'UV': 0
    }

def _aggregate_grab_disbursements(daily_totals, reports):
    for report in reports:
        transaction_date = report.transfer_date.date() - timedelta(days=1)
        if transaction_date in daily_totals:
            totals = daily_totals[transaction_date]
            totals['Grab_Disbursement'] = (totals['Grab_Disbursement'] or 0) + float(report.net_amount)


def _aggregate_gojek(daily_totals, reports):
    for report in reports:
        date = report.transaction_date
        daily_totals[date]['Gojek_Net'] += float(report.nett_amount or 0)
        daily_totals[date]['Gojek_Gross'] += float(report.amount or 0)

        # Add Gojek_QRIS logic
        if report.payment_type == 'QRIS':
            daily_totals[date]['Gojek_QRIS'] += float(report.nett_amount or 0)

def _aggregate_uv(daily_totals, reports):
    for report in reports:
        date = report.order_date.date()
        daily_totals[date]['UV'] += float(report.nominal or 0) - 5000

def _aggregate_grab(daily_totals, reports, brand):
    grabfood_gross_total = 0
    grabovo_gross_total = 0
    grabfood_net_total = 0
    grabovo_net_total = 0
    completed_report_ids_to_skip = _matching_completed_grab_report_ids(reports, daily_totals)

    for report in reports:
        if GRAB_REPORTS_TRANSFERRED_ONLY and not _is_transferred_grab_report(report):
            continue
        if EXCLUDE_COMPLETED_GRAB_REPORTS and _is_completed_grab_report(report):
            continue

        report_datetime = _grab_report_date(report)
        if not report_datetime:
            continue

        date = report_datetime.date()
        if date not in daily_totals:
            continue
        if id(report) in completed_report_ids_to_skip:
            continue

        daily_totals[date]['Grab_Net'] += float(report.total or 0)
        daily_totals[date]['Grab_Gross'] += float(report.amount or 0)
        if hasattr(report, 'jenis'):
            if report.jenis == 'OVO':
                grabovo_gross_total += float(report.amount or 0)
                grabovo_net_total += float(report.total or 0)
                daily_totals[date]['GrabOVO_Net'] += float(report.total or 0)
                daily_totals[date]['GrabOVO_Gross'] += float(report.amount or 0)
            elif report.jenis == 'GrabFood':
                grabfood_gross_total += float(report.amount or 0)
                grabfood_net_total += float(report.total or 0)
    for date in daily_totals:
        if mpr_calc.is_mp78_brand(brand):
            daily_totals[date]['Grab_Commission'] = (
                daily_totals[date]['Grab_Net'] * mpr_calc.MP78_GRAB_MANAGEMENT_COMMISSION_RATE
            )
        elif brand not in ["Pukis & Martabak Kota Baru"]:
            daily_totals[date]['Grab_Commission'] = (
                daily_totals[date]['Grab_Net'] * mpr_calc.MANAGEMENT_COMMISSION_RATE
            )
        else:
            daily_totals[date]['Grab_Commission'] = 0
    return {
        "grabfood_gross_total": grabfood_gross_total, "grabovo_gross_total": grabovo_gross_total,
        "grabfood_net_total": grabfood_net_total, "grabovo_net_total": grabovo_net_total
    }

def _aggregate_shopee(daily_totals, reports):
    for report in reports:
        if report.order_status != "Cancelled":
            date = report.order_create_time.date()
            daily_totals[date]['Shopee_Net'] += float(report.net_income or 0)
            daily_totals[date]['Shopee_Gross'] += float(report.order_amount or 0)

def _aggregate_shopeepay(daily_totals, reports):
    for report in reports:
        if report.transaction_type != "Withdrawal":
            date = report.create_time.date()
            daily_totals[date]['ShopeePay_Net'] += float(report.settlement_amount or 0)
            daily_totals[date]['ShopeePay_Gross'] += float(report.transaction_amount or 0)

def _aggregate_tiktok(daily_totals, reports):
    for report in reports:
        date = report.order_time.date()
        daily_totals[date]['Tiktok_Net'] += float(report.net_amount or 0)
        daily_totals[date]['Tiktok_Gross'] += float(report.gross_amount or 0)
        if report.settlement_time:
            daily_totals[date]['Tiktok_Settlement_Time'].add(report.settlement_time.date())

def _aggregate_tiktok_closing(daily_totals, reports, shift_days=7):
    for report in reports:
        date = report.order_time.date() + timedelta(days=shift_days)
        if date not in daily_totals:
            continue

        daily_totals[date]['Tiktok_Closing_Net'] += float(report.net_amount or 0)
        daily_totals[date]['Tiktok_Closing_Gross'] += float(report.gross_amount or 0)

def _aggregate_qpon(daily_totals, reports):
    for report in reports:
        if not report.bill_created_at:
            continue

        date = report.bill_created_at.date()
        if date not in daily_totals:
            continue

        qpon_amount = float(report.gross_amount or 0)
        daily_totals[date]['Qpon_Net'] += qpon_amount
        daily_totals[date]['Qpon_Gross'] += float(report.gross_amount or 0)

def _aggregate_qpon_mutations(daily_totals, reports, closing=False, shift_days=7):
    offset = timedelta(days=shift_days if closing else 0)
    reports_by_billing_id = {
        report.billing_id: report
        for report in reports
        if report.billing_id and report.bill_created_at and report.bill_created_at.date() + offset in daily_totals
    }
    billing_ids = list(reports_by_billing_id)
    for start in range(0, len(billing_ids), 500):
        mutations = (BankMutation.query.filter(
            BankMutation.platform_name == 'Qpon',
            BankMutation.platform_code.in_(billing_ids[start:start + 500]),
        ).order_by(BankMutation.id).all())
        matched_ids = set()
        for mutation in mutations:
            billing_id = mutation.platform_code
            if billing_id in matched_ids:
                continue
            matched_ids.add(billing_id)
            report_date = reports_by_billing_id[billing_id].bill_created_at.date() + offset
            totals = daily_totals[report_date]
            mutation_key = 'Qpon_Closing_Mutation' if closing else 'Qpon_Mutation'
            totals[mutation_key] = (totals.get(mutation_key) or 0) + float(mutation.transaction_amount or 0)
            if not closing:
                totals['Qpon_Difference'] = totals[mutation_key] - totals.get('Qpon_Net', 0)

def _aggregate_qpon_closing(daily_totals, reports, shift_days=7):
    for report in reports:
        if not report.bill_created_at:
            continue

        date = report.bill_created_at.date() + timedelta(days=shift_days)
        if date not in daily_totals:
            continue

        qpon_amount = float(report.gross_amount or 0)
        daily_totals[date]['Qpon_Closing_Net'] += qpon_amount
        daily_totals[date]['Qpon_Closing_Gross'] += float(report.gross_amount or 0)

def _aggregate_webshop(daily_totals, reports, platform='Webshop'):
    for report in reports:
        if not report.created_at:
            continue

        date = report.created_at.date()
        if date not in daily_totals:
            continue

        net_amount = getattr(report, "nett_value", None)
        if net_amount is None:
            net_amount = getattr(report, "net_value", 0)

        daily_totals[date][f'{platform}_Net'] += float(net_amount or 0)
        daily_totals[date][f'{platform}_Gross'] += float(report.gross_value or 0)

def _aggregate_cash(daily_totals, income_reports, expense_reports):
    cash_income_temp = defaultdict(float)
    cash_expense_temp = defaultdict(float)
    for report in income_reports:
        cash_income_temp[report.tanggal.date()] += float(report.total or 0)
    for report in expense_reports:
        cash_expense_temp[report.tanggal.date()] += float(report.total or 0)
    for date in cash_income_temp:
        daily_totals[date]['Cash_Income'] = int(round(cash_income_temp[date]))
    for date in cash_expense_temp:
        daily_totals[date]['Cash_Expense'] = int(round(cash_expense_temp[date]))

def _match_mutations(daily_totals, outlet_code, start_date, end_date):
    platforms = ['gojek', 'shopee', 'shopeepay']
    if not GRAB_DISBURSEMENT_FIRST_MATCHING:
        platforms.insert(1, 'grab')
    for platform in platforms:
        try:
            matcher = TransactionMatcher(platform)
            mutations = matcher.get_mutations_query(start_date.date(), end_date.date()).all()
            if platform == 'grab':
                mutations = [mutation for mutation in mutations
                             if not (mutation.platform_code or '').strip()]
            match_context = matcher.build_match_context(mutations=mutations, outlet_codes=[outlet_code])
            for date, totals in daily_totals.items():
                if platform == 'grab' and totals.get('Grab_Disbursement') is not None:
                    continue
                net_key = f'{platform.capitalize()}_Net' if platform != 'shopeepay' else 'ShopeePay_Net'
                mutation_key = f'{platform.capitalize()}_Mutation' if platform != 'shopeepay' else 'ShopeePay_Mutation'
                diff_key = f'{platform.capitalize()}_Difference' if platform != 'shopeepay' else 'ShopeePay_Difference'
                net_amount = totals.get(net_key, 0)

                if net_amount == 0:
                    if not SHOW_MUTATIONS_WITHOUT_PLATFORM_DATA:
                        continue

                    mutation_data = matcher.match_mutation_without_daily_total(
                        outlet_code,
                        date,
                        mutations,
                        match_context,
                    )
                else:
                    class MockDailyTotal:
                        def __init__(self, outlet_id, date, total_net):
                            self.outlet_id = outlet_id
                            self.date = date
                            self.total_net = total_net

                    mock_total = MockDailyTotal(outlet_code, date, net_amount)
                    _, mutation_data = matcher.match_transactions(mock_total, mutations, match_context)

                if mutation_data:
                    mutation_amount = float(mutation_data.get('transaction_amount', 0))
                    totals[mutation_key] = mutation_amount
                    totals[diff_key] = mutation_amount - float(net_amount)
                else:
                    totals[mutation_key] = None
                    totals[diff_key] = None
        except Exception as e:
            print(f"Warning: Mutation matching failed for {platform}: {str(e)}")
            continue

def _calculate_grand_totals(daily_totals):
    grand_totals = defaultdict(float)
    for day_totals in daily_totals.values():
        for key, value in day_totals.items():
            if value is not None and isinstance(value, (int, float)):
                grand_totals[key] += value

    grand_totals['Cash_Income'] = int(round(grand_totals['Cash_Income']))
    grand_totals['Cash_Expense'] = int(round(grand_totals['Cash_Expense']))
    grand_totals['Cash_Difference'] = grand_totals['Cash_Income'] - grand_totals['Cash_Expense']

    return dict(grand_totals)
