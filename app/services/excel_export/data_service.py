from datetime import datetime, timedelta
from collections import defaultdict
from app.models.outlet import Outlet
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.models.shopeepay_reports import ShopeepayReport
from app.models.tiktok_reports import TiktokReport
from app.models.pukis import Pukis
from app.models.ultra_voucher import VoucherReport
from app.models.income_category import IncomeCategory
from app.models.expense_category import ExpenseCategory
from app.utils.transaction_matcher import TransactionMatcher
from app.utils.pkb_mutation import get_minus_manual_entries
from sqlalchemy.orm import aliased

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
    grab_reports = GrabFoodReport.query.filter(GrabFoodReport.outlet_code == outlet_code, GrabFoodReport.tanggal_dibuat >= start_date, GrabFoodReport.tanggal_dibuat <= end_date_inclusive).all()
    shopee_reports = ShopeeReport.query.filter(ShopeeReport.outlet_code == outlet_code, ShopeeReport.order_create_time >= start_date, ShopeeReport.order_create_time <= end_date_inclusive).all()
    shopeepay_reports = ShopeepayReport.query.filter(ShopeepayReport.outlet_code == outlet_code, ShopeepayReport.create_time >= start_date, ShopeepayReport.create_time <= end_date_inclusive).all()
    tiktok_reports = TiktokReport.query.filter(TiktokReport.outlet_code == outlet_code, TiktokReport.order_time >= start_date, TiktokReport.order_time <= end_date_inclusive).all()
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
    _aggregate_shopee(daily_totals, shopee_reports)
    _aggregate_shopeepay(daily_totals, shopeepay_reports)
    _aggregate_tiktok(daily_totals, tiktok_reports)
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
            ~ManualEntry.description.ilike('%minus%')
        )
        .outerjoin(IncomeCat, (ManualEntry.category_id == IncomeCat.id) & (ManualEntry.entry_type == 'income'))
        .outerjoin(ExpenseCat, (ManualEntry.category_id == ExpenseCat.id) & (ManualEntry.entry_type == 'expense'))
        .add_entity(IncomeCat)
        .add_entity(ExpenseCat)
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
        "pukis_reports": pukis_reports,
        "minusan_by_date": minusan_by_date,
        **grab_totals
    }

def _init_daily_total():
    return {
        'Gojek_QRIS' : 0,'Gojek_Gross': 0, 'Gojek_Net': 0, 'Grab_Gross': 0, 'Grab_Net': 0, 'GrabOVO_Gross': 0, 'GrabOVO_Net': 0,
        'ShopeePay_Gross': 0, 'ShopeePay_Net': 0, 'Shopee_Gross': 0, 'Shopee_Net': 0,
        'Tiktok_Gross': 0, 'Tiktok_Net': 0, 'Cash_Income': 0, 'Cash_Expense': 0,
        'Gojek_Mutation': None, 'Gojek_Difference': 0, 'Grab_Difference': 0,
        'Grab_Commission': 0, 'Shopee_Mutation': None, 'Shopee_Difference': 0,
        'ShopeePay_Mutation': None, 'ShopeePay_Difference': 0, 'UV': 0
    }

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
    for report in reports:
        date = report.tanggal_dibuat.date()
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
        if brand not in ["Pukis & Martabak Kota Baru"]:
            daily_totals[date]['Grab_Commission'] = daily_totals[date]['Grab_Net'] * 1/74
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
    platforms = ['gojek', 'grab', 'shopee', 'shopeepay']
    for platform in platforms:
        try:
            matcher = TransactionMatcher(platform)
            mutations = matcher.get_mutations_query(start_date.date(), end_date.date()).all()
            for date, totals in daily_totals.items():
                net_key = f'{platform.capitalize()}_Net' if platform != 'shopeepay' else 'ShopeePay_Net'
                if totals.get(net_key, 0) == 0:
                    continue

                class MockDailyTotal:
                    def __init__(self, outlet_id, date, total_net):
                        self.outlet_id = outlet_id
                        self.date = date
                        self.total_net = total_net

                mock_total = MockDailyTotal(outlet_code, date, totals[net_key])
                _, mutation_data = matcher.match_transactions(mock_total, mutations)

                mutation_key = f'{platform.capitalize()}_Mutation' if platform != 'shopeepay' else 'ShopeePay_Mutation'
                diff_key = f'{platform.capitalize()}_Difference' if platform != 'shopeepay' else 'ShopeePay_Difference'

                if mutation_data:
                    mutation_amount = float(mutation_data.get('transaction_amount', 0))
                    totals[mutation_key] = mutation_amount
                    totals[diff_key] = mutation_amount - float(totals[net_key])
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