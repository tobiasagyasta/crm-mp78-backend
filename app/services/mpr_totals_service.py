from datetime import datetime

from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.mpr_mapping import MprMapping
from app.models.outlet import Outlet
from app.models.shopee_reports import ShopeeReport
from app.models.tiktok_reports import TiktokReport
from app.services.excel_export import mpr_calculations as mpr_calc


def get_mpr_mapping_for_outlet(outlet_code: str) -> tuple[Outlet | None, MprMapping | None]:
    outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
    if not outlet or outlet.brand not in ("MP78", "MPR"):
        return outlet, None

    if outlet.brand == "MP78":
        mapping = MprMapping.query.filter_by(mp78_outlet_code=outlet.outlet_code).first()
    else:
        mapping = MprMapping.query.filter_by(mpr_outlet_code=outlet.outlet_code).first()

    return outlet, mapping


def calculate_mpr_totals(
    mpr_outlet_code: str,
    start_date: datetime | None = None,
    end_date_inclusive: datetime | None = None,
) -> dict:
    gojek_query = GojekReport.query.filter(GojekReport.outlet_code == mpr_outlet_code)
    grab_query = GrabFoodReport.query.filter(GrabFoodReport.outlet_code == mpr_outlet_code)
    shopee_query = ShopeeReport.query.filter(ShopeeReport.outlet_code == mpr_outlet_code)
    tiktok_query = TiktokReport.query.filter(TiktokReport.outlet_code == mpr_outlet_code)

    if start_date and end_date_inclusive:
        gojek_query = gojek_query.filter(
            GojekReport.transaction_date >= start_date.date(),
            GojekReport.transaction_date <= end_date_inclusive.date(),
        )
        grab_query = grab_query.filter(
            GrabFoodReport.tanggal_dibuat >= start_date,
            GrabFoodReport.tanggal_dibuat <= end_date_inclusive,
        )
        shopee_query = shopee_query.filter(
            ShopeeReport.order_create_time >= start_date,
            ShopeeReport.order_create_time <= end_date_inclusive,
        )
        tiktok_query = tiktok_query.filter(
            TiktokReport.order_time >= start_date,
            TiktokReport.order_time <= end_date_inclusive,
        )

    totals = {
        "Gojek_Net": 0,
        "Gojek_QRIS": 0,
        "Grab_Net": 0,
        "GrabOVO_Net": 0,
        "Shopee_Net": 0,
        "Tiktok_Net": 0,
    }

    for report in gojek_query.all():
        amount = float(report.nett_amount or 0)
        totals["Gojek_Net"] += amount
        if report.payment_type == "QRIS":
            totals["Gojek_QRIS"] += amount

    for report in grab_query.all():
        amount = float(report.total or 0)
        totals["Grab_Net"] += amount
        if getattr(report, "jenis", None) == "OVO":
            totals["GrabOVO_Net"] += amount

    for report in shopee_query.all():
        if report.order_status != "Cancelled":
            totals["Shopee_Net"] += float(report.net_income or 0)

    for report in tiktok_query.all():
        totals["Tiktok_Net"] += float(report.net_amount or 0)

    mpr_totals = {
        "gojek": mpr_calc.gojek_net_value(totals, is_mpr=True),
        "grab": mpr_calc.grab_net_value(totals, is_mpr=True),
        "shopee": mpr_calc.shopee_net_value(totals, is_mpr=True),
        "tiktok": mpr_calc.tiktok_net_ac_value(totals, is_mpr=True),
    }

    return _round_totals(mpr_totals)


def _round_totals(totals: dict[str, float]) -> dict[str, float]:
    return {key: round(value, 2) for key, value in totals.items()}
