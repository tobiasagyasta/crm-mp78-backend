import csv
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
from types import SimpleNamespace
from contextlib import ExitStack
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask
from openpyxl import Workbook

from app.controllers import webshop_controller
from app.controllers import reports_controller, summary_controller
from app.controllers.summary_controller import get_report_detail
from app.extensions import db
from app.models.maxim_report import MaximReport
from app.models.outlet import Outlet
from app.models.webshop_report import WebshopReport
from app.services.closing_platforms import normalize_platform
from app.services.excel_export.data_service import (
    _aggregate_webshop, _calculate_grand_totals, _init_daily_total,
)
from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.sheets.daily_sheet import DailySheet
from app.services.excel_export.sheets.monthly_management_commission_sheet import (
    MonthlyManagementCommissionSheet,
)
from app.services.excel_export.sheets.monthly_mpr_commission_sheet import (
    MonthlyMprCommissionSheet,
)
from app.services.excel_export.sheets.pukis_closing_sheet import PukisClosingSheet
from app.services import reporting_service
from app.services.webshop_report_source import classify_report_source


class MaximReportsTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://", TESTING=True)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        WebshopReport.__table__.create(db.engine)
        MaximReport.__table__.create(db.engine)
        self.outlet_patch = patch.object(Outlet, "query")
        outlet_query = self.outlet_patch.start()
        outlet_query.filter_by.return_value.first.return_value = SimpleNamespace(outlet_code="OUT")

    def tearDown(self):
        self.outlet_patch.stop()
        db.session.remove()
        MaximReport.__table__.drop(db.engine)
        WebshopReport.__table__.drop(db.engine)
        self.context.pop()

    @staticmethod
    def sample_rows():
        path = Path(__file__).resolve().parents[1] / "docs" / "webshop-maxim.csv"
        with path.open(encoding="utf-8-sig", newline="") as sample:
            rows = list(csv.reader(sample))
        return rows[0], rows[1], rows[2]

    def upload(self, rows):
        content = StringIO()
        csv.writer(content).writerows(rows)
        with self.app.test_request_context(
            "/upload/webshop", method="POST",
            data={"file": (BytesIO(content.getvalue().encode()), "mixed.csv")},
            content_type="multipart/form-data",
        ):
            response, status = webshop_controller.upload_report_webshop()
        self.assertEqual(status, 201)
        return response.get_json()

    def test_mixed_sample_classification_commission_and_repeat_upload(self):
        header, webshop, maxim = self.sample_rows()
        columns = WebshopReport._resolve_columns(header)
        self.assertEqual(classify_report_source(webshop, columns), "webshop")
        self.assertEqual(classify_report_source(maxim, columns), "maxim")
        delivered = maxim.copy()
        delivered[columns["status"]] = "DELIVERED"
        ambiguous = delivered.copy()
        ambiguous[columns["order_id"]] = "ambiguous"
        ambiguous[columns["order_source"]] = "Webshop"
        unknown = delivered.copy()
        unknown[columns["order_id"]] = "unknown"
        unknown[columns["provider"]] = "Other"
        result = self.upload([header, webshop, maxim, delivered, ambiguous, unknown, delivered])
        self.assertEqual(result["platform_counts"], {"webshop": 1, "maxim": 1})
        self.assertEqual(result["total_records"], 2)
        self.assertEqual(result["skipped_records"], 4)
        self.assertEqual(WebshopReport.query.one().nett_value, Decimal("101850"))
        report = MaximReport.query.one()
        self.assertEqual(report.order_id, maxim[0])
        self.assertEqual(report.gross_value, Decimal("225000"))
        self.assertEqual(report.nett_value, Decimal("225000"))
        self.assertEqual(report.outlet_code, "OUT")
        self.assertEqual(report.created_at, datetime(2026, 11, 5, 19, 19, 44))
        reasons = [row["reason"] for row in result["skipped_rows_debug"]]
        self.assertEqual(reasons.count("Unknown or ambiguous provider/order source"), 2)
        self.assertEqual(self.upload([header, webshop, delivered])["total_records"], 0)

        detail = get_report_detail("maxim", "OUT", date(2026, 11, 5))
        self.assertEqual(detail["net_amount"], 225000)
        self.assertEqual(detail["record_count"], 1)

    def test_existing_webshop_id_prevents_maxim_double_count(self):
        header, _, maxim = self.sample_rows()
        maxim[WebshopReport._resolve_columns(header)["status"]] = "DELIVERED"
        db.session.add(WebshopReport(order_id=maxim[0]))
        db.session.commit()
        result = self.upload([header, maxim])
        self.assertEqual(result["total_records"], 0)
        self.assertEqual(MaximReport.query.count(), 0)

    def test_missing_source_headers_are_not_guessed(self):
        columns = WebshopReport._resolve_columns(["Order ID", "Status"])
        self.assertIsNone(classify_report_source(["1", "DELIVERED"], columns))
        result = self.upload([["Order ID", "Status"], ["1", "DELIVERED"]])
        self.assertEqual(result["total_records"], 0)
        self.assertEqual(result["skipped_rows_debug"][0]["reason"],
                         "Missing Provider or Order Source CSV header")

    def test_export_totals_and_closing_do_not_apply_commission(self):
        day = date(2026, 11, 5)
        totals = {day: _init_daily_total()}
        _aggregate_webshop(totals, [
            SimpleNamespace(created_at=datetime(2026, 11, 5), gross_value=225000, nett_value=225000)
        ], platform="Maxim")
        grand = _calculate_grand_totals(totals)
        self.assertEqual(grand["Maxim_Net"], 225000)
        daily = DailySheet(None, {
            "outlet": SimpleNamespace(brand="MPR"),
            "all_dates": [day], "daily_totals": totals, "grand_totals": grand,
            "minusan_by_date": {},
        }).build_preview()
        self.assertEqual(daily["totals"]["Maxim Net"], 225000)
        self.assertEqual(normalize_platform("Maxim_Net"), "maxim")
        data = {
            "outlet": SimpleNamespace(brand="MPR", outlet_name_gojek="Outlet"),
            "start_date": day, "end_date": day, "grand_totals": grand,
            "daily_totals": totals, "manual_entries": [],
        }
        sheet = ClosingSheet(Workbook(), data)
        with patch.object(sheet, "_is_closing_platform_disabled", return_value=False):
            sheet._write_grand_total_section()
        col = sheet.grand_total_col_start
        rows = {sheet.ws.cell(row, col).value: row for row in range(5, sheet.ws.max_row + 1)}
        self.assertEqual(sheet.ws.cell(rows["Maxim"], col + 1).value, 225000)
        self.assertEqual(sheet.ws.cell(4, col + 1).value, 225000)

    def test_monthly_commission_sheets_keep_maxim_at_zero(self):
        for sheet_class in (MonthlyManagementCommissionSheet, MonthlyMprCommissionSheet):
            platform = next(p for p in sheet_class.PLATFORM_GROUPS if p["key"] == "maxim")
            self.assertEqual(platform["rate"], 0)
            sheet = sheet_class(Workbook(), {})
            if sheet_class is MonthlyManagementCommissionSheet:
                values = sheet._platform_values({"maxim_net": 100}, platform, {})
            else:
                values = sheet._platform_values({"maxim_net": 100}, platform)
            self.assertEqual(values, (100, 0, 100))

    def test_totals_endpoint_includes_maxim_once(self):
        day = datetime(2026, 11, 5, 10)
        db.session.add_all([
            WebshopReport(order_id="w", outlet_code="OUT", created_at=day, nett_value=97),
            MaximReport(order_id="m", outlet_code="OUT", created_at=day, nett_value=100),
            MaximReport(order_id="other", outlet_code="OTHER", created_at=day, nett_value=999),
            MaximReport(order_id="late", outlet_code="OUT", created_at=datetime(2026, 12, 1), nett_value=999),
        ])
        db.session.commit()
        empty_query = MagicMock()
        empty_query.filter.return_value = empty_query
        empty_query.all.return_value = []
        models = [
            reports_controller.GojekReport, reports_controller.GrabFoodReport,
            reports_controller.ShopeeReport, reports_controller.ShopeepayReport,
            reports_controller.TiktokReport, summary_controller.QponReport,
            reports_controller.CashReport, reports_controller.ManualEntry,
        ]
        with ExitStack() as stack:
            for model in models:
                stack.enter_context(patch.object(model, "query", empty_query))
            stack.enter_context(patch.object(
                reports_controller, "get_mpr_mapping_for_outlet", return_value=(None, None)
            ))
            stack.enter_context(patch.object(
                reports_controller.RekeningInfoService, "get_outlet_rekenings", return_value=[]
            ))
            with self.app.test_request_context(
                "/?outlet_code=OUT&start_date=2026-11-01&end_date=2026-11-30"
            ):
                response, status = reports_controller.get_reports_totals()
        self.assertEqual(status, 200)
        totals = response.get_json()["totals"]
        self.assertEqual(totals["webshop"], 97)
        self.assertEqual(totals["maxim"], 100)
        self.assertEqual(totals["running_total"], 197)

    def test_pukis_and_mapped_mpr_closing_include_maxim(self):
        day = date(2026, 11, 5)
        daily = {day: _init_daily_total()}
        daily[day]["Maxim_Net"] = 100
        grand = _calculate_grand_totals(daily)
        pukis_data = {
            "outlet": SimpleNamespace(outlet_name_gojek="Outlet"),
            "start_date": day, "end_date": day, "all_dates": [day],
            "daily_totals": daily, "grand_totals": grand, "pukis_reports": [],
        }
        pukis = PukisClosingSheet(Workbook(), pukis_data)
        pukis._write_header()
        pukis._write_data()
        self.assertEqual(pukis.ws.cell(5, 9).value, "MAXIM")
        self.assertEqual(pukis.ws.cell(6, 9).value, 100)
        self.assertEqual(pukis.ws.cell(7, 9).value, 100)

        main_daily = {day: _init_daily_total()}
        main_daily[day]["Maxim_Net"] = 50
        main = {
            "outlet": SimpleNamespace(brand="MP78", outlet_name_gojek="Outlet"),
            "start_date": day, "end_date": day, "manual_entries": [],
            "daily_totals": main_daily, "grand_totals": _calculate_grand_totals(main_daily),
            "mpr_report_data": {
                "outlet": SimpleNamespace(brand="MPR"),
                "daily_totals": daily, "grand_totals": grand,
            },
        }
        closing = ClosingSheet(Workbook(), main)
        with patch.object(closing, "_is_closing_platform_disabled", return_value=False):
            closing._write_grand_total_section()
        col = closing.grand_total_col_start
        rows = {closing.ws.cell(row, col).value: row for row in range(5, closing.ws.max_row + 1)}
        self.assertEqual(closing.ws.cell(rows["Maxim"], col + 1).value, 50)
        self.assertEqual(closing.ws.cell(rows["Maxim MPR"], col + 1).value, 100)
        self.assertEqual(closing.ws.cell(4, col + 1).value, 150)

    def test_monthly_service_reports_maxim_without_commission(self):
        outlet = SimpleNamespace(
            outlet_code="OUT", outlet_name_gojek="Outlet", outlet_name_grab=None,
            closing_date=None, brand="MPR",
        )
        db.session.add(MaximReport(
            order_id="m", outlet_code="OUT", created_at=datetime(2026, 11, 5),
            nett_value=100,
        ))
        db.session.commit()
        empty_query = MagicMock()
        empty_query.filter.return_value = empty_query
        empty_query.all.return_value = []
        outlet_query = MagicMock()
        outlet_query.filter.return_value.all.return_value = [outlet]
        outlet_query.filter_by.return_value.all.return_value = [outlet]
        with ExitStack() as stack:
            stack.enter_context(patch.object(Outlet, "query", outlet_query))
            for model in (
                reporting_service.GojekReport, reporting_service.GrabFoodReport,
                reporting_service.ShopeeReport, reporting_service.ShopeepayReport,
                reporting_service.TiktokReport, reporting_service.QponReport,
            ):
                stack.enter_context(patch.object(model, "query", empty_query))
            mpr = reporting_service.generate_monthly_mpr_commission_data(2026)
            custom = reporting_service.generate_monthly_management_commission_data_custom_range(
                "Brand", date(2026, 11, 1), date(2026, 11, 30)
            )
        for period in (
            mpr["outlets"]["OUT"]["monthly_totals"][11],
            custom["outlets"]["OUT"]["monthly_totals"][(2026, 11)],
        ):
            self.assertEqual(period["maxim_net"], 100)
            self.assertEqual(period["maxim_commission"], 0)
            self.assertEqual(period["maxim_net_after_commission"], 100)
