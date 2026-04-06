from io import BytesIO
from openpyxl import Workbook
from app.models.mpr_mapping import MprMapping
from app.services.excel_export.data_service import get_report_data
from app.services.excel_export.sheets.daily_sheet import DailySheet
from app.services.excel_export.sheets.summary_sheet import SummarySheet
from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.sheets.pukis_sheet import PukisSheet
from app.services.excel_export.sheets.pukis_closing_sheet import PukisClosingSheet

class ExcelReportGenerator:
    def __init__(self, outlet_code: str, start_date, end_date, user_role: str):
        self.outlet_code = outlet_code
        self.start_date = start_date
        self.end_date = end_date
        self.user_role = user_role
        self.wb = Workbook()
        self.wb.remove(self.wb.active)  # Remove default sheet

    def generate_report(self) -> BytesIO:
        """
        Generates the full Excel report by fetching data and calling each sheet generator.
        """
        report_data = self._build_report_data(self.outlet_code)

        DailySheet(self.wb, report_data).generate()

        mpr_report_data = self._get_mpr_report_data(report_data)
        report_data['mpr_report_data'] = mpr_report_data
        if mpr_report_data:
            DailySheet(self.wb, mpr_report_data, sheet_name='MPR Daily').generate()

        # Define the sheet generators to run
        sheet_generators = [
            SummarySheet,
        ]

        # Conditionally add the Pukis sheet and closing sheet
        if report_data['outlet'].brand == "Pukis & Martabak Kota Baru":
            sheet_generators.append(PukisClosingSheet)
            sheet_generators.append(PukisSheet)
        else:
            sheet_generators.append(ClosingSheet)

        # Generate each sheet
        for sheet_class in sheet_generators:
            sheet_instance = sheet_class(self.wb, report_data)
            sheet_instance.generate()

        # Save workbook to a byte stream
        excel_file = BytesIO()
        self.wb.save(excel_file)
        excel_file.seek(0)

        return excel_file

    def _build_report_data(self, outlet_code: str) -> dict:
        report_data = get_report_data(outlet_code, self.start_date, self.end_date)
        report_data['user_role'] = self.user_role
        return report_data

    def _get_mpr_report_data(self, report_data: dict) -> dict | None:
        outlet = report_data['outlet']
        if outlet.brand != "MP78":
            return None

        mapping = MprMapping.query.filter_by(mp78_outlet_code=outlet.outlet_code).first()
        if not mapping or not mapping.mpr_outlet_code:
            return None

        try:
            return self._build_report_data(mapping.mpr_outlet_code)
        except ValueError as exc:
            print(
                "Warning: Skipping MPR Daily sheet for "
                f"{outlet.outlet_code}: {exc}"
            )
            return None
