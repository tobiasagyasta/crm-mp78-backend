from io import BytesIO
from openpyxl import Workbook
from app.services.excel_export.data_service import get_report_data
from app.services.excel_export.sheets.daily_sheet import DailySheet
from app.services.excel_export.sheets.summary_sheet import SummarySheet
from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.sheets.pukis_sheet import PukisSheet

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
        report_data = get_report_data(self.outlet_code, self.start_date, self.end_date)
        report_data['user_role'] = self.user_role

        # Define the sheet generators to run
        sheet_generators = [
            DailySheet,
            SummarySheet,
            ClosingSheet,
        ]

        # Conditionally add the Pukis sheet
        if report_data['outlet'].brand == "Pukis & Martabak Kota Baru":
            sheet_generators.append(PukisSheet)

        # Generate each sheet
        for sheet_class in sheet_generators:
            sheet_instance = sheet_class(self.wb, report_data)
            sheet_instance.generate()

        # Save workbook to a byte stream
        excel_file = BytesIO()
        self.wb.save(excel_file)
        excel_file.seek(0)

        return excel_file