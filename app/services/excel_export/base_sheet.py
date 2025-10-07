from abc import ABC, abstractmethod
from openpyxl.workbook import Workbook

class BaseSheet(ABC):
    def __init__(self, workbook: Workbook, sheet_name: str, data: dict):
        self.wb = workbook
        self.sheet_name = sheet_name
        self.data = data
        self.ws = self.wb.create_sheet(title=self.sheet_name)

    @abstractmethod
    def generate(self):
        """
        An abstract method that must be implemented by all concrete sheet classes.
        This method is responsible for generating the content of the Excel sheet.
        """
        pass