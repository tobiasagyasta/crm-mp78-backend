from io import StringIO
from unittest import TestCase

from app.models.qpon_reports import QponReport, qpon_csv_reader


class QponCsvDelimiterTest(TestCase):
    def test_semicolon_header_and_comma_amounts(self):
        content = ('Waktu Pembuatan Tagihan;Nomor Tagihan;Nama Toko;Total Penerimaan;Nett\n'
                   '2026-10-30;123;"Store; Main";1.234,50;1.200,00\n')
        reader = qpon_csv_reader(StringIO(content))
        columns = QponReport._resolve_columns(next(reader))
        row = next(reader)
        self.assertEqual(row[columns['outlet_name']], 'Store; Main')
        self.assertEqual(QponReport._parse_amount(row[columns['gross_amount']]), 1234.5)
        self.assertEqual(QponReport._parse_amount(row[columns['nett_amount']]), 1200.0)

    def test_comma_header_with_quoted_semicolons(self):
        content = ('Waktu Pembuatan Tagihan,Nomor Tagihan,Nama Toko,Total Penerimaan,Nett\n'
                   '2026-10-30,123,"Store; Main","1,234.50",1200\n')
        reader = qpon_csv_reader(StringIO(content))
        columns = QponReport._resolve_columns(next(reader))
        row = next(reader)
        self.assertEqual(row[columns['outlet_name']], 'Store; Main')
        self.assertEqual(QponReport._parse_amount(row[columns['gross_amount']]), 1234.5)

    def test_unrecognized_headers_use_semicolon_or_comma_fallback(self):
        self.assertEqual(list(qpon_csv_reader(StringIO('a;b;c\n1;2;3\n')))[1], ['1', '2', '3'])
        self.assertEqual(list(qpon_csv_reader(StringIO('a,b,c\n1,2,3\n')))[1], ['1', '2', '3'])
