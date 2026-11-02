from datetime import date
from unittest import TestCase

from app.models.bank_mutations import BankMutation
from app.services.mutation_report_importer import _mutation_rows_from_upload


class MutationCsvDelimiterTest(TestCase):
    def test_semicolon_statement_with_split_description(self):
        content = (
            "No.;Rekening;=;'2291560107;;;;;;;;;;\n"
            "Nama;=;HELEN;KUELANA;;;;;;;;;;\n"
            ";;;;;;;;;;;;;\n"
            "Tanggal;Keterangan;Cabang;Jumlah;Saldo;;;;;;;;;\n"
            "'30/10/2026;TRSF;E-BANKING;CR;3010/FTSCY/WS95051;"
            "717.274.002.291.560.000;26AU01FK02859792;VISIONET;INTERNASI;"
            "'0000;717274.00;CR;75472805.04;\n"
        )
        rows = _mutation_rows_from_upload(content.encode(), 'mutation.csv')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 5)
        parsed = BankMutation.parse_mutation_row(rows[0][2], '2291560107')
        self.assertEqual(parsed['tanggal'], date(2026, 10, 30))
        self.assertEqual(parsed['platform_name'], 'Grab')
        self.assertEqual(parsed['transaction_amount'], 717274.0)
        self.assertEqual(parsed['transaction_type'], 'CR')

    def test_comma_statement_with_quoted_amount(self):
        content = ('Metadata,example\nTanggal,Keterangan,Cabang,Jumlah,Saldo\n'
                   "'30/10/2026,VISIONET,0000,\"717,274.00 CR\",\"75,472,805.04\"\n")
        rows = _mutation_rows_from_upload(content.encode(), 'mutation.csv')
        self.assertEqual(len(rows), 1)
        parsed = BankMutation.parse_mutation_row(rows[0][2], '2291560107')
        self.assertEqual(parsed['platform_name'], 'Grab')
        self.assertEqual(parsed['transaction_amount'], 717274.0)

    def test_semicolon_headerless_fallback(self):
        content = "Date;Description;Value\n'30/10/2026;VISIONET;717274.00\n"
        rows = _mutation_rows_from_upload(content.encode(), 'mutation.csv')
        self.assertEqual(rows[0][2], ["'30/10/2026", 'VISIONET', '717274.00'])
