from app.extensions import db
from datetime import date, datetime, timedelta
from app.models.outlet import Outlet
import re
import csv
import io
import hashlib
from decimal import Decimal, InvalidOperation

class BankMutation(db.Model):
    __tablename__ = 'bank_mutations'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rekening_number = db.Column(db.String, nullable=False)
    tanggal = db.Column(db.Date, nullable=True)
    transaksi = db.Column(db.String, nullable=True)

    # Parsed fields
    transaction_type = db.Column(db.String, nullable=True)
    transaction_id = db.Column(db.String, nullable=True)
    transaction_amount = db.Column(db.Numeric, nullable=True)
    platform_code = db.Column(db.String, nullable=True)
    platform_name = db.Column(db.String, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)    

    __table_args__ = (
        db.Index('ix_bank_mutations_platform_date_code', 'platform_name', 'tanggal', 'platform_code'),
        db.Index('ix_bank_mutations_platform_date_amount', 'platform_name', 'tanggal', 'transaction_amount'),
        db.Index('ix_bank_mutations_platform_date_id', 'platform_name', 'tanggal', 'id'),
    )

    @staticmethod
    def parse_pkb_report(report_content):
        """
        Parses the entire content of a PKB bank mutation report.

        Args:
            report_content (str): The raw string content of the report CSV.

        Returns:
            dict: A structured dictionary containing account_info, transactions, and summary.
        """
        account_info = {}
        transactions = []
        summary = {}
        
        lines = report_content.strip().split('\n')
        reader = csv.reader(lines)

        header_found = False
        transactions_started = False

        for row in reader:
            if not row or not any(field.strip() for field in row):
                continue

            # Clean up row data
            row = [item.strip() for item in row]
            line_str = ",".join(row)

            # Detect transaction header
            if row == ["Tanggal Transaksi", "Keterangan", "Cabang", "Jumlah", "Saldo"]:
                header_found = True
                transactions_started = True
                continue

            # Parse account info before the transaction header
            if not header_found and ":" in line_str:
                parts = line_str.split(':', 1)
                key = parts[0].strip()
                value = parts[1].strip().split(',')[0]
                if "No. rekening" in key:
                    account_info['account_number'] = value
                elif "Nama" in key:
                    account_info['account_name'] = value
                elif "Periode" in key:
                    account_info['period'] = value
                elif "Kode Mata Uang" in key:
                    account_info['currency'] = value

            # Detect end of transactions (summary section)
            if transactions_started and (line_str.startswith("Saldo Awal") or line_str.startswith("Mutasi Debet")):
                transactions_started = False

            # Parse transaction rows
            if transactions_started and len(row) == 5:
                # Basic check to avoid parsing the header again or invalid lines
                if row[0] != "Tanggal Transaksi":
                    parsed_row = BankMutation._parse_pkb_transaction_row(row)
                    if parsed_row:
                        transactions.append(parsed_row)

            # Parse summary footer
            if not transactions_started and header_found and ":" in line_str:
                parts = line_str.split(':', 1)
                key = parts[0].strip()
                value_part = parts[1].strip()
                # Use regex to find the first currency-like value in the string
                currency_match = re.search(r'[\d,]+\.\d{2}', value_part)
                if currency_match:
                    value = currency_match.group(0)
                    if "Saldo Awal" in key:
                        summary['initial_balance'] = BankMutation._parse_currency(value)
                    elif "Mutasi Debet" in key:
                        summary['debit_mutation'] = BankMutation._parse_currency(value)
                    elif "Mutasi Kredit" in key:
                        summary['credit_mutation'] = BankMutation._parse_currency(value)
                    elif "Saldo Akhir" in key:
                        summary['final_balance'] = BankMutation._parse_currency(value)

        return {
            "account_info": account_info,
            "transactions": transactions,
            "summary": summary,
        }

    @staticmethod
    def _parse_pkb_transaction_row(row):
        """
        Parses a single transaction row from a PKB report.
        """
        try:
            tanggal = datetime.strptime(row[0], '%d/%m/%Y').date()
            keterangan = row[1]
            amount_str = row[3]
            transaction_type = 'DB' if 'DB' in amount_str else 'CR'
            transaction_amount = BankMutation._parse_currency(amount_str)

            # Match platform codes in formats: 'ABC-123', 'ABC 123', or 'ABC123' (case-insensitive)
            match = re.search(r"([A-Za-z]{3})[-\s]?(\d{3})", keterangan, re.IGNORECASE)
            if match:
                letters = match.group(1).upper()
                digits = match.group(2)
                platform_code = f"{letters}-{digits}"
            else:
                platform_code = None

            transaction_id = f"{tanggal.strftime('%Y%m%d')}-{platform_code or 'NA'}-{int(transaction_amount)}-{transaction_type}"

            return {
                'tanggal': tanggal,
                'transaksi': keterangan,
                'transaction_type': transaction_type,
                'transaction_amount': transaction_amount,
                'platform_name': 'PKB',
                'platform_code': platform_code,
                'transaction_id': transaction_id
            }

        except (ValueError, IndexError, InvalidOperation) as e:
            # print(f"[PKB Parse Error] Skipping malformed row: {row} | Error: {e}")
            return None

    @staticmethod
    def _parse_currency(value_str):
        """
        Helper to convert a currency string like "135,000.00 CR" to a float.
        """
        if isinstance(value_str, (int, float, Decimal)):
            return float(value_str)
        if not isinstance(value_str, str):
            return 0.0
        # Remove all non-numeric characters except the decimal point
        cleaned_str = re.sub(r'[^\d.]', '', value_str)
        if not cleaned_str:
            return 0.0
        try:
            return float(cleaned_str)
        except (ValueError, InvalidOperation):
            return 0.0

    @staticmethod
    def _row_value(row, index, default=''):
        return row[index] if len(row) > index else default

    @staticmethod
    def _as_text(value):
        if value is None:
            return ''
        return str(value).strip()

    @staticmethod
    def _row_text(row):
        return " ".join(BankMutation._as_text(value) for value in row)

    @staticmethod
    def _compact_row_text(row):
        return " ".join(
            BankMutation._as_text(value)
            for value in row
            if BankMutation._as_text(value)
        )

    @staticmethod
    def _statement_description_after_outlet_code(value):
        description = BankMutation._as_text(value)
        if not description:
            return ''

        match = re.search(r'\([A-Za-z]{3}[-\s]?\d{3}\)\s*(.*)$', description)
        if match:
            return match.group(1).strip()

        return description

    @staticmethod
    def _parse_transaction_date(value, reference_date=None):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, (int, float)):
            return (datetime(1899, 12, 30) + timedelta(days=int(value))).date()

        raw_date_str = BankMutation._as_text(value)
        has_excel_text_quote = raw_date_str.startswith("'")
        date_str = raw_date_str.replace("'", "")
        if not date_str:
            return None
        if re.fullmatch(r'\d+(\.0+)?', date_str):
            return (datetime(1899, 12, 30) + timedelta(days=int(float(date_str)))).date()

        reference_date = reference_date or date.today()

        numeric_date_match = re.fullmatch(r'(\d{1,2})([/-])(\d{1,2})\2(\d{4})', date_str)
        if numeric_date_match:
            separator = numeric_date_match.group(2)
            preferred_formats = (
                (f'%d{separator}%m{separator}%Y', f'%m{separator}%d{separator}%Y')
                if has_excel_text_quote
                else (f'%m{separator}%d{separator}%Y', f'%d{separator}%m{separator}%Y')
            )
            candidates = []
            candidate_dates = set()
            for index, fmt in enumerate(preferred_formats):
                try:
                    parsed_date = datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
                if parsed_date not in candidate_dates:
                    candidates.append((abs((parsed_date - reference_date).days), index, parsed_date))
                    candidate_dates.add(parsed_date)
            if candidates:
                return min(candidates)[2]

        for fmt in ('%Y-%m-%d', '%d-%b-%y', '%d-%b-%Y'):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _is_statement_mutation_row(row):
        return BankMutation._normalize_statement_mutation_row(row) is not None

    @staticmethod
    def _normalize_statement_mutation_row(row):
        """
        Normalize supported bank mutation exports into:
        [Tanggal/Date, Keterangan/Description, Cabang/Branch, Jumlah/Amount, Saldo/Balance].

        REY 0385 exports the five statement columns cleanly. REY 7777 and HK 107
        split the description into many CSV cells, so the amount/direction/balance
        fields need to be found from the right side of the row.
        """
        if len(row) < 5:
            return None

        tanggal = BankMutation._parse_transaction_date(BankMutation._row_value(row, 0))
        if tanggal is None:
            return None

        if (
            BankMutation._as_text(BankMutation._row_value(row, 1))
            and BankMutation._parse_currency(BankMutation._row_value(row, 3)) > 0
            and BankMutation._parse_currency(BankMutation._row_value(row, 4)) > 0
        ):
            return [
                BankMutation._row_value(row, 0),
                BankMutation._as_text(BankMutation._row_value(row, 1)),
                BankMutation._as_text(BankMutation._row_value(row, 2)),
                BankMutation._as_text(BankMutation._row_value(row, 3)),
                BankMutation._as_text(BankMutation._row_value(row, 4)),
            ]

        direction_index = None
        for index in range(len(row) - 1, 1, -1):
            value = BankMutation._as_text(row[index]).upper().replace("'", "")
            if value in {"CR", "DB"}:
                amount_value = BankMutation._as_text(BankMutation._row_value(row, index - 1))
                balance_value = BankMutation._as_text(BankMutation._row_value(row, index + 1))
                if BankMutation._parse_currency(amount_value) > 0 and BankMutation._parse_currency(balance_value) > 0:
                    direction_index = index
                    break

        if direction_index is None:
            return None

        branch_index = direction_index - 2
        branch = BankMutation._as_text(BankMutation._row_value(row, branch_index))
        description_values = row[1:branch_index]
        description = BankMutation._compact_row_text(description_values)
        if not description:
            return None

        amount = (
            f"{BankMutation._as_text(BankMutation._row_value(row, direction_index - 1))} "
            f"{BankMutation._as_text(BankMutation._row_value(row, direction_index))}"
        ).strip()

        return [
            BankMutation._row_value(row, 0),
            description,
            branch,
            amount,
            BankMutation._as_text(BankMutation._row_value(row, direction_index + 1)),
        ]

    @staticmethod
    def _parse_statement_mutation_row(row, platform_name, platform_code=None):
        row = BankMutation._normalize_statement_mutation_row(row)
        if not row:
            return None

        tanggal = BankMutation._parse_transaction_date(row[0])
        amount_str = BankMutation._as_text(BankMutation._row_value(row, 3))
        transaction_type = 'DB' if 'DB' in amount_str.upper() else 'CR' if 'CR' in amount_str.upper() else None
        transaction_amount = BankMutation._parse_currency(amount_str)
        transaksi = BankMutation._statement_description_after_outlet_code(
            BankMutation._row_value(row, 1)
        )

        return {
            'tanggal': tanggal,
            'transaction_type': transaction_type,
            'transaction_id': None,
            'transaction_amount': transaction_amount,
            'platform_code': platform_code,
            'platform_name': platform_name,
            'transaksi': transaksi
        }

    @staticmethod
    def _extract_gojek_platform_code(text):
        match = re.search(r'([GM]\d{5,})\b', text.upper())
        if match:
            return match.group(1)

        match = re.search(r'\b[GM][A-Z0-9][A-Z0-9-]*\b', text.upper())
        return match.group(0) if match else None

    @staticmethod
    def _extract_shopee_statement_info(text):
        match = re.search(r'(MC|SF)\s*(\d{1,4})\s+(\d{3,})\b', text.upper())
        if not match:
            return None, None

        platform_name = 'ShopeeFood' if match.group(1) == 'SF' else 'Shopee'
        return platform_name, match.group(3)

    @staticmethod
    def _extract_mp78_code(text):
        match = re.search(r'\(\s*([A-Za-z0-9]{2,10})\s*\)', text)
        return match.group(1).upper() if match else None

    @staticmethod
    def _mp78_description_after_code(text):
        transaksi = BankMutation._as_text(text)
        match = re.search(r'\(\s*[A-Za-z0-9]{2,10}\s*\)\s*(.*)$', transaksi)
        return match.group(1).strip() if match else transaksi

    @staticmethod
    def _build_mp78_transaction_id(rekening_number, tanggal, transaksi, amount_str, saldo):
        parts = [
            BankMutation._as_text(rekening_number),
            tanggal.isoformat() if tanggal else '',
            re.sub(r'\s+', ' ', BankMutation._as_text(transaksi)),
            re.sub(r'\s+', ' ', BankMutation._as_text(amount_str)),
            re.sub(r'\s+', ' ', BankMutation._as_text(saldo)),
        ]
        return hashlib.sha256('|'.join(parts).encode('utf-8')).hexdigest()

    @staticmethod
    def parse_mp78_row(row, rekening_number=None):
        row = BankMutation._normalize_statement_mutation_row(row)
        if not row:
            return None

        transaksi = BankMutation._as_text(BankMutation._row_value(row, 1))
        mp78_code = BankMutation._extract_mp78_code(transaksi)
        if not mp78_code:
            return None

        outlet = Outlet.query.filter(db.func.upper(Outlet.mp78_code) == mp78_code).first()
        if not outlet:
            return None

        tanggal = BankMutation._parse_transaction_date(row[0])
        amount_str = BankMutation._as_text(BankMutation._row_value(row, 3))
        transaction_type = 'DB' if 'DB' in amount_str.upper() else 'CR' if 'CR' in amount_str.upper() else None
        transaction_amount = BankMutation._parse_currency(amount_str)

        return {
            'tanggal': tanggal,
            'transaksi': BankMutation._mp78_description_after_code(transaksi),
            'transaction_type': transaction_type,
            'transaction_amount': transaction_amount,
            'mp78_code': mp78_code,
            'outlet_code': outlet.outlet_code,
            'transaction_id': BankMutation._build_mp78_transaction_id(
                rekening_number,
                tanggal,
                transaksi,
                amount_str,
                BankMutation._row_value(row, 4),
            ),
        }

    @staticmethod
    def parse_unassigned_row(row, rekening_number=None):
        row = BankMutation._normalize_statement_mutation_row(row)
        if not row:
            return None

        tanggal = BankMutation._parse_transaction_date(row[0])
        transaksi = BankMutation._as_text(BankMutation._row_value(row, 1))
        amount_str = BankMutation._as_text(BankMutation._row_value(row, 3))
        transaction_type = 'DB' if 'DB' in amount_str.upper() else 'CR' if 'CR' in amount_str.upper() else None
        transaction_amount = BankMutation._parse_currency(amount_str)

        return {
            'tanggal': tanggal,
            'transaksi': transaksi,
            'transaction_type': transaction_type,
            'transaction_id': BankMutation._build_mp78_transaction_id(
                rekening_number,
                tanggal,
                transaksi,
                amount_str,
                BankMutation._row_value(row, 4),
            ),
            'transaction_amount': transaction_amount,
            'platform_code': None,
            'platform_name': 'Unknown',
        }

    @staticmethod
    def parse_gojek_row(row):
        """
        Parse a Gojek CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 10 (index 9) is 'BANGSA'.
        - platform_code: from column 6, from first 'G' to end
        - transaction_amount: from column 12
        - platform_name: 'Gojek'
        - tanggal: from column 1
        """
        try:
            if BankMutation._is_statement_mutation_row(row):
                row_text = BankMutation._row_text(row)
                if 'BANGSA' not in row_text.upper() and 'GOJEK' not in row_text.upper():
                    return None
                return BankMutation._parse_statement_mutation_row(
                    row,
                    'Gojek',
                    BankMutation._extract_gojek_platform_code(row_text)
                )

            # Check if col 10 is 'BANGSA'
            if BankMutation._as_text(BankMutation._row_value(row, 9)).upper() != "BANGSA":
                return None

            tanggal = BankMutation._parse_transaction_date(row[0])
            # Extract platform_code from column 6 (index 5), from first 'G' to end
            col6 = BankMutation._as_text(BankMutation._row_value(row, 5))
            # Find the first occurrence of 'G' or 'M'
            g_idx = col6.find('G')
            m_idx = col6.find('M')
            if g_idx == -1: g_idx = float('inf')
            if m_idx == -1: m_idx = float('inf')
            start_idx = min(g_idx, m_idx)
            platform_code = col6[start_idx:] if start_idx != float('inf') else col6

            # Transaction amount from column 12 (index 11)
            transaction_amount = BankMutation._parse_currency(BankMutation._row_value(row, 11))

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': platform_code,
                'platform_name': 'Gojek',
                'transaksi': ",".join(BankMutation._as_text(value) for value in row)
            }
        except Exception as e:
            return None
    @staticmethod
    def parse_grab_row(row):
        """
        Parse a Grab CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 8 (index 7) is 'VISIONET'.
        - platform_code: no platform code for Grab (set to None)
        - transaction_amount: from column 11 (index 10)
        - platform_name: 'Grab'
        - tanggal: from column 1 (index 0)
        """
        try:
            if BankMutation._is_statement_mutation_row(row):
                row_text = BankMutation._row_text(row)
                if 'VISIONET' not in row_text.upper():
                    return None
                return BankMutation._parse_statement_mutation_row(row, 'Grab')

            # Only parse if col 8 is 'VISIONET'
            if BankMutation._as_text(BankMutation._row_value(row, 7)).upper() != 'VISIONET':
                return None

            tanggal = BankMutation._parse_transaction_date(row[0])
            transaction_amount = BankMutation._parse_currency(BankMutation._row_value(row, 10))

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': None,
                'platform_name': 'Grab',
                'transaksi': ",".join(BankMutation._as_text(value) for value in row)
            }
        except Exception as e:
            return None
    @staticmethod
    def parse_shopee_row(row):
        """
        Parse a Shopee CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 10 (index 9) is 'INTERNATION'.
        - platform_code: column 8 (index 7)
        - transaction_amount: from column 12 (index 11)
        - platform_name: Check in column 6 (index 5). If it ends with 'MC', set to 'Shopee', if it ends with 'SF', set to 'ShopeeFood'
        - tanggal: from column 1 (index 0)
        """
        try:
            if BankMutation._is_statement_mutation_row(row):
                row_text = BankMutation._row_text(row)
                row_text_upper = row_text.upper()
                if 'SHOPEE' not in row_text_upper and 'INTERNATION' not in row_text_upper:
                    return None
                platform_name, platform_code = BankMutation._extract_shopee_statement_info(row_text)
                if not platform_name:
                    platform_name = 'ShopeeFood' if ' SHOPEEFOOD' in row_text_upper else 'Shopee'
                return BankMutation._parse_statement_mutation_row(row, platform_name, platform_code)

            # Only parse if col 10 (index 9) is 'INTERNATION'
            if BankMutation._as_text(BankMutation._row_value(row, 9)).upper() != 'INTERNATION':
                return None

            tanggal = BankMutation._parse_transaction_date(row[0])
            platform_code = BankMutation._as_text(BankMutation._row_value(row, 7))  # column 8 (index 7)
            transaction_amount = BankMutation._parse_currency(BankMutation._row_value(row, 11))

            # Determine platform_name from column 6 (index 5)
            col6 = BankMutation._as_text(BankMutation._row_value(row, 5))
            if col6.endswith('MC'):
                platform_name = 'Shopee'
            elif col6.endswith('SF'):
                platform_name = 'ShopeeFood'
            else:
                platform_name = '-'  # Default/fallback

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': platform_code,
                'platform_name': platform_name,
                'transaksi': ",".join(BankMutation._as_text(value) for value in row)
            }
        except Exception as e:
            return None

    @staticmethod
    def parse_mutation_row(row, rekening_number=None):
        """
        Main parser for supported bank mutation rows.
        It first normalizes the mutation table shape, then dispatches to the
        platform-specific parser.
        """
        normalized_row = BankMutation._normalize_statement_mutation_row(row)
        if not normalized_row:
            return None

        parser = BankMutation.detect_platform(normalized_row)
        if not parser:
            mp78_data = BankMutation.parse_mp78_row(normalized_row, rekening_number)
            if mp78_data:
                mp78_data['_mutation_model'] = 'mp78'
                return mp78_data
            return BankMutation.parse_unassigned_row(normalized_row, rekening_number)

        return parser(normalized_row)

    @staticmethod
    def detect_platform(row):
        """
        Detects the platform for a given CSV row and returns the corresponding parser function.
        """
        normalized_row = BankMutation._normalize_statement_mutation_row(row) or row
        row_text = BankMutation._row_text(normalized_row).upper()
        # Gojek: Only parse if col 10 (index 9) is 'BANGSA'
        if BankMutation._as_text(BankMutation._row_value(normalized_row, 9)).upper() == "BANGSA":
            return BankMutation.parse_gojek_row
        # Grab: Only parse if col 8 (index 7) is 'VISIONET'
        if BankMutation._as_text(BankMutation._row_value(normalized_row, 7)).upper() == 'VISIONET':
            return BankMutation.parse_grab_row
        # Shopee: Only parse if col 10 (index 9) is 'INTERNATION'
        if BankMutation._as_text(BankMutation._row_value(normalized_row, 9)).upper() == 'INTERNATION':
            return BankMutation.parse_shopee_row
        if 'VISIONET' in row_text:
            return BankMutation.parse_grab_row
        if 'BANGSA' in row_text or 'GOJEK' in row_text:
            return BankMutation.parse_gojek_row
        if 'SHOPEE' in row_text or 'INTERNATION' in row_text:
            return BankMutation.parse_shopee_row
        # Add more platform checks here as needed
        return None

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.platform_name} {self.transaction_amount}>"
