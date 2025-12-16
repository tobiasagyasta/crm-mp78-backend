from app.extensions import db
from datetime import datetime
from app.models.outlet import Outlet
import re
import csv
import io
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
            # Check if col 10 is 'BANGSA'
            if row[9].strip().upper() != "BANGSA":
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            # Extract platform_code from column 6 (index 5), from first 'G' to end
            col6 = row[5].strip()
            col6 = row[5].strip()
            # Find the first occurrence of 'G' or 'M'
            g_idx = col6.find('G')
            m_idx = col6.find('M')
            if g_idx == -1: g_idx = float('inf')
            if m_idx == -1: m_idx = float('inf')
            start_idx = min(g_idx, m_idx)
            platform_code = col6[start_idx:] if start_idx != float('inf') else col6

            # Transaction amount from column 12 (index 11)
            amount_str = row[11].replace(',', '').replace('"', '').strip()
            transaction_amount = float(amount_str)

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': platform_code,
                'platform_name': 'Gojek',
                'transaksi': ",".join(row)
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
            # Only parse if col 8 is 'VISIONET'
            if row[7].strip().upper() != 'VISIONET':
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            amount_str = row[10].replace(',', '').replace('"', '').strip()
            transaction_amount = float(amount_str)

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': None,
                'platform_name': 'Grab',
                'transaksi': ",".join(row)
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
            # Only parse if col 10 (index 9) is 'INTERNATION'
            if row[9].strip().upper() != 'INTERNATION':
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            platform_code = row[7].strip()  # column 8 (index 7)
            amount_str = row[11].replace(',', '').replace('"', '').replace(".00",'').strip()
            transaction_amount = float(amount_str)

            # Determine platform_name from column 6 (index 5)
            col6 = row[5].strip()
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
                'transaksi': ",".join(row)
            }
        except Exception as e:
            return None

    @staticmethod
    def detect_platform(row):
        """
        Detects the platform for a given CSV row and returns the corresponding parser function.
        """
        # Gojek: Only parse if col 10 (index 9) is 'BANGSA'
        if row[9].strip().upper() == "BANGSA":
            return BankMutation.parse_gojek_row
        # Grab: Only parse if col 8 (index 7) is 'VISIONET'
        if row[7].strip().upper() == 'VISIONET':
            return BankMutation.parse_grab_row
        # Shopee: Only parse if col 10 (index 9) is 'INTERNATION'
        if row[9].strip().upper() == 'INTERNATION':
            return BankMutation.parse_shopee_row
        # Add more platform checks here as needed
        return None

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.platform_name} {self.transaction_amount}>"