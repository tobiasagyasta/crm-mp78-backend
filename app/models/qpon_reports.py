import csv
import re
from datetime import datetime

from app.extensions import db
from app.models.outlet import Outlet
from sqlalchemy import func, or_


class QponReport(db.Model):
    __tablename__ = "qpon_reports"

    DEFAULT_COLUMN_INDEXES = {
        "bill_created_at": 0,
        "billing_id": 1,
        "outlet_name": 3,
        "gross_amount": 5,
        "nett_amount": 14,
    }
    HEADER_TO_FIELD = {
        "waktu pembuatan tagihan": "bill_created_at",
        "nomor tagihan": "billing_id",
        "nama toko": "outlet_name",
        "total penerimaan": "gross_amount",
        "nett": "nett_amount",
    }

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    billing_id = db.Column(db.String, nullable=False, unique=True, index=True)
    bill_created_at = db.Column(db.DateTime, nullable=True)
    brand_name = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    outlet_name = db.Column(db.String, nullable=True)
    gross_amount = db.Column(db.Numeric, nullable=True)
    nett_amount = db.Column(db.Numeric, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)

    def __repr__(self):
        return f"<QponReport {self.billing_id}, {self.bill_created_at}>"

    @staticmethod
    def _clean_str(value) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _parse_amount(value) -> float:
        raw = QponReport._clean_str(value)
        if raw == "":
            return 0.0

        sanitized = re.sub(r"[^\d,.\-]", "", raw)
        if sanitized in {"", "-", ".", ",", "-.", "-,"}:
            return 0.0

        is_negative = sanitized.startswith("-")
        normalized_input = sanitized[1:] if is_negative else sanitized
        dot_count = normalized_input.count(".")
        comma_count = normalized_input.count(",")

        if dot_count > 0 and comma_count > 0:
            decimal_sep = "." if normalized_input.rfind(".") > normalized_input.rfind(",") else ","
            thousands_sep = "," if decimal_sep == "." else "."
            normalized = normalized_input.replace(thousands_sep, "").replace(decimal_sep, ".")
        elif dot_count > 0 or comma_count > 0:
            sep = "." if dot_count > 0 else ","
            occurrences = normalized_input.count(sep)
            if occurrences > 1:
                normalized = normalized_input.replace(sep, "")
            else:
                sep_index = normalized_input.find(sep)
                digits_after = len(normalized_input) - sep_index - 1
                if digits_after in (1, 2):
                    normalized = normalized_input.replace(sep, ".")
                else:
                    normalized = normalized_input.replace(sep, "")
        else:
            normalized = normalized_input

        if normalized in {"", "."}:
            return 0.0

        try:
            amount = float(normalized)
            return -amount if is_negative else amount
        except Exception:
            return 0.0

    @staticmethod
    def _parse_date(value) -> datetime | None:
        raw = QponReport._clean_str(value)
        if raw == "":
            return None

        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _normalize_header(h: str) -> str:
        header = "" if h is None else str(h)
        header = header.replace("\ufeff", "").strip().lower()
        return " ".join(header.split())

    @staticmethod
    def _resolve_columns(headers: list[str] | None) -> dict:
        resolved = {}
        if headers:
            for idx, header in enumerate(headers):
                normalized_header = QponReport._normalize_header(header)
                field_name = QponReport.HEADER_TO_FIELD.get(normalized_header)
                if field_name and field_name not in resolved:
                    resolved[field_name] = idx

        for field_name, fallback_idx in QponReport.DEFAULT_COLUMN_INDEXES.items():
            if field_name not in resolved:
                resolved[field_name] = fallback_idx

        return resolved

    @staticmethod
    def parse_qpon_row(row: list[str], columns: dict) -> dict | None:
        try:
            if not row:
                return None

            def _value(field: str) -> str:
                idx = columns.get(field)
                if idx is None or idx < 0 or idx >= len(row):
                    return ""
                return row[idx]

            billing_id_raw = _value("billing_id")
            bill_created_at_raw = _value("bill_created_at")
            outlet_name_raw = _value("outlet_name")
            gross_amount_raw = _value("gross_amount")
            nett_amount_raw = _value("nett_amount")

            billing_id = QponReport._clean_str(billing_id_raw)
            if billing_id == "":
                return None

            bill_created_at = QponReport._parse_date(bill_created_at_raw)
            outlet_name = QponReport._clean_str(outlet_name_raw)
            gross_amount = QponReport._parse_amount(gross_amount_raw)
            nett_amount = QponReport._parse_amount(nett_amount_raw)

            outlet = None
            normalized = outlet_name.strip().lower()
            outlet = (
            Outlet.query
            .filter(
                or_(
                    func.lower(func.trim(Outlet.outlet_name_qpon)) == normalized,
                    func.lower(func.trim(Outlet.outlet_name_grab)) == normalized,
                )
            )
            .first()
        )

            brand_name = outlet.brand if outlet else None
            outlet_code = outlet.outlet_code if outlet else None

            return {
                "billing_id": billing_id,
                "bill_created_at": bill_created_at,
                "brand_name": brand_name,
                "outlet_code": outlet_code,
                "outlet_name": outlet_name,
                "gross_amount": gross_amount,
                "nett_amount": nett_amount,
            }
        except Exception as e:
            print(f"[QPON Parse Error] Row: {row} | Error: {str(e)}")
            return None


def load_qpon_csv(file_path: str) -> list[dict]:
    parsed_rows = []
    with open(file_path, newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader, None)
        columns = QponReport._resolve_columns(headers)

        for row in reader:
            parsed = QponReport.parse_qpon_row(row, columns)
            if parsed:
                parsed_rows.append(parsed)

    return parsed_rows
