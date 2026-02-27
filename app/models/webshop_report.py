import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from app.extensions import db
from app.models.outlet import Outlet


class WebshopReport(db.Model):
    __tablename__ = "webshop_reports"

    HEADER_TO_FIELD = {
        "order id": "order_id",
        "brand": "brand_name",
        "branch": "branch",
        "status": "status",
        "net order value": "gross_value",
        "created at": "created_at",
    }
    DEFAULT_COLUMN_INDEXES = {
        "order_id": 1,
        "brand_name": 6,
        "branch": 7,
        "status": 10,
        "gross_value": 21,
        "created_at": 24,
    }

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.String, nullable=False, index=True)
    brand_name = db.Column(db.String, nullable=True)
    branch = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    gross_value = db.Column(db.Numeric, nullable=True)
    nett_value = db.Column(db.Numeric, nullable=True)
    created_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f"<WebshopReport {self.order_id}, {self.created_at}>"

    @staticmethod
    def _clean_str(value) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_header(header) -> str:
        normalized = WebshopReport._clean_str(header).replace("\ufeff", "").lower()
        return " ".join(normalized.split())

    @staticmethod
    def _resolve_columns(headers: list[str] | None) -> dict:
        resolved = {}
        if headers:
            for idx, header in enumerate(headers):
                normalized = WebshopReport._normalize_header(header)
                field_name = WebshopReport.HEADER_TO_FIELD.get(normalized)
                if field_name and field_name not in resolved:
                    resolved[field_name] = idx

        for field_name, fallback_idx in WebshopReport.DEFAULT_COLUMN_INDEXES.items():
            if field_name not in resolved:
                resolved[field_name] = fallback_idx

        return resolved

    @staticmethod
    def _parse_decimal_amount(value) -> Decimal:
        raw = WebshopReport._clean_str(value)
        if raw == "":
            return Decimal("0")

        sanitized = re.sub(r"[^\d,.\-]", "", raw)
        if sanitized in {"", "-", ".", ",", "-.", "-,"}:
            return Decimal("0")

        is_negative = sanitized.startswith("-")
        normalized_input = sanitized[1:] if is_negative else sanitized

        dot_count = normalized_input.count(".")
        comma_count = normalized_input.count(",")

        if dot_count > 0 and comma_count > 0:
            if normalized_input.rfind(",") > normalized_input.rfind("."):
                normalized = normalized_input.replace(".", "").replace(",", ".")
            else:
                normalized = normalized_input.replace(",", "")
        elif comma_count > 0:
            if comma_count > 1:
                normalized = normalized_input.replace(",", "")
            else:
                left, right = normalized_input.split(",", 1)
                if len(right) in (1, 2):
                    normalized = f"{left}.{right}"
                else:
                    normalized = f"{left}{right}"
        elif dot_count > 0:
            if dot_count > 1:
                normalized = normalized_input.replace(".", "")
            else:
                left, right = normalized_input.split(".", 1)
                if len(right) in (1, 2):
                    normalized = f"{left}.{right}"
                else:
                    normalized = f"{left}{right}"
        else:
            normalized = normalized_input

        if normalized in {"", ".", "-"}:
            return Decimal("0")

        try:
            amount = Decimal(normalized)
            return -amount if is_negative else amount
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")

    @staticmethod
    def _parse_created_at(value) -> datetime | None:
        raw = WebshopReport._clean_str(value)
        if raw == "":
            return None

        raw = " ".join(raw.split())
        for fmt in (
            "%B %d, %Y %I:%M:%S%p",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def parse_webshop_row(row: list[str], columns: dict) -> dict | None:
        try:
            if not row:
                return None

            def _value(field: str) -> str:
                idx = columns.get(field)
                if idx is None or idx < 0 or idx >= len(row):
                    return ""
                return row[idx]

            status = WebshopReport._clean_str(_value("status"))
            if status.upper() != "DELIVERED":
                return None

            order_id = WebshopReport._clean_str(_value("order_id"))
            if order_id == "":
                return None

            brand_name = WebshopReport._clean_str(_value("brand_name")).strip('"').strip("'")
            branch = WebshopReport._clean_str(_value("branch")).strip('"').strip("'")

            gross_value = WebshopReport._parse_decimal_amount(_value("gross_value"))
            nett_value = (gross_value * Decimal("0.97")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            created_at = WebshopReport._parse_created_at(_value("created_at"))

            outlet = Outlet.query.filter_by(outlet_name_webshop=branch).first()
            outlet_code = outlet.outlet_code if outlet else None

            return {
                "order_id": order_id,
                "brand_name": brand_name,
                "branch": branch,
                "outlet_code": outlet_code,
                "gross_value": gross_value,
                "nett_value": nett_value,
                "created_at": created_at,
            }
        except Exception as e:
            print(f"[WEBSHOP Parse Error] Row: {row} | Error: {str(e)}")
            return None


def load_webshop_csv(file_path: str) -> list[dict]:
    parsed_rows = []
    with open(file_path, newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader, None)
        columns = WebshopReport._resolve_columns(headers)

        for row in reader:
            parsed = WebshopReport.parse_webshop_row(row, columns)
            if parsed:
                parsed_rows.append(parsed)

    return parsed_rows
