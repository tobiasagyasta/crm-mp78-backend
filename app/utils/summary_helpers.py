from datetime import datetime

from flask import request


def parse_required_date():
    value = request.args.get("date")
    if not value:
        raise ValueError("date is required")

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("date must be in YYYY-MM-DD format")


def get_pagination():
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)

    return max(page, 1), min(max(per_page, 1), 100)


def serialize_outlet(outlet):
    return {
        "outlet_code": outlet.outlet_code,
        "outlet_name": outlet.outlet_name_gojek,
        "brand_name": outlet.brand,
    }


def day_bounds(report_date):
    start_datetime = datetime.combine(report_date, datetime.min.time())
    end_datetime = datetime.combine(report_date, datetime.max.time())

    return start_datetime, end_datetime


def to_float(value):
    return float(value) if value is not None else 0
