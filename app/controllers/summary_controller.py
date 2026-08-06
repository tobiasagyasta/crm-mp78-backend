from datetime import timedelta

from flask import Blueprint, jsonify, request
from sqlalchemy import func

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.outlet import Outlet
from app.models.qpon_reports import QponReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.shopee_reports import ShopeeReport
from app.models.tiktok_reports import TiktokReport
from app.models.webshop_report import WebshopReport
from app.utils.summary_helpers import (
    day_bounds,
    get_pagination,
    parse_required_date,
    serialize_outlet,
    to_float,
)


summary_bp = Blueprint("summary", __name__, url_prefix="/summary")


REPORT_EXISTENCE_CONFIG = {
    "gojek": {
        "model": GojekReport,
        "date_expression": GojekReport.transaction_date,
        "is_datetime": False,
        "gross_expression": GojekReport.amount,
        "net_expression": GojekReport.nett_amount,
        "id_attr": "transaction_id",
        "date_attr": "transaction_date",
        "display_attr": "merchant_name",
    },
    "grab": {
        "model": GrabFoodReport,
        "date_expression": func.coalesce(
            GrabFoodReport.diperbarui_pada,
            GrabFoodReport.tanggal_dibuat,
        ),
        "is_datetime": True,
        "gross_expression": GrabFoodReport.amount,
        "net_expression": GrabFoodReport.total,
        "id_attr": "id_transaksi",
        "date_attr": "diperbarui_pada",
        "fallback_date_attr": "tanggal_dibuat",
        "display_attr": "nama_toko",
    },
    "shopee": {
        "model": ShopeeReport,
        "date_expression": ShopeeReport.order_create_time,
        "is_datetime": True,
        "extra_filters": [ShopeeReport.order_status != "Cancelled"],
        "gross_expression": ShopeeReport.order_amount,
        "net_expression": ShopeeReport.net_income,
        "id_attr": "order_id",
        "date_attr": "order_create_time",
        "display_attr": "store_name",
    },
    "shopeepay": {
        "model": ShopeepayReport,
        "date_expression": ShopeepayReport.create_time,
        "is_datetime": True,
        "extra_filters": [ShopeepayReport.transaction_type != "Withdrawal"],
        "gross_expression": ShopeepayReport.transaction_amount,
        "net_expression": ShopeepayReport.settlement_amount,
        "id_attr": "transaction_id",
        "date_attr": "create_time",
        "display_attr": "merchant_store_name",
    },
    "tiktok": {
        "model": TiktokReport,
        "date_expression": TiktokReport.order_time,
        "is_datetime": True,
        "gross_expression": TiktokReport.gross_amount,
        "net_expression": TiktokReport.net_amount,
        "id_attr": "outlet_order_id",
        "date_attr": "order_time",
        "display_attr": "store_name",
    },
    "qpon": {
        "model": QponReport,
        "date_expression": QponReport.bill_created_at,
        "is_datetime": True,
        "gross_expression": QponReport.gross_amount,
        "net_expression": QponReport.nett_amount,
        "id_attr": "billing_id",
        "date_attr": "bill_created_at",
        "display_attr": "outlet_name",
    },
    "webshop": {
        "model": WebshopReport,
        "date_expression": WebshopReport.created_at,
        "is_datetime": True,
        "gross_expression": WebshopReport.gross_value,
        "net_expression": WebshopReport.nett_value,
        "id_attr": "order_id",
        "date_attr": "created_at",
        "display_attr": "branch",
    },
}


def apply_report_date_filter(query, date_expression, report_date, is_datetime):
    if is_datetime:
        start_datetime, end_datetime = day_bounds(report_date)
        return query.filter(
            date_expression >= start_datetime,
            date_expression <= end_datetime,
        )

    return query.filter(date_expression == report_date)


def get_existing_report_outlet_codes(config, outlet_codes, report_date):
    if not outlet_codes:
        return set()

    model = config["model"]
    query = db.session.query(model.outlet_code).filter(
        model.outlet_code.in_(outlet_codes),
        model.outlet_code.isnot(None),
    )
    query = apply_report_date_filter(
        query,
        config["date_expression"],
        report_date,
        config["is_datetime"],
    )

    for extra_filter in config.get("extra_filters", []):
        query = query.filter(extra_filter)

    return {outlet_code for (outlet_code,) in query.distinct().all()}


def get_report_existence_sets(outlet_codes, report_date):
    return {
        platform: get_existing_report_outlet_codes(config, outlet_codes, report_date)
        for platform, config in REPORT_EXISTENCE_CONFIG.items()
    }


def normalize_platform_code(value):
    return str(value).strip() if value is not None and str(value).strip() else None


def shopee_store_suffixes(store_id):
    normalized_store_id = normalize_platform_code(store_id)
    if not normalized_store_id:
        return set()

    digits = "".join(char for char in normalized_store_id if char.isdigit())
    if len(digits) < 4:
        return set()

    suffixes = {digits[-4:]}
    if len(digits) >= 5:
        suffixes.add(digits[-5:])

    return suffixes


def get_mutation_existence_sets(outlets, mutation_date):
    gojek_store_ids = {
        normalize_platform_code(outlet.store_id_gojek)
        for outlet in outlets
        if normalize_platform_code(outlet.store_id_gojek)
    }
    shopee_suffixes = set()
    for outlet in outlets:
        shopee_suffixes.update(shopee_store_suffixes(outlet.store_id_shopee))

    gojek_matches = set()
    if gojek_store_ids:
        gojek_matches = {
            platform_code
            for (platform_code,) in db.session.query(BankMutation.platform_code)
            .filter(
                BankMutation.platform_name == "Gojek",
                BankMutation.tanggal == mutation_date,
                BankMutation.platform_code.in_(gojek_store_ids),
            )
            .distinct()
            .all()
        }

    shopee_matches = {"shopeefood": set(), "shopee": set()}
    if shopee_suffixes:
        mutation_rows = (
            db.session.query(BankMutation.platform_name, BankMutation.platform_code)
            .filter(
                BankMutation.platform_name.in_(("ShopeeFood", "Shopee")),
                BankMutation.tanggal == mutation_date,
                BankMutation.platform_code.in_(shopee_suffixes),
            )
            .distinct()
            .all()
        )
        for platform_name, platform_code in mutation_rows:
            shopee_matches[platform_name.lower()].add(platform_code)

    return {
        "gojek": gojek_matches,
        "shopeefood": shopee_matches["shopeefood"],
        "shopee": shopee_matches["shopee"],
    }


def get_outlet_mutation_flags(outlet, mutation_existence_sets):
    gojek_store_id = normalize_platform_code(outlet.store_id_gojek)
    shopee_suffixes = shopee_store_suffixes(outlet.store_id_shopee)

    return {
        "gojek": bool(gojek_store_id and gojek_store_id in mutation_existence_sets["gojek"]),
        "shopeefood": bool(shopee_suffixes & mutation_existence_sets["shopeefood"]),
        "shopee": bool(shopee_suffixes & mutation_existence_sets["shopee"]),
    }


def serialize_date_value(value):
    return value.isoformat() if value else None


def build_report_detail_query(config, outlet_code, report_date):
    model = config["model"]
    query = model.query.filter(model.outlet_code == outlet_code)
    query = apply_report_date_filter(
        query,
        config["date_expression"],
        report_date,
        config["is_datetime"],
    )

    for extra_filter in config.get("extra_filters", []):
        query = query.filter(extra_filter)

    return query


def serialize_report_record(record, config):
    record_date = getattr(record, config["date_attr"], None)
    if record_date is None and config.get("fallback_date_attr"):
        record_date = getattr(record, config["fallback_date_attr"], None)

    return {
        "id": getattr(record, config["id_attr"], None),
        "date": serialize_date_value(record_date),
        "gross_amount": to_float(getattr(record, config["gross_expression"].key)),
        "net_amount": to_float(getattr(record, config["net_expression"].key)),
        "display_name": getattr(record, config["display_attr"], None),
    }


def get_report_detail(platform, outlet_code, report_date):
    config = REPORT_EXISTENCE_CONFIG[platform]
    query = build_report_detail_query(config, outlet_code, report_date)
    summary = query.with_entities(
        func.count(),
        func.coalesce(func.sum(config["gross_expression"]), 0),
        func.coalesce(func.sum(config["net_expression"]), 0),
    ).one()
    record_count, gross_amount, net_amount = summary
    records = query.all()

    return {
        "exists": record_count > 0,
        "record_count": record_count,
        "gross_amount": to_float(gross_amount),
        "net_amount": to_float(net_amount),
        "records": [serialize_report_record(record, config) for record in records],
    }


def get_report_details(outlet_code, report_date):
    return {
        platform: get_report_detail(platform, outlet_code, report_date)
        for platform in REPORT_EXISTENCE_CONFIG
    }


def build_mutation_detail_query(platform, outlet, mutation_date):
    if platform == "gojek":
        gojek_store_id = normalize_platform_code(outlet.store_id_gojek)
        if not gojek_store_id:
            return BankMutation.query.filter(False)

        return BankMutation.query.filter(
            BankMutation.platform_name == "Gojek",
            BankMutation.tanggal == mutation_date,
            BankMutation.platform_code == gojek_store_id,
        )

    shopee_suffixes = shopee_store_suffixes(outlet.store_id_shopee)
    if not shopee_suffixes:
        return BankMutation.query.filter(False)

    platform_name = "ShopeeFood" if platform == "shopeefood" else "Shopee"
    return BankMutation.query.filter(
        BankMutation.platform_name == platform_name,
        BankMutation.tanggal == mutation_date,
        BankMutation.platform_code.in_(shopee_suffixes),
    )


def serialize_mutation_record(mutation):
    return {
        "id": mutation.id,
        "transaction_id": mutation.transaction_id,
        "date": serialize_date_value(mutation.tanggal),
        "platform_name": mutation.platform_name,
        "platform_code": mutation.platform_code,
        "amount": to_float(mutation.transaction_amount),
        "transaksi": mutation.transaksi,
    }


def get_mutation_detail(platform, outlet, mutation_date):
    query = build_mutation_detail_query(platform, outlet, mutation_date)
    record_count, amount = query.with_entities(
        func.count(),
        func.coalesce(func.sum(BankMutation.transaction_amount), 0),
    ).one()
    records = query.order_by(BankMutation.id.asc()).all()

    return {
        "exists": record_count > 0,
        "record_count": record_count,
        "amount": to_float(amount),
        "records": [serialize_mutation_record(mutation) for mutation in records],
    }


def get_mutation_details(outlet, mutation_date):
    return {
        "gojek": get_mutation_detail("gojek", outlet, mutation_date),
        "shopeefood": get_mutation_detail("shopeefood", outlet, mutation_date),
        "shopee": get_mutation_detail("shopee", outlet, mutation_date),
    }


def build_summary_outlet_query(outlet_code, brand_name):
    query = Outlet.query.filter(Outlet.status == "Active")

    if outlet_code:
        return query.filter(Outlet.outlet_code == outlet_code)
    if brand_name:
        return query.filter(Outlet.brand == brand_name)

    raise ValueError("outlet_code or brand_name is required")


@summary_bp.route("/platforms", methods=["GET"])
def get_platform_summary():
    try:
        report_date = parse_required_date()
        mutation_date = report_date + timedelta(days=1)
        outlet_code = request.args.get("outlet_code")
        brand_name = request.args.get("brand_name")
        page, per_page = get_pagination()

        query = build_summary_outlet_query(outlet_code, brand_name)
        total_records = query.count()
        total_pages = (total_records + per_page - 1) // per_page
        outlets = (
            query.order_by(Outlet.outlet_name_gojek)
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        outlet_codes = [outlet.outlet_code for outlet in outlets]
        report_existence_sets = get_report_existence_sets(outlet_codes, report_date)
        mutation_existence_sets = get_mutation_existence_sets(outlets, mutation_date)

        data = []
        for outlet in outlets:
            platforms = {
                platform: outlet.outlet_code in existing_outlet_codes
                for platform, existing_outlet_codes in report_existence_sets.items()
            }
            platforms["mutations"] = get_outlet_mutation_flags(outlet, mutation_existence_sets)

            row = serialize_outlet(outlet)
            row["platforms"] = platforms
            data.append(row)

        return jsonify({
            "date": report_date.isoformat(),
            "mutation_date": mutation_date.isoformat(),
            "filters": {
                "outlet_code": outlet_code,
                "brand_name": brand_name,
            },
            "data": data,
            "pagination": {
                "current_page": page,
                "per_page": per_page,
                "total_records": total_records,
                "total_pages": total_pages,
            },
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_bp.route("/platforms/details", methods=["GET"])
def get_platform_summary_details():
    try:
        report_date = parse_required_date()
        mutation_date = report_date + timedelta(days=1)
        outlet_code = request.args.get("outlet_code")
        if not outlet_code:
            raise ValueError("outlet_code is required")

        outlet = Outlet.query.filter(Outlet.outlet_code == outlet_code).first()
        if not outlet:
            return jsonify({"error": "outlet not found"}), 404

        platforms = get_report_details(outlet.outlet_code, report_date)
        platforms["mutations"] = get_mutation_details(outlet, mutation_date)

        return jsonify({
            "date": report_date.isoformat(),
            "mutation_date": mutation_date.isoformat(),
            "outlet": serialize_outlet(outlet),
            "platforms": platforms,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
