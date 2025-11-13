from collections import defaultdict
from pathlib import Path

import pandas as pd
from flask import Blueprint, jsonify, current_app
from sqlalchemy.exc import SQLAlchemyError

from app.extensions import db
from app.models.outlet import Outlet

admin_tools_bp = Blueprint("admin_tools", __name__, url_prefix="/admin/tools")

EXCEL_FILENAME = "Code Webshop dan Tiktok.xlsx"


def normalize_cell(value) -> str:
    """Normalize incoming cell/string values for consistent comparisons."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _excel_file_path() -> Path:
    project_root = Path(current_app.root_path).parent
    return project_root / "example_tiktok" / EXCEL_FILENAME


@admin_tools_bp.route("/apply_outlet_codes_from_excel", methods=["POST"])
def apply_outlet_codes_from_excel():
    try:
        df = pd.read_excel(_excel_file_path())
    except Exception as exc:
        current_app.logger.exception("Failed to read TikTok/Webshop mapping Excel.")
        return jsonify({"status": "error", "message": str(exc)}), 500

    stats = {
        "total_excel_rows": 0,
        "updated_outlets": 0,
        "skipped_invalid_rows": 0,
        "skipped_no_match": 0,
        "skipped_already_up_to_date": 0,
    }

    outlets_by_name = defaultdict(list)
    for outlet in Outlet.query.all():
        normalized_name = normalize_cell(outlet.outlet_name_gojek)
        if normalized_name:
            outlets_by_name[normalized_name].append(outlet)

    for _, row in df.iterrows():
        stats["total_excel_rows"] += 1
        outlet_name = normalize_cell(row.get("outlet_name_gojek"))
        new_code = normalize_cell(row.get("Kode WEBSHOP dan Tiktok"))

        if not outlet_name or not new_code:
            stats["skipped_invalid_rows"] += 1
            continue

        matches = outlets_by_name.get(outlet_name, [])
        if not matches:
            stats["skipped_no_match"] += 1
            continue

        updates_this_row = 0
        for outlet in matches:
            existing_code = normalize_cell(outlet.outlet_code_tiktok_webshop)
            if existing_code == new_code:
                continue
            outlet.outlet_code_tiktok_webshop = new_code
            updates_this_row += 1

        if updates_this_row == 0:
            stats["skipped_already_up_to_date"] += 1
        else:
            stats["updated_outlets"] += updates_this_row

    try:
        db.session.commit()
    except SQLAlchemyError as exc:
        db.session.rollback()
        current_app.logger.exception("Failed to commit TikTok/Webshop outlet code updates.")
        return jsonify({"status": "error", "message": str(exc)}), 500

    stats["status"] = "ok"
    return jsonify(stats)
