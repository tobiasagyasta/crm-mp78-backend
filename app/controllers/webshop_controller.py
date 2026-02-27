import csv
from io import StringIO

from flask import jsonify, request

from app.controllers.reports_controller import reports_bp
from app.extensions import db
from app.models.webshop_report import WebshopReport


@reports_bp.route("/upload/webshop", methods=["POST"])
def upload_report_webshop():
    files = request.files.getlist("file")
    if not files:
        single_file = request.files.get("file")
        files = [single_file] if single_file else []

    if not files:
        return jsonify({"msg": "No files uploaded"}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        debug_skipped = []
        seen_order_ids = set()

        for file in files:
            if file is None:
                continue

            file_contents = file.read().decode("utf-8-sig", errors="replace")
            reader = csv.reader(StringIO(file_contents))

            headers = next(reader, None)
            if not headers:
                skipped_reports += 1
                debug_skipped.append(
                    {
                        "row_number": 1,
                        "reason": "Missing CSV header row",
                        "row": [],
                    }
                )
                continue

            columns = WebshopReport._resolve_columns(headers)
            reports = []
            report_debug_rows = []

            for idx, row in enumerate(reader):
                row_number = idx + 2

                try:
                    parsed = WebshopReport.parse_webshop_row(row, columns)
                    if parsed is None:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Parse failed",
                                "row": row,
                            }
                        )
                        continue

                    order_id = parsed.get("order_id")
                    if not order_id:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Missing order_id",
                                "row": row,
                            }
                        )
                        continue

                    if order_id in seen_order_ids:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Duplicate order_id in upload",
                                "row": row,
                            }
                        )
                        continue

                    exists = WebshopReport.query.filter_by(order_id=order_id).first()
                    if exists:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Duplicate order_id in database",
                                "row": row,
                            }
                        )
                        continue

                    reports.append(WebshopReport(**parsed))
                    report_debug_rows.append((row_number, row))
                    seen_order_ids.add(order_id)
                    total_reports += 1
                except Exception as row_error:
                    db.session.rollback()
                    skipped_reports += 1
                    debug_skipped.append(
                        {
                            "row_number": row_number,
                            "reason": f"Row DB error: {str(row_error)}",
                            "row": row,
                        }
                    )
                    continue

            if reports:
                try:
                    db.session.bulk_save_objects(reports)
                    db.session.commit()
                except Exception as bulk_error:
                    db.session.rollback()
                    total_reports -= len(reports)
                    skipped_reports += len(reports)
                    for row_number, row in report_debug_rows:
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": f"Bulk insert failed: {str(bulk_error)}",
                                "row": row,
                            }
                        )

        return (
            jsonify(
                {
                    "msg": "Webshop reports uploaded successfully",
                    "total_records": total_reports,
                    "skipped_records": skipped_reports,
                    "skipped_rows_debug": debug_skipped,
                }
            ),
            201,
        )
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
