import csv
from io import StringIO

from flask import jsonify, request

from app.controllers.reports_controller import reports_bp
from app.extensions import db
from app.models.qpon_reports import QponReport


@reports_bp.route("/upload/qpon", methods=["POST"])
def upload_report_qpon():
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
        seen_billing_ids = set()

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

            columns = QponReport._resolve_columns(headers)
            reports = []
            report_debug_rows = []

            for idx, row in enumerate(reader):
                row_number = idx + 2

                try:
                    parsed = QponReport.parse_qpon_row(row, columns)
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

                    billing_id = parsed.get("billing_id")
                    if not billing_id:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Missing billing_id",
                                "row": row,
                            }
                        )
                        continue

                    if billing_id in seen_billing_ids:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Duplicate billing_id in upload",
                                "row": row,
                            }
                        )
                        continue

                    exists = QponReport.query.filter_by(billing_id=billing_id).first()
                    if exists:
                        skipped_reports += 1
                        debug_skipped.append(
                            {
                                "row_number": row_number,
                                "reason": "Duplicate billing_id in database",
                                "row": row,
                            }
                        )
                        continue

                    reports.append(QponReport(**parsed))
                    report_debug_rows.append((row_number, row))
                    seen_billing_ids.add(billing_id)
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
                    "msg": "QPON reports uploaded successfully",
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
