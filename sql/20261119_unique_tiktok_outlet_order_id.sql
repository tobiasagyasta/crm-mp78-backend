-- This intentionally fails if existing order IDs contain duplicates, including
-- IDs differing only by surrounding spaces. Review these first; do not delete
-- historical records automatically:
-- SELECT trim(outlet_order_id), count(*) FROM tiktok_reports
-- GROUP BY trim(outlet_order_id) HAVING count(*) > 1;
CREATE UNIQUE INDEX uq_tiktok_reports_outlet_order_id_trimmed
    ON tiktok_reports (trim(outlet_order_id));
