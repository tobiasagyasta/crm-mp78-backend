def classify_report_source(row, columns):
    """Classify a shared export row without guessing about unknown sources."""
    def value(field):
        index = columns.get(field)
        if index is None or index >= len(row):
            return ""
        return str(row[index] or "").strip().strip("\"'").strip().casefold()

    provider = value("provider")
    source = value("order_source")
    if provider == "maxim" and source in ("", "maxim"):
        return "maxim"
    if provider == "klikit" and source == "webshop":
        return "webshop"
    return None
