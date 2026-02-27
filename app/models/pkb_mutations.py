from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.outlet import Outlet


_PKB_CODE_NORMALIZE_RE = re.compile(r"^\s*([A-Za-z]{2,5})[\s-]?(\d{2,5})\s*$")
_PKB_CODE_WITH_SEPARATOR_RE = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z]{2,5})[-\s]+(\d{2,5})(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_PKB_CODE_COMPACT_RE = re.compile(
    r"(?:(?<=\s)|^)([A-Za-z]{2,5})(\d{2,5})(?=\s|$|[.,;:])",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_AMOUNT_WITH_DIRECTION_RE = re.compile(
    r"^\s*([+-]?\d[\d,]*(?:\.\d{1,2})?)\s*(DB|CR)\s*$",
    re.IGNORECASE,
)
_DIRECTION_THEN_AMOUNT_RE = re.compile(
    r"^\s*(DB|CR)\s*([+-]?\d[\d,]*(?:\.\d{1,2})?)\s*$",
    re.IGNORECASE,
)


class PKBMutation(db.Model):
    __tablename__ = "pkb_mutations"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    trx_date = db.Column(db.Date, nullable=False, index=True)
    keterangan = db.Column(db.Text, nullable=False)
    branch_code = db.Column(db.String(20), nullable=True)
    direction = db.Column(
        db.Enum("DB", "CR", name="pkb_mutation_direction"),
        nullable=False,
    )
    mutation_type = db.Column(
        db.Enum("minusan", "setoran", name="pkb_mutation_type"),
        nullable=False,
        index=True,
    )
    pkb_code = db.Column(db.String(16), nullable=True, index=True)
    outlet_code = db.Column(db.String(50), nullable=True, index=True)
    nominal_abs = db.Column(db.Numeric(18, 2), nullable=False)
    nominal_signed = db.Column(db.Numeric(18, 2), nullable=False)
    balance = db.Column(db.Numeric(18, 2), nullable=True)
    external_uid = db.Column(db.String(64), nullable=False, unique=True)
    raw_row = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())

    def __repr__(self) -> str:
        return (
            f"<PKBMutation id={self.id} trx_date={self.trx_date} "
            f"direction={self.direction} nominal_signed={self.nominal_signed}>"
        )


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_hash_value(value: Any) -> str:
    return re.sub(r"\s+", " ", _to_text(value))


def _parse_decimal(value: str) -> Decimal:
    cleaned = _to_text(value).replace(",", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if cleaned in {"", "-", ".", "-."}:
        raise ValueError(f"Invalid decimal value: {value}")
    if cleaned.count(".") > 1:
        raise ValueError(f"Invalid decimal value: {value}")
    try:
        parsed = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value}") from exc
    return parsed.quantize(Decimal("0.01"))


def _serialize_csv_row(row: list[str]) -> str:
    stream = io.StringIO()
    writer = csv.writer(stream, lineterminator="")
    writer.writerow(row)
    return stream.getvalue()


def normalize_pkb_code(raw: str) -> Optional[str]:
    """
    Normalize PKB code to canonical format PREFIX-NNN.
    """
    if not raw:
        return None

    normalized = _to_text(raw)
    match = _PKB_CODE_NORMALIZE_RE.match(normalized)
    if not match:
        compact = re.sub(r"[\s-]+", "", normalized)
        compact_match = re.fullmatch(r"([A-Za-z]{2,5})(\d{2,5})", compact)
        if not compact_match:
            return None
        prefix, digits = compact_match.group(1), compact_match.group(2)
        return f"{prefix.upper()}-{digits}"

    prefix, digits = match.group(1), match.group(2)
    return f"{prefix.upper()}-{digits}"


def extract_pkb_code(keterangan: str) -> Optional[str]:
    """
    Extract the first valid PKB code from keterangan.
    """
    if not keterangan:
        return None

    text = re.sub(r"\s+", " ", _to_text(keterangan))

    for pattern in (_PKB_CODE_WITH_SEPARATOR_RE, _PKB_CODE_COMPACT_RE):
        for match in pattern.finditer(text):
            candidate = f"{match.group(1)}-{match.group(2)}"
            normalized = normalize_pkb_code(candidate)
            if normalized:
                return normalized

    return None


def parse_jumlah(jumlah: str) -> tuple[str, Decimal, Decimal]:
    """
    Parse jumlah string into direction, absolute amount, and signed amount.
    """
    raw_value = re.sub(r"\s+", " ", _to_text(jumlah)).upper()
    match = _AMOUNT_WITH_DIRECTION_RE.match(raw_value)
    if match:
        amount_raw, direction = match.group(1), match.group(2).upper()
    else:
        inverted = _DIRECTION_THEN_AMOUNT_RE.match(raw_value)
        if not inverted:
            raise ValueError(f"Invalid jumlah format: {jumlah}")
        direction, amount_raw = inverted.group(1).upper(), inverted.group(2)

    amount_abs = _parse_decimal(amount_raw)
    if amount_abs < 0:
        amount_abs = -amount_abs

    amount_signed = amount_abs if direction == "CR" else -amount_abs
    return direction, amount_abs, amount_signed


def build_external_uid(
    trx_date: date,
    keterangan: str,
    cabang: Optional[str],
    jumlah: str,
    saldo: str,
    account_no: Optional[str] = None,
) -> str:
    """
    Build deterministic SHA-256 UID from stable source fields.
    """
    parts = [
        trx_date.isoformat(),
        _normalize_hash_value(keterangan),
        _normalize_hash_value(cabang),
        _normalize_hash_value(jumlah),
        _normalize_hash_value(saldo),
    ]
    if account_no:
        parts.append(_normalize_hash_value(account_no))
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_meta_row(row: list[str], meta: dict[str, str]) -> None:
    text = " ".join(_to_text(cell) for cell in row if _to_text(cell))
    if not text or ":" not in text:
        return

    key, value = text.split(":", 1)
    key_norm = key.strip().lower()
    value_norm = value.strip().strip(",")
    if not value_norm:
        return

    if "no" in key_norm and "rekening" in key_norm and "account_no" not in meta:
        meta["account_no"] = value_norm
    elif key_norm.startswith("nama") and "account_name" not in meta:
        meta["account_name"] = value_norm
    elif "kode mata uang" in key_norm and "currency" not in meta:
        meta["currency"] = value_norm
    elif "periode" in key_norm and "period" not in meta:
        meta["period"] = value_norm


def parse_pkb_bank_csv(file_path: str) -> tuple[dict[str, str], list[dict[str, Any]]]:
    """
    Parse PKB bank statement CSV into metadata and normalized transaction rows.
    """
    meta: dict[str, str] = {}
    parsed_rows: list[dict[str, Any]] = []

    with open(file_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        rows = list(reader)

    header_index: Optional[int] = None
    for idx, row in enumerate(rows):
        if not row:
            continue

        first_col = _to_text(row[0]).lower()
        if "tanggal transaksi" in first_col:
            header_index = idx
            break

        _parse_meta_row(row, meta)

    if header_index is None:
        raise ValueError("CSV header 'Tanggal Transaksi' not found.")

    for row in rows[header_index + 1 :]:
        if not row or not any(_to_text(cell) for cell in row):
            continue

        raw_row = _serialize_csv_row(row)
        first_col = _to_text(row[0])
        lower_first_col = first_col.lower()

        if lower_first_col.startswith("saldo awal"):
            break
        if lower_first_col.startswith("mutasi debet"):
            break
        if lower_first_col.startswith("mutasi kredit"):
            break
        if lower_first_col.startswith("saldo akhir"):
            break
        if not _DATE_RE.match(first_col):
            continue

        keterangan = _to_text(row[1]) if len(row) > 1 else ""
        branch_code = _to_text(row[2]) if len(row) > 2 else ""
        jumlah = _to_text(row[3]) if len(row) > 3 else ""
        saldo = _to_text(row[4]) if len(row) > 4 else ""

        try:
            trx_date = datetime.strptime(first_col, "%d/%m/%Y").date()
            direction, nominal_abs, nominal_signed = parse_jumlah(jumlah)
            balance = _parse_decimal(saldo) if saldo else None
        except ValueError:
            continue

        mutation_type = "minusan" if direction == "DB" else "setoran"
        pkb_code = extract_pkb_code(keterangan)

        parsed_rows.append(
            {
                "trx_date": trx_date,
                "keterangan": keterangan,
                "branch_code": branch_code or None,
                "direction": direction,
                "mutation_type": mutation_type,
                "pkb_code": pkb_code,
                "nominal_abs": nominal_abs,
                "nominal_signed": nominal_signed,
                "balance": balance,
                "external_uid": build_external_uid(
                    trx_date=trx_date,
                    keterangan=keterangan,
                    cabang=branch_code,
                    jumlah=jumlah,
                    saldo=saldo,
                    account_no=meta.get("account_no"),
                ),
                "raw_row": raw_row,
            }
        )

    return meta, parsed_rows


def _build_outlet_code_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    results = (
        db.session.query(Outlet.pkb_code, Outlet.outlet_code)
        .filter(Outlet.pkb_code.isnot(None), Outlet.outlet_code.isnot(None))
        .all()
    )
    for pkb_code, outlet_code in results:
        normalized = normalize_pkb_code(_to_text(pkb_code))
        if normalized and outlet_code:
            mapping[normalized] = outlet_code
    return mapping


def ingest_pkb_csv(file_path: str) -> dict[str, int]:
    """
    Ingest PKB CSV rows into pkb_mutations table in one transaction.
    """
    _, parsed_rows = parse_pkb_bank_csv(file_path)
    outlet_map = _build_outlet_code_map()

    created = 0
    skipped = 0

    try:
        for row_data in parsed_rows:
            resolved_outlet_code = (
                outlet_map.get(row_data["pkb_code"]) if row_data["pkb_code"] else None
            )
            mutation = PKBMutation(
                trx_date=row_data["trx_date"],
                keterangan=row_data["keterangan"],
                branch_code=row_data["branch_code"],
                direction=row_data["direction"],
                mutation_type=row_data["mutation_type"],
                pkb_code=row_data["pkb_code"],
                outlet_code=resolved_outlet_code,
                nominal_abs=row_data["nominal_abs"],
                nominal_signed=row_data["nominal_signed"],
                balance=row_data["balance"],
                external_uid=row_data["external_uid"],
                raw_row=row_data["raw_row"],
            )

            try:
                with db.session.begin_nested():
                    db.session.add(mutation)
                    db.session.flush()
                created += 1
            except IntegrityError:
                skipped += 1

        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    return {"created": created, "skipped": skipped}
