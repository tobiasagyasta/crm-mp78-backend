from dataclasses import dataclass
from datetime import date

@dataclass
class KasTransaction:
    tanggal: date
    keterangan: str
    tipe: str
    jumlah: float