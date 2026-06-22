from dataclasses import dataclass

from sqlalchemy import or_

from app.models.bank_mutations import BankMutation
from app.models.outlet import Outlet
from app.models.rekening import Rekening


@dataclass(frozen=True)
class OutletRekeningInfo:
    outlet_label: str
    platform_name: str
    rekening_name: str | None
    rekening_number: str | None

    @property
    def display_value(self) -> str:
        if not self.rekening_number:
            return "-"
        if self.rekening_name:
            return f"{self.rekening_name} - {self.rekening_number}"
        return self.rekening_number


class RekeningInfoService:
    PLATFORM_RULES = (
        ("Gojek", "store_id_gojek"),
        ("Grab", "store_id_grab"),
        ("ShopeeFood", "store_id_shopee"),
        ("Shopee", "store_id_shopee"),
    )

    @classmethod
    def get_outlet_rekenings(cls, outlet: Outlet) -> list[OutletRekeningInfo]:
        if not outlet:
            return []

        mutation_rows = cls._get_mutation_rekening_rows(outlet)
        if mutation_rows:
            return mutation_rows

        linked_rekening = cls._get_linked_rekening(outlet)
        if not linked_rekening:
            return []

        return [
            OutletRekeningInfo(
                outlet_label=cls.outlet_label(outlet),
                platform_name="Linked",
                rekening_name=linked_rekening.name,
                rekening_number=linked_rekening.rekening_number,
            )
        ]

    @classmethod
    def outlet_label(cls, outlet: Outlet) -> str:
        outlet_code = getattr(outlet, "outlet_code", None)
        brand = getattr(outlet, "brand", None)
        if brand and outlet_code:
            return f"{brand} - {outlet_code}"
        return outlet_code or getattr(outlet, "outlet_name_gojek", None) or "-"

    @classmethod
    def _get_mutation_rekening_rows(cls, outlet: Outlet) -> list[OutletRekeningInfo]:
        conditions = cls._build_mutation_conditions(outlet)
        if not conditions:
            return []

        rows = (
            BankMutation.query
            .filter(or_(*conditions))
            .with_entities(
                BankMutation.platform_name,
                BankMutation.rekening_number,
            )
            .distinct()
            .order_by(
                BankMutation.platform_name.asc(),
                BankMutation.rekening_number.asc(),
            )
            .all()
        )

        if not rows:
            return []

        rekening_numbers = [
            cls._clean_text(rekening_number)
            for _, rekening_number in rows
            if cls._clean_text(rekening_number)
        ]
        rekenings_by_number = {
            rekening.rekening_number: rekening
            for rekening in Rekening.query
            .filter(Rekening.rekening_number.in_(rekening_numbers))
            .all()
        }

        seen = set()
        result = []
        for platform_name, rekening_number in rows:
            rekening_number = cls._clean_text(rekening_number)
            if not rekening_number:
                continue

            key = (platform_name, rekening_number)
            if key in seen:
                continue
            seen.add(key)

            rekening = rekenings_by_number.get(rekening_number)
            result.append(
                OutletRekeningInfo(
                    outlet_label=cls.outlet_label(outlet),
                    platform_name=platform_name or "-",
                    rekening_name=rekening.name if rekening else None,
                    rekening_number=rekening_number,
                )
            )

        return result

    @classmethod
    def _build_mutation_conditions(cls, outlet: Outlet):
        conditions = []
        for platform_name, store_id_field in cls.PLATFORM_RULES:
            platform_codes = cls._platform_codes_for_outlet(outlet, platform_name, store_id_field)
            if not platform_codes:
                continue

            conditions.append(
                (BankMutation.platform_name == platform_name)
                & (BankMutation.platform_code.in_(platform_codes))
            )

        return conditions

    @classmethod
    def _platform_codes_for_outlet(
        cls,
        outlet: Outlet,
        platform_name: str,
        store_id_field: str,
    ) -> list[str]:
        store_id = cls._clean_text(getattr(outlet, store_id_field, None))
        if not store_id:
            return []

        if platform_name in ("ShopeeFood", "Shopee"):
            return cls._unique_values([store_id, store_id[-4:], store_id[-5:]])

        return [store_id]

    @classmethod
    def _get_linked_rekening(cls, outlet: Outlet):
        rekening_id = getattr(outlet, "rekening_id", None)
        if not rekening_id:
            return None
        return Rekening.query.get(rekening_id)

    @staticmethod
    def _clean_text(value) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @classmethod
    def _unique_values(cls, values: list[str | None]) -> list[str]:
        result = []
        seen = set()
        for value in values:
            clean_value = cls._clean_text(value)
            if clean_value and clean_value not in seen:
                result.append(clean_value)
                seen.add(clean_value)
        return result
