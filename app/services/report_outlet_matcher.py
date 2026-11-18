from app.services.excel_export.mpr_calculations import MPR_BRANDS


def _key(value):
    return str(value or '').strip().strip("'").strip().casefold()


class ReportOutletMatcher:
    """Resolve report outlets and learn missing platform IDs in the transaction."""

    def __init__(self, outlets, platform):
        self.id_field = f'store_id_{platform}'
        self.updated_count = 0
        self.by_id = {}
        self.by_name = {}
        for outlet in outlets:
            identifier = _key(getattr(outlet, self.id_field))
            if identifier:
                self.by_id[identifier] = outlet
            for field in ('outlet_name_gojek', 'outlet_name_grab'):
                name = _key(getattr(outlet, field))
                if name:
                    matches = self.by_name.setdefault(name, [])
                    if not any(match is outlet for match in matches):
                        matches.append(outlet)

    def resolve(self, identifier, name):
        identifier_key = _key(identifier)
        if identifier_key and identifier_key in self.by_id:
            return self.by_id[identifier_key]
        matches = self.by_name.get(_key(name), [])
        if len(matches) != 1:
            return None
        outlet = matches[0]
        if _key(outlet.brand) in {_key(brand) for brand in MPR_BRANDS}:
            return outlet
        existing = _key(getattr(outlet, self.id_field))
        if identifier_key and existing and existing != identifier_key:
            return None
        if identifier_key and not existing:
            setattr(outlet, self.id_field, str(identifier).strip().strip("'").strip())
            self.by_id[identifier_key] = outlet
            self.updated_count += 1
        return outlet
