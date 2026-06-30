AVAILABLE_CLOSING_PLATFORMS = {
    "gojek": "Gojek",
    "grab": "Grab",
    "shopee_food": "ShopeeFood",
    "shopeepay": "ShopeePay",
    "tiktok": "Tiktok",
    "qpon": "Qpon",
    "webshop": "Webshop",
}

PLATFORM_ALIASES = {
    "gojek": "gojek",
    "gojekmutation": "gojek",
    "gojek_mutation": "gojek",
    "grab": "grab",
    "grabnet": "grab",
    "grab_net": "grab",
    "grab_net_raw": "grab",
    "shopee": "shopee_food",
    "shopeefood": "shopee_food",
    "shopee_food": "shopee_food",
    "shopeenet": "shopee_food",
    "shopee_net": "shopee_food",
    "shopeepay": "shopeepay",
    "shopee_pay": "shopeepay",
    "shopeepaynet": "shopeepay",
    "shopeepay_net": "shopeepay",
    "tiktok": "tiktok",
    "tiktoknet": "tiktok",
    "tiktok_net": "tiktok",
    "qpon": "qpon",
    "qponnet": "qpon",
    "qpon_net": "qpon",
    "webshop": "webshop",
    "webshopnet": "webshop",
    "webshop_net": "webshop",
}

HEADER_PLATFORM_MAP = {
    "Gojek_Mutation": "gojek",
    "Grab_Net": "grab",
    "Grab_Net_Raw": "grab",
    "Shopee_Net": "shopee_food",
    "ShopeePay_Net": "shopeepay",
    "Tiktok_Net": "tiktok",
    "Qpon_Net": "qpon",
    "Webshop_Net": "webshop",
}


def normalize_platform(platform):
    value = str(platform or "").strip().lower()
    value = value.replace("(ac)", "").replace("mpr", "")
    value = value.replace("-", "_").replace(" ", "_").replace("/", "_")
    value = "_".join(part for part in value.split("_") if part)
    compact_value = value.replace("_", "")

    canonical = PLATFORM_ALIASES.get(value) or PLATFORM_ALIASES.get(compact_value)
    if canonical:
        return canonical

    raise ValueError(f"Unsupported closing platform: {platform}")


def normalize_platforms(platforms):
    seen = set()
    for platform in platforms or []:
        canonical = normalize_platform(platform)
        seen.add(canonical)
    return [
        platform
        for platform in AVAILABLE_CLOSING_PLATFORMS
        if platform in seen
    ]


def disabled_platforms_for_outlet(outlet):
    return normalize_platforms(getattr(outlet, "disabled_closing_platforms", None) or [])


def is_platform_disabled(outlet, platform):
    if not outlet:
        return False
    return normalize_platform(platform) in disabled_platforms_for_outlet(outlet)


def platform_for_header(header):
    return HEADER_PLATFORM_MAP.get(header)


def available_platforms_payload():
    return [
        {"key": key, "label": label}
        for key, label in AVAILABLE_CLOSING_PLATFORMS.items()
    ]
