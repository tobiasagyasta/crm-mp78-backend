MPR_STANDARD_NET_RATE = 0.92
MPR_QRIS_OVO_NET_RATE = 0.98
MANAGEMENT_COMMISSION_RATE = 1 / 74
ENABLE_MP78_MANAGEMENT_AC = True

## Reminder : Make sure to add shopee_net_ac_value

def value_with_mutation_fallback(totals, mutation_key, net_key):
    return totals.get(mutation_key) or totals.get(net_key, 0)


def rated_value(value, is_mpr, rate):
    if is_mpr:
        return value * rate

    return value


def gofood_value(totals, is_mpr=False):
    gofood = totals.get('Gojek_Net', 0) - totals.get('Gojek_QRIS', 0)
    return rated_value(gofood, is_mpr, MPR_STANDARD_NET_RATE)


def gojek_qris_value(totals, is_mpr=False):
    return rated_value(
        totals.get('Gojek_QRIS', 0),
        is_mpr,
        MPR_QRIS_OVO_NET_RATE
    )


def gojek_net_value(totals, is_mpr=False):
    if is_mpr:
        return gofood_value(totals, is_mpr=True) + gojek_qris_value(totals, is_mpr=True)

    return totals.get('Gojek_Net', 0)


def gojek_net_ac_value(totals):
    gojek_qris = totals.get('Gojek_QRIS', 0)
    gofood = totals.get('Gojek_Net', 0) - gojek_qris
    return (
        (gojek_qris * MPR_QRIS_OVO_NET_RATE)
        + (gofood * MPR_STANDARD_NET_RATE)
        + (totals.get('Gojek_Difference') or 0)
    )


def grabfood_value(totals, is_mpr=False):
    grabfood = totals.get('Grab_Net', 0) - totals.get('GrabOVO_Net', 0)
    return rated_value(grabfood, is_mpr, MPR_STANDARD_NET_RATE)


def grab_ovo_value(totals, is_mpr=False):
    return rated_value(
        totals.get('GrabOVO_Net', 0),
        is_mpr,
        MPR_QRIS_OVO_NET_RATE
    )


def grab_net_value(totals, is_mpr=False):
    if is_mpr:
        return grabfood_value(totals, is_mpr=True) + grab_ovo_value(totals, is_mpr=True)

    return totals.get('Grab_Net', 0)


def grab_net_ac_value(totals):
    return grab_net_value(totals, is_mpr=True)


def shopee_net_value(totals, is_mpr=False):
    return rated_value(
        totals.get('Shopee_Net', 0),
        is_mpr,
        MPR_STANDARD_NET_RATE
    )


def shopee_net_ac_value(totals):
    return (
        (totals.get('Shopee_Net', 0) * MPR_STANDARD_NET_RATE)
        + (totals.get('Shopee_Difference') or 0)
    )


def shopeepay_net_value(totals, is_mpr=False):
    return rated_value(
        totals.get('ShopeePay_Net', 0),
        is_mpr,
        MPR_QRIS_OVO_NET_RATE
    )


def shopeepay_net_ac_value(totals):
    return (
        (totals.get('ShopeePay_Net', 0) * MPR_QRIS_OVO_NET_RATE)
        + (totals.get('ShopeePay_Difference') or 0)
    )


def standard_net_ac_value(totals, net_key):
    return totals.get(net_key, 0) * MPR_STANDARD_NET_RATE


def management_net_ac_value(totals, net_key, difference_key=None):
    net = totals.get(net_key, 0)
    difference = (totals.get(difference_key) or 0) if difference_key else 0
    return net - (net * MANAGEMENT_COMMISSION_RATE) + difference


def mp78_ac_value_for_header(totals, header):
    if header == 'Gojek_Mutation':
        return management_net_ac_value(totals, 'Gojek_Net', 'Gojek_Difference')
    if header == 'Grab_Net':
        return management_net_ac_value(totals, 'Grab_Net', 'Grab_Difference')

    return None


def mpr_ac_value_for_header(totals, header):
    if header == 'Gojek_Mutation':
        return gojek_net_ac_value(totals)
    if header == 'Grab_Net':
        return grab_net_ac_value(totals)
    if header == 'Shopee_Net':
        return shopee_net_ac_value(totals)
    if header == 'ShopeePay_Net':
        return shopeepay_net_ac_value(totals)
    if header == 'Tiktok_Net':
        return standard_net_ac_value(totals, 'Tiktok_Net')

    return None
