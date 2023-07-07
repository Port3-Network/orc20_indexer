import ast


def parse_deploy_name(content):
    return content.get("name", "")


def parse_v(content):
    return content.get("v", "")


def parse_msg(content):
    return content.get("msg", "")


def parse_tick(content):
    if "tick" not in content:
        return None
    return str(content["tick"]).lower()


def parse_id_except_deploy(content):
    if "id" not in content:
        return None
    return str(content["id"]).lower()


def parse_amt(content):
    if "amt" not in content:
        return None

    amt = str(content["amt"])
    amt = amt.replace(",", "")
    try:
        amt = float(amt)
        if amt >= 0:
            return amt
    except ValueError:
        return None


def parse_nonce(content):
    if "n" not in content:
        return None
    n = content["n"]
    try:
        n = int(n)
        if n >= 0:
            return n
    except ValueError:
        return None


def parse_cancel_nonce(content):
    if "n" not in content:
        return None
    n = content["n"]
    try:
        nonce_list = ast.literal_eval(n)
        if type(nonce_list) is list:
            return nonce_list
    except Exception:
        return None


def parse_deploy_ug(content):
    ug = content.get("ug", "true")
    if ug not in ["true", "false"]:
        return None
    return True if ug == 'true' else False


def parse_deploy_wp(content):
    wp = content.get("wp", "true")
    if wp not in ["true", "false"]:
        return None
    return True if wp == 'true' else False


def parse_deploy_id(content, content_block_height, inscription_number):
    oip3_block_height = 788836
    if content_block_height >= oip3_block_height:
        return str(inscription_number)

    try:
        return str(content["id"]).lower()
    except KeyError:
        return None


def parse_deploy_dec(content):
    if "dec" not in content:
        return 18
    try:
        dec = int(content["dec"])
        if 0 <= dec <= 18:
            return dec
    except KeyError:
        return None


def parse_deploy_max(content, dec):
    if dec is None:
        return None

    uint256_max_str = 2 ** 256 - 1
    if "max" not in content:
        return str(uint256_max_str)

    max = str(content["max"])
    max = max.replace(",", "")
    try:
        if int(max) >= 0:
            return str(max)
    except ValueError as e:
        try:
            max = float(max)
            if max >= 0 and len(str(content["max"]).split(".")[1]) <= dec:
                return str(max)
        except ValueError:
            return None


def parse_deploy_lim(content, max, dec):
    if max is None or dec is None:
        return None

    if "lim" not in content:
        return "1"

    lim = str(content["lim"])
    lim = lim.replace(",", "")
    try:
        lim = int(lim)
        if lim >= 0 and lim <= int(max):
            return str(lim)
    except ValueError:
        try:
            lim = float(lim)
            if lim >= 0 and lim <= float(max) and len(str(content["lim"]).split(".")[1]) <= int(dec):
                return str(lim)
        except ValueError:
            return None
