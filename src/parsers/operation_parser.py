import os
import sys

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.parsers.field_parser import *  # noqa


def parse_upgrade_tick(content):
    tick_content = {
        "tick": parse_tick(content),
        "tick_id": parse_id_except_deploy(content),
    }

    for key, value in tick_content.items():
        if value is None:
            return None, f"{key} is invalid"

    return tick_content, None


def parse_upgrade_content(content, dec):

    upgrade_content = {"dec": dec}
    for key, value in content.items():
        if key == "dec":
            upgrade_content[key] = parse_deploy_dec(content)
            continue
        if key == "v":
            upgrade_content[key] = parse_v(content)
            continue
        if key == "msg":
            upgrade_content[key] = parse_msg(content)
            continue
        if key == "ug":
            upgrade_content[key] = parse_deploy_ug(content)
            continue

    for key, value in upgrade_content.items():
        if key == "max":
            upgrade_content[key] = parse_deploy_max(
                content, upgrade_content["dec"])
            continue
        if key == "lim":
            upgrade_content[key] = parse_deploy_lim(
                content, upgrade_content["max"], upgrade_content["dec"])
            continue

    for key, value in upgrade_content.items():
        if value is None:
            return None, f"{key} is invalid"

    return upgrade_content, None


def parse_deploy_content(content, content_block_height, inscription_number):

    deploy_content = {
        "tick": parse_tick(content),
        "name": parse_deploy_name(content),
        "v": parse_v(content),
        "msg": parse_msg(content),
        "ug": parse_deploy_ug(content),
        "wp": parse_deploy_wp(content),
        "tick_id": parse_deploy_id(content, content_block_height, inscription_number),
        "dec": parse_deploy_dec(content),
    }
    deploy_content["max"] = parse_deploy_max(
        content, deploy_content["dec"])
    deploy_content["lim"] = parse_deploy_lim(
        content, deploy_content["max"], deploy_content["dec"])

    for key, value in deploy_content.items():
        if value is None:
            return None, f"{key} is invalid"
    return deploy_content, None


def parse_mint_content(content):
    mint_content = {
        "tick": parse_tick(content),
        "tick_id": parse_id_except_deploy(content),
        "amt": parse_amt(content),
        "msg": parse_msg(content),
    }
    for key, value in mint_content.items():
        if value is None:
            return None, f"{key} is invalid"
    return mint_content, None


def parse_send_content(content):
    send_content = {
        "tick": parse_tick(content),
        "tick_id": parse_id_except_deploy(content),
        "msg": parse_msg(content),
        "n": parse_nonce(content),
    }
    amt = parse_amt(content)
    if amt is not None:
        send_content["amt"] = amt

    for key, value in send_content.items():
        if value is None:
            return None, f"{key} is invalid"
    return send_content, None


def parse_cancel_content(content):
    cancel_content = {
        "tick": parse_tick(content),
        "tick_id": parse_id_except_deploy(content),
        "msg": parse_msg(content),
        "n": parse_cancel_nonce(content),
    }
    for key, value in cancel_content.items():
        if value is None:
            return None, f"{key} is invalid"
    return cancel_content, None
