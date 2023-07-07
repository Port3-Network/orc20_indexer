import asyncio


def genarate_transaction_json(event, method, token_id=None, quantity=None):
    transaction = {
        "id": event.id,
        "inscription_id": event.inscription_id,
        "inscription_number": event.inscription_number,
        "block_height": event.block_height,
        "method": method,
        "token_id": token_id,
        "quantity": quantity,
        "from": event["from"],
        "to": event.to,
        "time": event.time,
        "valid": False,
        "invalid_reason": "invalid transaction"
    }

    return transaction


def generate_balance_json(event, token_info, balance_id, amount):
    return {
        "id": balance_id,
        "address": event.to,
        "tick": token_info["tick"],
        "tick_id": token_info["tick_id"],
        "inscription_id": token_info["inscription_id"],
        "inscription_number": token_info["inscription_number"],
        "balance": amount,
        "available_balance": amount,
    }


def generate_pool_send_json(event, content):
    return {
        "transaction_id": event.id,
        "inscription_id": event.inscription_id,
        "nonce": content["n"],
        "amt": content["amt"],
    }


def generate_pool_mint_json(event, amount):
    return {
        "transaction_id": event.id,
        "inscription_id": event.inscription_id,
        "amt": amount
    }


def save_invalid_transaction_task(pgsql, transaction, invalid_reason, balance_info=None):
    transaction["valid"] = False
    transaction["invalid_reason"] = invalid_reason
    tasks = [asyncio.ensure_future(pgsql.save_transaction_info(transaction))]
    if balance_info:
        tasks.append(asyncio.ensure_future(
            pgsql.save_balance_info(balance_info)))
    return tasks


async def update_to_balance_in_mint(pgsql, event, token_info, amount, mint_transaction=None):
    if mint_transaction is None:
        mint_transaction = generate_pool_mint_json(event, amount)
    else:
        mint_transaction["transaction_id"] = event.id

    balance_id = f"{event.to}-{token_info.id}"
    balance_infos = await pgsql.get_balance_by_id(balance_id)
    if not balance_infos:
        balance_json = generate_balance_json(
            event, token_info, balance_id, amount)
        balance_json["received_mint_pool"] = [mint_transaction]
        return [asyncio.ensure_future(pgsql.save_balance_info(balance_json))]

    balance_info = balance_infos[0]
    new_balance = float(balance_info.balance) + amount

    received_mint_pool = balance_info.received_mint_pool
    received_mint_pool.append(mint_transaction)

    return [asyncio.ensure_future(
        pgsql.update_balance_info(balance_id, {
            "balance": new_balance,
            "received_mint_pool": received_mint_pool,
        }))]


async def update_to_balance_in_send(pgsql, event, token_info, amount, send_transaction):
    balance_id = f"{event.to}-{token_info.id}"
    balance_info = await pgsql.get_balance_by_id(balance_id)
    send_transaction["transaction_id"] = event.id
    if not balance_info:
        balance_json = generate_balance_json(
            event, token_info, balance_id, amount)
        balance_json["received_send_pool"] = [send_transaction]
        return [asyncio.ensure_future(pgsql.save_balance_info(balance_json))]

    balance_info = balance_info[0]
    new_balance = float(balance_info.balance)
    if event["from"] != event["to"]:
        new_balance += amount
    new_available_balance = float(balance_info.available_balance) + amount

    received_send_pool = balance_info.received_send_pool
    received_send_pool.append(send_transaction)

    return [asyncio.ensure_future(
        pgsql.update_balance_info(balance_id, {
            "balance": new_balance,
            "available_balance": new_available_balance,
            "received_send_pool": received_send_pool,
        }))]


async def set_transaction_invalid(pgsql, pending_send_pool, invalid_reason):
    tasks = []
    for pending_send_transaction in pending_send_pool:
        pending_transactions = await pgsql.get_transaction_by_id(pending_send_transaction["transaction_id"])
        if not pending_transactions:
            continue
        tasks.append(asyncio.ensure_future(
            pgsql.update_transaction_info(
                pending_transactions[0].id,
                {"valid": False, "invalid_reason": invalid_reason})
        ))
    return tasks


async def query_token_by_tick_and_tick_id(pgsql, tick, tick_id, block_height=None):

    OIP3_BLOCK_HEIGHT = 788836

    # before oip3, use token_id to find token
    if block_height is not None and int(block_height) < OIP3_BLOCK_HEIGHT:
        return await get_token_by_token_id(pgsql, tick, tick_id)

    # after oip3, use inscription_number to find token
    if block_height is not None and int(block_height) >= OIP3_BLOCK_HEIGHT:
        return await get_token_by_inscription_number(pgsql, tick, tick_id)

    # if block_height is None, try both ways
    token = await get_token_by_token_id(pgsql, tick, tick_id)
    if token is not None:
        return token
    return await get_token_by_inscription_number(pgsql, tick, tick_id)


async def get_token_by_token_id(pgsql, tick, tick_id):
    token_id = f"{tick}-{tick_id}"
    token_infos = await pgsql.get_token_by_id(token_id)
    if token_infos:
        return token_infos[0]
    return None


async def get_token_by_inscription_number(pgsql, tick, tick_id):
    try:
        inscription_number = int(tick_id)
        token_infos = await pgsql.get_token_by_inscription_number(inscription_number, tick)
        if token_infos:
            return token_infos[0]
        return None
    except ValueError:
        return None
