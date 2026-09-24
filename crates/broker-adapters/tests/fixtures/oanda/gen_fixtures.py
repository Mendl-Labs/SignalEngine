#!/usr/bin/env python3
"""Generates the OANDA JSON fixtures in this directory.

PROVENANCE: every fixture GENERATED HERE is AUTHORED FROM DOCUMENTATION (and the field names in the legacy
SignalEngine connector's OANDA parse), NOT recorded from a live or practice account. They exist only for
response shapes that have NOT been measured (see README.md next to this file); every shape that was measured
on a practice account on 2026-09-23 uses the recorded, sanitised response under real/ instead, and the
authored fixtures it replaced were deleted. Every object fixture carries `_fixture_provenance` so no one
mistakes them for recordings; a test asserts the label is present.

Run: python3 gen_fixtures.py   (rewrites the files next to it; output is deterministic)
"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
PROV = "authored from documentation, not recorded from a live account"
ACCT = "101-001-1234567-001"
TAG = "rb1:run1:EUR/USD:buy"
T0 = "1758463200.000000000"
T1 = "1758463201.250000000"


def w(name, obj):
    if isinstance(obj, dict):
        obj = {"_fixture_provenance": PROV, **obj}
    with open(os.path.join(HERE, name), "w", newline="\n") as f:
        if isinstance(obj, str):
            f.write(obj)
        else:
            f.write(json.dumps(obj, indent=2) + "\n")


def summary(**over):
    a = {
        "id": ACCT, "alias": "practice", "currency": "USD", "balance": "100000.0000",
        "createdByUserID": 1234567, "createdTime": "1700000000.000000000",
        "pl": "1200.5000", "resettablePL": "1200.5000", "financing": "-3.1000", "commission": "0.0000",
        "marginRate": "0.02", "openTradeCount": 2, "openPositionCount": 2, "pendingOrderCount": 1,
        "hedgingEnabled": False, "unrealizedPL": "250.5000", "NAV": "100250.5000",
        "marginUsed": "1100.0000", "marginAvailable": "99150.5000", "positionValue": "55000.0000",
        "marginCloseoutUnrealizedPL": "250.5000", "marginCloseoutNAV": "100250.5000",
        "marginCloseoutMarginUsed": "1100.0000", "marginCloseoutPositionValue": "55000.0000",
        "marginCloseoutPercent": "0.00549", "withdrawalLimit": "99150.5000",
        "marginCallMarginUsed": "1100.0000", "marginCallPercent": "0.01098",
        "lastTransactionID": "6400",
    }
    a.update(over)
    return {"account": a, "lastTransactionID": "6400"}


w("account_summary_ok.json", summary())
w("account_summary_hedging.json", summary(hedgingEnabled=True))
w("account_summary_other_account.json", summary(id="101-001-7654321-001"))
w("account_summary_short_pos.json", summary(balance="50000.0000", NAV="49800.0000", unrealizedPL="-200.0000", marginUsed="2500.0000", marginAvailable="47300.0000"))
bad = summary(); del bad["account"]["NAV"]
w("account_summary_missing_nav.json", bad)
bad = summary(); del bad["account"]["hedgingEnabled"]
w("account_summary_missing_hedging_flag.json", bad)
w("account_summary_eur.json", summary(currency="EUR", balance="10000.0000", NAV="10010.0000", unrealizedPL="10.0000", marginUsed="200.0000", marginAvailable="9810.0000"))


def inst(name, kind="CURRENCY", disp=5, units=0, mn="1", mx="100000000", margin="0.02", pip=-4):
    return {"name": name, "type": kind, "displayName": name.replace("_", "/"), "pipLocation": pip,
            "displayPrecision": disp, "tradeUnitsPrecision": units, "minimumTradeSize": mn,
            "maximumTrailingStopDistance": "1.00000", "minimumTrailingStopDistance": "0.00050",
            "maximumPositionSize": "0", "maximumOrderUnits": mx, "marginRate": margin,
            "guaranteedStopLossOrderMode": "DISABLED", "tags": [], "financing": {"longRate": "-0.0100", "shortRate": "0.0040"}}


w("instruments_ok.json", {"instruments": [
    inst("EUR_USD"), inst("GBP_USD"), inst("USD_JPY", disp=3, pip=-2), inst("AUD_USD"),
    inst("XAU_USD", kind="METAL", disp=3, units=0, mx="100000", margin="0.05", pip=-2),
    inst("DE30_EUR", kind="CFD", disp=1, units=1, mn="0.1", mx="2500", margin="0.05", pip=0),
], "lastTransactionID": "6400"})
bad = inst("EUR_USD"); del bad["tradeUnitsPrecision"]
w("instruments_malformed_row.json", {"instruments": [inst("GBP_USD"), bad], "lastTransactionID": "6400"})
w("instruments_bad_min.json", {"instruments": [inst("EUR_USD", mn="0")], "lastTransactionID": "6400"})


def pos(instrument, long_u="0", short_u="0", long_avg=None, short_avg=None, upl="0.0000", mu="0.0000"):
    l = {"units": long_u, "pl": "10.0000", "unrealizedPL": upl if long_u != "0" else "0.0000", "resettablePL": "10.0000"}
    s = {"units": short_u, "pl": "0.0000", "unrealizedPL": upl if short_u != "0" else "0.0000", "resettablePL": "0.0000"}
    if long_avg:
        l["averagePrice"] = long_avg; l["tradeIDs"] = ["6301"]
    if short_avg:
        s["averagePrice"] = short_avg; s["tradeIDs"] = ["6302"]
    return {"instrument": instrument, "pl": "10.0000", "unrealizedPL": upl, "marginUsed": mu,
            "resettablePL": "10.0000", "financing": "-0.4000", "commission": "0.0000",
            "guaranteedExecutionFees": "0.0000", "long": l, "short": s}


w("open_positions_ok.json", {"positions": [
    pos("GBP_USD", short_u="-5000", short_avg="1.27000", upl="-40.0000", mu="127.0000"),
    pos("EUR_USD", long_u="10000", long_avg="1.10000", upl="290.5000", mu="220.0000"),
], "lastTransactionID": "6400"})
w("open_positions_empty.json", {"positions": [], "lastTransactionID": "6400"})
w("open_positions_hedged.json", {"positions": [
    pos("EUR_USD", long_u="1000", short_u="-400", long_avg="1.10000", short_avg="1.10500", upl="5.0000"),
], "lastTransactionID": "6400"})
w("open_positions_bad_sign.json", {"positions": [pos("EUR_USD", long_u="-1000")], "lastTransactionID": "6400"})
w("position_single_long.json", {"position": pos("EUR_USD", long_u="10000", long_avg="1.10000", upl="290.5000", mu="220.0000"), "lastTransactionID": "6400"})
w("position_single_short.json", {"position": pos("GBP_USD", short_u="-5000", short_avg="1.27000", upl="-40.0000", mu="127.0000"), "lastTransactionID": "6400"})


# ---- orders (resources)
def order(oid, state, typ, instrument="EUR_USD", units="1000", cid=TAG, **extra):
    o = {"id": oid, "createTime": T0, "state": state, "type": typ, "instrument": instrument, "units": units,
         "timeInForce": "FOK" if typ == "MARKET" else "GTC", "positionFill": "DEFAULT"}
    if cid is not None:
        o["clientExtensions"] = {"id": cid, "tag": "mendl-rb"}
    o.update(extra)
    return o


w("order_filled_market.json", {"order": order("6372", "FILLED", "MARKET", fillingTransactionID="6373", filledTime=T1), "lastTransactionID": "6400"})
w("order_filled_market_sell.json", {"order": order("6382", "FILLED", "MARKET", units="-1000", cid="rb1:run1:EUR/USD:sell", fillingTransactionID="6383", filledTime=T1), "lastTransactionID": "6400"})
w("order_filled_no_txn_id.json", {"order": order("6372", "FILLED", "MARKET", filledTime=T1), "lastTransactionID": "6400"})
w("order_pending_limit.json", {"order": order("6390", "PENDING", "LIMIT", units="2000", cid="rb1:run1:EUR/USD:limit", price="1.09500", triggerCondition="DEFAULT", partialFill="DEFAULT_FILL"), "lastTransactionID": "6400"})
w("order_cancelled_client_request.json", {"order": order("6390", "CANCELLED", "LIMIT", units="2000", cid="rb1:run1:EUR/USD:limit", price="1.09500", cancellingTransactionID="6391", cancelledTime=T1), "lastTransactionID": "6400"})
w("order_cancelled_expired.json", {"order": order("6390", "CANCELLED", "LIMIT", units="2000", cid="rb1:run1:EUR/USD:limit", price="1.09500", cancellingTransactionID="6392", cancelledTime=T1), "lastTransactionID": "6400"})
w("order_cancelled_no_txn.json", {"order": order("6390", "CANCELLED", "LIMIT", units="2000", cid="rb1:run1:EUR/USD:limit", price="1.09500", cancelledTime=T1), "lastTransactionID": "6400"})
w("order_filled_partial.json", {"order": order("6372", "FILLED", "MARKET", units="1000", fillingTransactionID="6374", filledTime=T1), "lastTransactionID": "6400"})
w("order_filled_overfill.json", {"order": order("6372", "FILLED", "MARKET", units="1000", fillingTransactionID="6375", filledTime=T1), "lastTransactionID": "6400"})
w("order_filled_wrong_txn_order.json", {"order": order("6372", "FILLED", "MARKET", fillingTransactionID="6376", filledTime=T1), "lastTransactionID": "6400"})
w("order_filled_sign_flip.json", {"order": order("6372", "FILLED", "MARKET", fillingTransactionID="6377", filledTime=T1), "lastTransactionID": "6400"})
w("order_unknown_state.json", {"order": order("6372", "REPLACED", "MARKET"), "lastTransactionID": "6400"})
w("order_other_client_id.json", {"order": order("6372", "FILLED", "MARKET", cid="rb1:someone-else", fillingTransactionID="6373", filledTime=T1), "lastTransactionID": "6400"})

w("pending_orders_ok.json", {"orders": [
    order("6390", "PENDING", "LIMIT", units="2000", cid="rb1:run1:EUR/USD:limit", price="1.09500"),
    {"id": "6395", "createTime": T0, "state": "PENDING", "type": "STOP_LOSS", "tradeID": "6301", "price": "1.08000", "timeInForce": "GTC", "triggerCondition": "DEFAULT"},
    order("6396", "PENDING", "LIMIT", instrument="GBP_USD", units="-3000", cid=None, price="1.30000"),
    order("6397", "PENDING", "LIMIT", instrument="USD_JPY", units="500", cid="manual-ticket-9", price="148.500"),
], "lastTransactionID": "6400"})
w("pending_orders_empty.json", {"orders": [], "lastTransactionID": "6400"})


# ---- transactions
def fill(tid, oid, units, price, instrument="EUR_USD", cid=TAG, pl="0.0000", commission="0.0000"):
    return {"transaction": {"id": tid, "accountID": ACCT, "userID": 1234567, "batchID": oid, "requestID": "42", "time": T1,
            "type": "ORDER_FILL", "orderID": oid, "clientOrderID": cid, "instrument": instrument, "units": units,
            "gainQuoteHomeConversionFactor": "1", "lossQuoteHomeConversionFactor": "1", "price": price,
            "fullVWAP": price, "reason": "MARKET_ORDER", "pl": pl, "financing": "0.0000", "commission": commission,
            "guaranteedExecutionFee": "0.0000", "accountBalance": "100000.0000",
            "halfSpreadCost": "0.0500"}, "lastTransactionID": "6400"}


w("txn_fill_buy.json", fill("6373", "6372", "1000", "1.10052"))
w("txn_fill_sell.json", fill("6383", "6382", "-1000", "1.09948", cid="rb1:run1:EUR/USD:sell"))
w("txn_fill_buy_commission.json", fill("6373", "6372", "1000", "1.10052", commission="0.5000"))
w("txn_fill_partial.json", fill("6374", "6372", "400", "1.10052"))
w("txn_fill_overfill.json", fill("6375", "6372", "1500", "1.10052"))
w("txn_fill_wrong_order.json", fill("6376", "6999", "1000", "1.10052"))
w("txn_fill_sign_flip.json", fill("6377", "6372", "-1000", "1.10052"))
w("txn_fill_no_price.json", {"transaction": {"id": "6373", "type": "ORDER_FILL", "orderID": "6372", "instrument": "EUR_USD", "units": "1000"}})
w("txn_cancel_client_request.json", {"transaction": {"id": "6391", "time": T1, "type": "ORDER_CANCEL", "orderID": "6390", "reason": "CLIENT_REQUEST"}, "lastTransactionID": "6400"})
w("txn_cancel_expired.json", {"transaction": {"id": "6392", "time": T1, "type": "ORDER_CANCEL", "orderID": "6390", "reason": "TIME_IN_FORCE_EXPIRED"}, "lastTransactionID": "6400"})
w("txn_not_a_cancel.json", fill("6391", "6390", "1", "1.1"))


# ---- create-order responses
def create_txn(tid, typ="MARKET_ORDER", units="1000", cid=TAG, instrument="EUR_USD"):
    return {"id": tid, "accountID": ACCT, "userID": 1234567, "batchID": tid, "requestID": "42", "time": T0,
            "type": typ, "instrument": instrument, "units": units, "timeInForce": "FOK" if typ == "MARKET_ORDER" else "GTC",
            "positionFill": "DEFAULT", "reason": "CLIENT_ORDER", "clientExtensions": {"id": cid, "tag": "mendl-rb"}}


w("create_market_buy_filled.json", {"orderCreateTransaction": create_txn("6372"), "orderFillTransaction": fill("6373", "6372", "1000", "1.10052")["transaction"],
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_market_partial_fill.json", {"orderCreateTransaction": create_txn("6372"), "orderFillTransaction": fill("6374", "6372", "400", "1.10052")["transaction"],
    "relatedTransactionIDs": ["6372", "6374"], "lastTransactionID": "6374"})
w("create_market_cancelled_margin.json", {"orderCreateTransaction": create_txn("6372"),
    "orderCancelTransaction": {"id": "6373", "time": T1, "type": "ORDER_CANCEL", "orderID": "6372", "reason": "INSUFFICIENT_MARGIN"},
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_market_cancelled_liquidity.json", {"orderCreateTransaction": create_txn("6372"),
    "orderCancelTransaction": {"id": "6373", "time": T1, "type": "ORDER_CANCEL", "orderID": "6372", "reason": "INSUFFICIENT_LIQUIDITY"},
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_market_cancelled_halted.json", {"orderCreateTransaction": create_txn("6372"),
    "orderCancelTransaction": {"id": "6373", "time": T1, "type": "ORDER_CANCEL", "orderID": "6372", "reason": "MARKET_HALTED"},
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_market_no_fill_no_cancel.json", {"orderCreateTransaction": create_txn("6372"), "relatedTransactionIDs": ["6372"], "lastTransactionID": "6372"})
w("create_wrong_client_id.json", {"orderCreateTransaction": create_txn("6372", cid="rb1:someone-else"),
    "orderFillTransaction": fill("6373", "6372", "1000", "1.10052")["transaction"], "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_no_create_txn.json", {"orderFillTransaction": fill("6373", "6372", "1000", "1.10052")["transaction"], "lastTransactionID": "6373"})
w("create_fill_wrong_order.json", {"orderCreateTransaction": create_txn("6372"), "orderFillTransaction": fill("6373", "6999", "1000", "1.10052")["transaction"],
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_fill_wrong_direction.json", {"orderCreateTransaction": create_txn("6372"), "orderFillTransaction": fill("6373", "6372", "-1000", "1.10052")["transaction"],
    "relatedTransactionIDs": ["6372", "6373"], "lastTransactionID": "6373"})
w("create_cancel_wrong_order.json", {"orderCreateTransaction": create_txn("6372"),
    "orderCancelTransaction": {"id": "6373", "time": T1, "type": "ORDER_CANCEL", "orderID": "6999", "reason": "MARKET_HALTED"}, "lastTransactionID": "6373"})
w("create_json_but_truncated.json", '{"orderCreateTransaction": {"id": "6372", "clientExtensions": {"id": "rb1:run1:EUR/')

# ---- reject / error bodies
w("error_400_insufficient_margin.json", {"orderRejectTransaction": {"id": "6373", "time": T1, "type": "MARKET_ORDER_REJECT", "instrument": "EUR_USD", "units": "9000000",
    "rejectReason": "INSUFFICIENT_MARGIN", "clientExtensions": {"id": TAG}}, "relatedTransactionIDs": ["6373"], "lastTransactionID": "6373",
    "errorCode": "INSUFFICIENT_MARGIN", "errorMessage": "Insufficient margin to execute order"})
w("error_400_market_halted.json", {"orderRejectTransaction": {"id": "6373", "type": "MARKET_ORDER_REJECT", "rejectReason": "MARKET_HALTED"},
    "errorCode": "MARKET_HALTED", "errorMessage": "Market is halted"})
w("error_400_plain.json", {"errorMessage": "Invalid value specified for 'units'"})
w("error_401.json", {"errorMessage": "Insufficient authorization to perform request."})
w("error_403.json", {"errorMessage": "Forbidden"})
w("error_404_order.json", {"errorCode": "ORDER_DOES_NOT_EXIST", "errorMessage": "The Order specified does not exist"})
w("error_404_account.json", {"errorMessage": "Invalid value specified for 'accountID'"})
w("error_429.json", {"errorMessage": "Too Many Requests"})
w("error_500.json", {"errorMessage": "Internal server error"})
w("error_503_html.txt", "<html><body><h1>503 Service Unavailable</h1><p>secret-gateway-page</p></body></html>")
w("account_summary_truncated.json", '{"account": {"id": "101-001-1234567-001", "currency": "USD", "balance": "1000')

# ---- cancel / close
w("cancel_ok.json", {"orderCancelTransaction": {"id": "6391", "time": T1, "type": "ORDER_CANCEL", "orderID": "6390", "reason": "CLIENT_REQUEST"},
    "relatedTransactionIDs": ["6391"], "lastTransactionID": "6391"})
w("cancel_no_txn.json", {"relatedTransactionIDs": [], "lastTransactionID": "6391"})

CID = "rb1:fl:20260921T150000Z:EURUSD:1:abc"
w("close_cancelled.json", {
    "longOrderCreateTransaction": create_txn("6410", units="-10000", cid=CID),
    "longOrderCancelTransaction": {"id": "6411", "type": "ORDER_CANCEL", "orderID": "6410", "reason": "MARKET_HALTED"},
    "relatedTransactionIDs": ["6410", "6411"], "lastTransactionID": "6411"})
w("close_rejected_400.json", {"longOrderRejectTransaction": {"id": "6412", "type": "MARKET_ORDER_REJECT", "rejectReason": "MARKET_HALTED"},
    "relatedTransactionIDs": ["6412"], "lastTransactionID": "6412", "errorCode": "MARKET_HALTED", "errorMessage": "Market is halted"})
w("close_wrong_tag.json", {
    "longOrderCreateTransaction": create_txn("6410", units="-10000", cid="rb1:other"),
    "longOrderFillTransaction": fill("6411", "6410", "-10000", "1.10148")["transaction"], "lastTransactionID": "6411"})
w("close_no_transactions.json", {"relatedTransactionIDs": [], "lastTransactionID": "6411"})


# ---- pricing
def price(instrument, bid, ask, tradeable=True, cbid=None, cask=None):
    return {"type": "PRICE", "time": T1, "status": "tradeable" if tradeable else "non-tradeable", "tradeable": tradeable, "instrument": instrument,
            "bids": [{"price": bid, "liquidity": 10000000}] if tradeable else [], "asks": [{"price": ask, "liquidity": 10000000}] if tradeable else [],
            "closeoutBid": cbid or bid, "closeoutAsk": cask or ask}


w("pricing_ok.json", {"time": T1, "prices": [
    price("EUR_USD", "1.10048", "1.10052"), price("GBP_USD", "1.26996", "1.27004"), price("USD_JPY", "148.498", "148.512"),
], "homeConversions": [
    {"currency": "USD", "accountGain": "1.0", "accountLoss": "1.0", "positionValue": "1.0"},
    {"currency": "JPY", "accountGain": "0.00673", "accountLoss": "0.00674", "positionValue": "0.006736"},
]})
w("pricing_closed.json", {"time": T1, "prices": [price("EUR_USD", "1.10048", "1.10052", tradeable=False)], "homeConversions": []})
w("pricing_crossed.json", {"time": T1, "prices": [price("EUR_USD", "1.10060", "1.10040")], "homeConversions": []})
w("pricing_missing_instrument.json", {"time": T1, "prices": [], "homeConversions": []})
print("wrote", len([f for f in os.listdir(HERE) if f != "gen_fixtures.py"]), "fixtures")
