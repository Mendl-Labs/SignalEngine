"""Generates tests/fixtures/alpaca/*.json.

These are HAND-WRITTEN to Alpaca's documented response shapes (and to the field names visible in
SignalEngine's real-payload tests). They are NOT recordings from a live account.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))


def write(name, obj):
    path = os.path.join(OUT, name + ".json")
    with open(path, "w", newline="\n") as f:
        if isinstance(obj, str):
            f.write(obj + "\n")
        else:
            f.write(json.dumps(obj, indent=2) + "\n")


# ------------------------------------------------------------------ account
account = {
    "id": "904837e3-3b76-47ec-b432-046db621571b",
    "account_number": "PA3TESTFIXT1",
    "status": "ACTIVE",
    "currency": "USD",
    "cash": "52840.17",
    "portfolio_value": "100210.55",
    "equity": "100210.55",
    "last_equity": "99870.10",
    "buying_power": "152840.17",
    "long_market_value": "47370.38",
    "short_market_value": "0",
    "trading_blocked": False,
    "transfers_blocked": False,
    "account_blocked": False,
    "pattern_day_trader": False,
    "shorting_enabled": True,
    "multiplier": "2",
    "daytrade_count": 0,
}
write("account_ok", account)
write("account_live", {**account, "account_number": "912345678"})
write("account_trading_blocked", {**account, "trading_blocked": True})
write("account_account_blocked", {**account, "account_blocked": True})
write("account_not_active", {**account, "status": "ACCOUNT_UPDATED"})
write("account_pdt", {**account, "pattern_day_trader": True})
missing = dict(account)
del missing["trading_blocked"]
write("account_missing_blocked_flag", missing)

# ------------------------------------------------------------------ positions
def pos(symbol, qty, avg, mv, side="long", price=None):
    return {
        "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
        "symbol": symbol,
        "exchange": "ARCA",
        "asset_class": "us_equity",
        "avg_entry_price": avg,
        "qty": qty,
        "qty_available": qty,
        "side": side,
        "market_value": mv,
        "cost_basis": mv,
        "unrealized_pl": "12.34",
        "current_price": price or avg,
    }


write("positions_ok", [
    pos("SPY", "12.345678901", "512.10", "6321.55"),
    pos("EFA", "40", "81.25", "3300.00"),
    pos("IEF", "1.5", "95.40", "143.10"),
])
write("positions_empty", "[]")
write("positions_short", [pos("DBC", "10", "22.10", "-221.00", side="short")])

# ------------------------------------------------------------------ orders
def order(**kw):
    o = {
        "id": "61e69015-8549-4bfd-b9c3-01e75843f47d",
        "client_order_id": "mvp1:run1:SPY:buy",
        "created_at": "2026-09-21T13:31:05.123456Z",
        "updated_at": "2026-09-21T13:31:05.200000Z",
        "submitted_at": "2026-09-21T13:31:05.120000Z",
        "filled_at": None,
        "expired_at": None,
        "canceled_at": None,
        "failed_at": None,
        "replaced_at": None,
        "asset_id": "b28f4066-5c6d-479b-a2af-85dc1a8f16fb",
        "symbol": "SPY",
        "asset_class": "us_equity",
        "notional": None,
        "qty": "10",
        "filled_qty": "0",
        "filled_avg_price": None,
        "order_class": "",
        "order_type": "market",
        "type": "market",
        "side": "buy",
        "time_in_force": "day",
        "limit_price": None,
        "stop_price": None,
        "status": "accepted",
        "extended_hours": False,
        "legs": None,
    }
    o.update(kw)
    return o


write("order_accepted_market", order(qty="1.5"))
write("order_accepted_limit", order(qty="10", order_type="limit", type="limit", limit_price="512.10", status="new"))
write("order_pending_new", order(status="pending_new"))
write("order_filled", order(qty="1.5", filled_qty="1.5", filled_avg_price="154.03", status="filled",
                            filled_at="2026-09-21T13:31:06.500000Z"))
write("order_partially_filled", order(qty="10", filled_qty="4", filled_avg_price="512.30", status="partially_filled",
                                      order_type="limit", type="limit", limit_price="512.40"))
write("order_canceled_partial", order(qty="10", filled_qty="4", filled_avg_price="512.30", status="canceled",
                                      order_type="limit", type="limit", limit_price="512.40",
                                      filled_at="2026-09-21T13:35:00.000000Z",
                                      canceled_at="2026-09-21T13:40:00.000000Z"))
write("order_expired_partial", order(qty="10", filled_qty="2.5", filled_avg_price="512.30", status="expired",
                                     order_type="limit", type="limit", limit_price="512.40",
                                     expired_at="2026-09-21T20:00:00.000000Z"))
write("order_done_for_day_partial", order(qty="10", filled_qty="3", filled_avg_price="512.30", status="done_for_day",
                                          order_type="limit", type="limit", limit_price="512.40"))
write("order_canceled_zero", order(qty="10", status="canceled", canceled_at="2026-09-21T13:40:00.000000Z"))
write("order_expired_zero", order(qty="10", status="expired", expired_at="2026-09-21T20:00:00.000000Z"))
write("order_rejected", order(qty="10", status="rejected", failed_at="2026-09-21T13:31:06.000000Z"))
write("order_notional_filled", order(qty=None, notional="100", filled_qty="0.6493", filled_avg_price="154.01",
                                     status="filled", filled_at="2026-09-21T13:31:06.500000Z"))
write("order_unknown_status", order(status="replaced"))
missing_fq = order()
del missing_fq["filled_qty"]
write("order_missing_filled_qty", missing_fq)
write("order_wrong_client_id", order(client_order_id="someone-elses-order"))
write("order_no_client_id", order(client_order_id=None))
write("order_close_position", order(id="7a2c4d10-0000-4000-8000-00000000c105", client_order_id="a1b2c3d4-close",
                                    side="sell", qty="12.345678901", symbol="SPY", status="pending_new"))
write("orders_open", [
    order(id="61e69015-8549-4bfd-b9c3-01e75843f47d", client_order_id="mvp1:run1:SPY:buy", status="new",
          qty="10", order_type="limit", type="limit", limit_price="512.10"),
    order(id="0f9e0d1e-5b7a-4a52-9c2e-777777777777", client_order_id="e3b0c442-98fc-1c14-9afb-f4c8996fb924",
          symbol="EFA", side="sell", qty="5", status="partially_filled", filled_qty="2",
          filled_avg_price="81.30", order_type="limit", type="limit", limit_price="81.40"),
])
write("orders_empty", "[]")

# ------------------------------------------------------------------ errors
write("error_401_unauthorized", {"code": 40110000, "message": "request is not authorized"})
write("error_403_forbidden", {"message": "forbidden."})
write("error_403_buying_power", {"available": "1500.10", "buying_power": "1500.10", "cost_basis": "5121.00",
                                 "code": 40310000, "message": "insufficient buying power", "symbol": "SPY"})
write("error_403_pdt", {"code": 40310100, "message": "trade denied due to pattern day trading protection"})
write("error_422_not_fractionable", {"code": 42210000, "message": "asset \"BRK.A\" is not fractionable"})
write("error_422_qty_invalid", {"code": 42210000, "message": "qty must be > 0"})
write("error_422_insufficient_qty", {"available": "5", "code": 40310000, "existing_qty": "5",
                                     "held_for_orders": "0", "message": "insufficient qty available for order (requested: 10, available: 5)",
                                     "related_orders": [], "symbol": "SPY"})
write("error_422_duplicate_client_order_id", {"code": 40010001, "message": "client_order_id must be unique"})
write("error_422_not_cancelable", {"code": 42210000, "message": "order is not cancelable"})
write("error_404_order", {"code": 40410000, "message": "order not found for 61e69015-8549-4bfd-b9c3-01e75843f47d"})
write("error_404_position", {"code": 40410000, "message": "position does not exist"})
write("error_429", {"code": 42910000, "message": "rate limit exceeded"})
write("error_500", {"code": 50010000, "message": "internal server error"})

# ------------------------------------------------------------------ clock / calendar
write("clock_open", {"timestamp": "2026-09-21T09:45:12.345678-04:00", "is_open": True,
                     "next_open": "2026-09-22T09:30:00-04:00", "next_close": "2026-09-21T16:00:00-04:00"})
write("clock_closed", {"timestamp": "2026-09-19T12:00:00.000000-04:00", "is_open": False,
                       "next_open": "2026-09-21T09:30:00-04:00", "next_close": "2026-09-21T16:00:00-04:00"})
write("calendar_ok", [
    {"date": "2026-11-25", "open": "09:30", "close": "16:00", "session_open": "0400", "session_close": "2000",
     "settlement_date": "2026-11-27"},
    {"date": "2026-11-27", "open": "09:30", "close": "13:00", "session_open": "0400", "session_close": "1700",
     "settlement_date": "2026-11-30"},
    {"date": "2026-11-30", "open": "09:30", "close": "16:00", "session_open": "0400", "session_close": "2000",
     "settlement_date": "2026-12-01"},
])

# ------------------------------------------------------------------ assets
def asset(symbol, **kw):
    a = {
        "id": "b28f4066-5c6d-479b-a2af-85dc1a8f16fb",
        "class": "us_equity",
        "exchange": "ARCA",
        "symbol": symbol,
        "name": symbol + " test asset",
        "status": "active",
        "tradable": True,
        "marginable": True,
        "shortable": True,
        "easy_to_borrow": True,
        "fractionable": True,
    }
    a.update(kw)
    return a


write("asset_spy", asset("SPY"))
write("asset_whole_only", asset("BRK.A", exchange="NYSE", fractionable=False))
write("asset_inactive", asset("DEAD", status="inactive", tradable=False))
write("asset_min_order", asset("MINO", min_order_size="5", min_trade_increment="0.5", price_increment="0.05"))
write("asset_missing_tradable", {k: v for k, v in asset("BAD").items() if k != "tradable"})
write("assets_list", [asset("SPY"), asset("BRK.A", fractionable=False), asset("IEF")])

# ------------------------------------------------------------------ flatten
close_spy = order(id="7a2c4d10-0000-4000-8000-00000000c105", client_order_id="a1b2c3d4-close-spy", side="sell",
                  qty="12.345678901", symbol="SPY", status="pending_new")
close_efa = order(id="7a2c4d10-0000-4000-8000-00000000c106", client_order_id="a1b2c3d4-close-efa", side="sell",
                  qty="40", symbol="EFA", status="pending_new")
write("flatten_ok_207", [
    {"symbol": "SPY", "status": 200, "body": close_spy},
    {"symbol": "EFA", "status": 200, "body": close_efa},
])
write("flatten_partial_207", [
    {"symbol": "SPY", "status": 200, "body": close_spy},
    {"symbol": "EFA", "status": 403, "body": {"code": 40310000, "message": "insufficient qty available for order"}},
])
print("wrote", len(os.listdir(OUT)), "fixtures")
