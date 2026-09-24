# Recorded OANDA practice-account responses

These files are REAL responses recorded from an OANDA **practice** account on **2026-09-23**, sanitised (account and user ids
removed; the account id appears as the placeholder `ACCOUNT_ID`); no token or other secret is in any file. They are
byte-for-byte what the recording produced after that sanitising, EXCEPT that they are stored one document per file: the
HTTP status of each response is not part of the file, it is listed in `tests/oanda_real.rs` (`RECORDED`).

They are observations of the practice environment, NOT of the live host. Live behaviour can differ.

What they establish (see `product-mandate/VENUE_FACTS.md`, "MEASURED on the practice accounts"):

| file (prefix `oanda_211402__`) | shape |
|---|---|
| `place_filled`, `duplicate_post` | market order created and filled; the same client id posted again produced a SECOND fill |
| `place_limit`, `get_limit_pending`, `get_limit_cancelled` | a resting limit order, its `PENDING` lookup by `@clientID`, and the 404 after it was cancelled |
| `get_by_client_id_filled`, `get_by_client_id_missing` | `GET /orders/@id` of a FILLED / never-seen order: 404 `NO_SUCH_ORDER` |
| `cancel_by_client_id`, `cancel_again` | cancel 200 with `orderCancelTransaction`; cancelling again 404 `ORDER_DOESNT_EXIST` |
| `sell_short` | a market sell with no position opened a short |
| `close_short`, `close_long_only` | position close responses (`...OrderCreateTransaction`, `...OrderFillTransaction`) |
| `close_wrong_side`, `close_nothing` | `ALL` for a side that does not exist (400) and nothing open at all (404), `CLOSEOUT_POSITION_DOESNT_EXIST` |
| `reject_too_big`, `reject_zero`, `reject_market_gtc`, `reject_fractional` | HTTP 400 with `orderRejectTransaction`, `rejectReason == errorCode` |
| `reject_bad_instrument` | unknown instrument: HTTP 400 `oanda::rest::core::InvalidParameterException`, NO reject transaction |
| `transactions_sinceid`, `transactions_list` | the transaction stream after a checkpoint, and the paged listing (page URLs, not transactions) |
| `list_orders_all` | an empty order list |
| `oanda_smoke__account_summary`, `oanda_smoke__instruments`, `oanda_smoke__pricing_home_conversions` | the account summary (with `lastTransactionID`), two instrument rows (`maximumPositionSize` "0", `tags` as objects) and a price with home conversions, recorded while the smoke test ran |
| `oanda_smoke__position_flat_previously_traded`, `oanda_smoke__position_never_traded_404`, `oanda_smoke__positions_all_with_flat_entries`, `oanda_smoke__open_positions_empty` | an instrument traded before and flat now is HTTP 200 with both sides at "0"; a never-traded one is 404 `NO_SUCH_POSITION`; `GET /positions` lists flat entries, `GET /openPositions` does not |

Not recorded (still authored from documentation, see `../README.md`): open positions and single positions that are actually
HELD (long, short, hedged), `GET /orders/<numeric id>` of a finished order, `GET /transactions/<id>`, cancel-at-creation bodies,
401/403/429/5xx bodies, and whether a close echoes `longClientExtensions`. Files named `oanda_211402__*` are the first recording
session, `oanda_smoke__*` the second.
