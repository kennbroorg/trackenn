<div align="center" style="margin-bottom: 10px;">
    <a href="https://twitter.com/intent/follow?screen_name=kennbroorg">
	<img alt="follow on Twitter" src="https://img.shields.io/twitter/follow/kennbroorg.svg?label=follow%20&style=for-the-badge&logo=twitter&labelColor=abcdef&color=1da1f2">
    </a>
</div>

<div align="center" style="margin-bottom: 10px;">
    <img alt="Python" src="https://img.shields.io/badge/python-3.14-informational.svg?style=for-the-badge">
    <img alt="FastAPI" src="https://img.shields.io/badge/interface-fastapi-yellowgreen.svg?style=for-the-badge">
</div>

---

<div align="center">
    <img alt="Logo" src="front/assets/Logo-color.png">
</div>

---

<h1 align="center">tracKenn</h1>

<h1 id="description">Description</h1>
Traceability project in blockchain. Ethereum and some features in Binance Smart Chain for now. Transactions, internals, ERC-20 transfers, contracts, internal tagging, labeling, etc

---

<div align="center">
    <a href="https://vimeo.com/917133268?share=copy"><img width="480" height="270" src="front/assets/trackenn.gif"></a>
</div>
<div align="center">
    <a href="https://vimeo.com/917133268?share=copy">Video Demo</a>
</div>

<h1 id="installation">Installation</h1>

<h3 id="docker">Docker (recommended)</h3>

Needs Docker with Compose and [just](https://github.com/casey/just#installation).

``` shell
git clone https://github.com/kennbroorg/trackenn.git
cd trackenn
cp config.example.yaml config.yaml    # then set your API keys, see API Keys
just up                               # http://127.0.0.1:4200
```

- Only `127.0.0.1:4200` is published (nginx). The API is reachable at `/api`, port 5000 stays inside Docker. Use another port with `TRACKENN_PORT=8080 just up`; the allowed origins follow it unless `TRACKENN_FRONT_ORIGINS` is set.
- The backend runs with your user id (`id -u`/`id -g`, or `TRACKENN_UID`/`TRACKENN_GID`), so `./data-db` stays yours.
- The databases live in `./data-db`. Bring an existing one with `just import default-LB.db` (it is copied with the `dbname` of `config.yaml`) and back it up with `just export`.
- The front image downloads the latest front release; pin one with `FRONTEND_VERSION=v0.0.6 just up`, or build a local checkout with `just up-local-front` (`TRACKENN_FRONT_DIR`, default `../trackenn-front`).
- `just` lists every recipe: `down`, `logs`, `ps`, `test`, `scan`, `rebuild`, `clean`...

<h3 id="native">Native</h3>

``` shell
git clone https://github.com/kennbroorg/trackenn.git    # Clone repo
cd trackenn                                             # Enter to directory cloned
pip install -r requirements.txt                         # Install dependencies
just front                                              # Download the web UI into ./front (see below)
python3 app.py                                          # Execute the app
```

`just front` downloads the latest front release asset into `./front` (same source the Docker image and the packaged binaries use). Pin a version with `just front v1.2.3`, or set `FRONTEND_VERSION` for an equivalent `curl` one-liner without `just`. The binaries (`trackenn-linux.zip`, `trackenn-windows.zip`) already bundle the front, no download needed there.

The API runs on FastAPI + Uvicorn at `http://127.0.0.1:5000` (interactive docs at `/docs`).

The API only accepts the front from `http://127.0.0.1:4200` (CORS and the `POST /reset_db` origin check). To serve the front from another origin, list them comma separated in `TRACKENN_FRONT_ORIGINS`, exactly as the browser sends them (scheme, host and port, no trailing slash; `*` is ignored):

``` shell
TRACKENN_FRONT_ORIGINS="http://127.0.0.1:4200,http://localhost:4200" python3 app.py
```

`GET /messages?address=<addr>&network=eth` reads the on-chain messages already stored for an address: transactions whose `input` decodes to readable UTF-8 text (e.g. Euler's negotiation with its attacker), where the address is `from` or `to` (case-insensitive). Read-only, ordered by time. Missing `address` answers 400; an unknown address, one with no messages or a database not created yet answers an empty list. A database that exists but cannot be read (locked by a collection in progress, corrupt) answers 503 with a fixed message (the sqlite error is only logged), never an empty list. A database file that exists but cannot be opened (permissions, read-only mount) counts as unreadable, not as "not created yet". `network` defaults to `eth` (only chain supported so far).

`group=true` collapses repeated messages: groups by `(from, to, text)` (case-insensitive), ordered by `first_timeStamp`, each entry `{from, to, text, count, first_timeStamp, last_timeStamp, hashes}` with every transaction hash kept in time order (so, e.g., a sender repeating the same text to the central address shows up once with its `count`, instead of one row per repetition). `limit`/`offset` paginate either the plain messages or, with `group=true`, the groups: `limit` is a positive integer up to 1000, `offset` is >= 0 (0 by default); an invalid value answers 400. Giving `limit` and/or `offset` wraps the response in `{total, limit, offset, items}` (`total` counts messages, or groups when `group=true`); without them the body is the plain list, unchanged. The `non_message_skipped` decision below always counts every skipped transaction of the address, not just the current page. The lookup uses the `idx_transactions_from_lower`/`idx_transactions_to_lower` expression indexes on `t_transactions` (created at schema init and, idempotently, at every collection, so older databases get them too), so matching by address never scans the whole table.

`GET /created-contracts?address=<addr>&network=eth&max_calls=<n>` lists the contracts an address created, directly and through the contracts it created in turn, recursively: `{address, contracts: [{address, creator, level, creations: [{hash, timeStamp, type}]}], truncated, limit, incomplete, pending, calls, decisions}`. Level 1 comes from data already stored when the address was collected (no API call), or from its own `txlist`/`txlistinternal` when it was not; deeper levels call Etherscan's `txlistinternal` for each contract found at the previous level, one call per contract. Deduplicates by contract address, not by transaction hash: a `CREATE` that reverted and was retried keeps both creation transactions under the same contract (`creations` has 2 entries). Stops at depth 3 or 50 contracts total, whichever comes first, and reports it (`truncated: true`, `limit: "depth"` or `"contracts"`). The contracts cap makes no further API call; at the depth cap no verification call is made either, so a chain that legitimately ends there is indistinguishable from one the cap cut short unless the cache already has the answer (`depth_cap_reached`) — otherwise the cut is reported as unverified (`depth_cut_unverified`). `max_calls` (default 50, max 200, values outside that range are clamped) caps every explorer download spent answering the query, cache hits included for free; `calls` reports how many were actually made, and running out mid-walk keeps everything found so far, marks `incomplete: true` and lists the unexpanded addresses in `pending` (`call_cap_reached`).

Each contract's creations are cached in `t_created_contracts_cache`, so a repeated query makes no API call for contracts already fetched; `refresh=true` refetches from the last cached block and merges new creations. This includes level 1 of an address never collected: its `txlist` and `txlistinternal` downloads are cached too (under distinct cache keys, since both answer for the same address), so a second query for a popular, uncollected address also costs no calls. If an account list (`txlist`, `txlistinternal`) hits its row cap (`api_client.MAX_ROWS_PER_LIST`), the walk still returns what it has but reports it with a `history_incomplete` decision instead of staying silent. If Etherscan fails mid-walk (rate limit, timeout), the walk stops calling and returns what it found with `incomplete: true` and `pending` (contracts not expanded); failed calls are never cached, so the next query retries only those. Missing `address` answers 400; `network` defaults to `eth`.

`decisions` is the evidence of every automatic choice the walk made on the analyst's behalf, for the UI to show: `[{code, scope, rule, reason, details}]`, also appended to `t_decisions`. A database that cannot be read is logged and answered as `incomplete: true` with a `db_error` decision, never as "not collected". Codes: `db_error`, `served_from_cache`, `cache_unavailable`, `upstream_error`, `history_incomplete`, `size_cap_reached`, `depth_cap_reached`. A decision never deletes data, it says what was left out and why. `reason` never carries an API key (urls are masked). A `db_error` decision is returned and logged but cannot be stored in `t_decisions` while that same database is unreadable.

The collection flow (`/stream-data-info`) leaves the same kind of evidence in `t_decisions`. Every token contract the internal spam/phishing classifier tags (core/token_classifier.py) gets a `token_flagged` decision scoped to the contract address, naming the tag that fired as `rule` and, in `details`, the value that triggered it (a token name/symbol, a spoofed sender address; `null` when it cannot be recovered) plus the central address being investigated. Whenever a `txlist`/`txlistinternal`/`tokentx`/`tokennfttx`/`token1155tx` download hits its row cap (`api_client.MAX_ROWS_PER_LIST`), a `history_incomplete` decision is recorded for that address and list, in the initial collection and in the cached-path catch-up alike. Both are deduplicated per `(code, scope, rule, details)` and query address (so two capped lists of one address are two decisions), and re-collecting or re-tagging an already-investigated address never produces duplicate decisions. Re-collecting an address also removes stale `spam-unsolicited` tags written by an older classifier rule: for each token contract in that address's transfers, the tag is dropped (from `t_tags` and the graph's `t_nodes_classification`) only when the contract has no strong-signal tag (`spam-phishing`, `spam-spoofed-sender`, `spam-native-impersonation`) from any investigation. No other tag is touched, and each removal leaves a `tag_removed` decision (rule `unsolicited-without-strong-signal`, `details.previous_tag`, the central address).

`spam-homoglyph` (S6/S8, odd/tasks/backend-backlog-c.md T7/T8): a strong signal, like `spam-phishing`, `spam-spoofed-sender` and `spam-native-impersonation`, so it also confirms `spam-unsolicited`. A token's name or symbol is run through a skeleton pipeline before comparison: first every Unicode format character (category Cf: zero-width characters, direction marks) is removed, then the result is normalized with the stdlib `unicodedata.normalize("NFKC", ...)` (collapsing full-width letters like `Ｗ` and mathematical alphanumeric symbols like `𝐖` to plain Latin), then every pinned Cyrillic/Greek letter visually confusable with a Latin one is replaced by that Latin letter (e.g. Cyrillic `В` U+0412 -> `B`). The token is flagged when the original text is non-ASCII, its skeleton differs from the original text, and the skeleton (trimmed, case-insensitive) equals a symbol or name in the pinned Uniswap list (`data/tokenLists.json`) or the chain's native coin -- while the contract is not that token's own official address (the native coin is never an ERC-20, so it is always flagged; a non-ASCII official contract whose skeleton is its own symbol/name is never flagged). The pinned confusables table and the symbol/name -> official contract index both live in `core/token_classifier.py`; the index is built once per process (cached like the other classifier data/ snapshots), no new dependency and no network call. Its `token_flagged` evidence carries the original text, the skeleton, the imitated symbol, the official contract it imitates (`"native"` for the native coin), the normalization applied (`"normalization": "NFKC"`, always present), and the list of every character changed to compute the skeleton in reading order, including NFKC changes (e.g. `"Ｗ U+FF37 -> W"`) and removed invisible characters (e.g. `"U+200B -> (removed)"`).

The classifier's public lists are versioned snapshots in `data/`, not live network calls: `data/phishingDomains.json` (the MetaMask `eth-phishing-detect` blacklist) and `data/tokenLists.json` (chainId-1 tokens from the Uniswap Labs default list) each carry `{source, revision, ...}` so a regeneration is auditable; `data/classifierLists.json` holds the hand-curated lure words, impersonated wallets and the always-on baseline token contracts. `core/token_classifier.py` loads all three once per process and never touches the network at runtime. A token name that only matches the blacklist (no URL with one of the usual TLDs, no lure word) is not tagged when its contract has a non-risky label in `data/etherscanCombinedAllLabels.json` (a known project token, e.g. Tornado Cash's own vTORN "Tornado.Cash Voucher"); labels such as `blocked` or `spam-token` never count as known. A missing or invalid file falls back to the hardcoded baseline, logs a warning, and is reported as a `cache_unavailable` decision (one per broken file) instead of failing silently. Regenerate the two downloaded snapshots with `python install/labels/update_classifier_lists.py`, which refuses to write anything if the source does not match the revision pinned in the script (same pattern as `install/labels/update_ofac_labels.py`); bump the pin on purpose, with the new revision reviewed, never by pointing it at whatever the source serves today.

`data/functionSignatures.json` (S12, odd/tasks/backend-backlog-c.md T12) pins the text signatures GET /calls falls back to when a selector is not resolved from the stored `functionName` or a fetched ABI: `{source, generated_at, signatures: {<0xselector>: [<text signature>, ...]}}`, selectors lowercased, signature lists sorted and deduplicated; a selector 4byte.directory has nothing for is omitted (absent means unknown), never an empty list. Regenerate it with `python install/signatures/update_signatures.py --db <path> [--db <path> ...]`, which looks up a hardcoded common set (ERC-20, WETH, Uniswap V2/V3 routers, multicall, ERC-721, permit) plus every distinct 4-byte selector found in `t_transactions.input` across the given SQLite databases (opened read-only); unlike `update_ofac_labels.py`/`update_classifier_lists.py` there is no single revision to pin against a crowd-submitted database, so the source URL and the generation date are recorded instead. A network failure leaves the existing file untouched and exits non-zero; no network call happens at app runtime.

`GET /messages` only returns transactions whose `input` decodes as readable text. When at least one transaction is skipped (contract calls, binary data), it records one summary decision per query, `non_message_skipped`, with the number of transactions skipped and shown. The `/messages` response body itself is unchanged.

`GET /balances?address=<addr>&network=eth[&onchain=true&max_calls=<n>]` computes the case balance from the data already collected, without calling the explorer: `{address, eth: {inflow, outflow, fees, net}, tokens: [{contract, symbol, decimals, inflow, outflow, net, transfers_in, transfers_out, flagged_tags}], spam: [...same shape...], nfts: [{contract, token_id, symbol, standard, units_in, units_out, held}], decisions}`. `network` only accepts `eth` or `bsc`; any other value answers 400 with `Invalid 'network' value: use eth or bsc`. ETH adds `t_transactions` and `t_internals` values (failed ones skipped) and subtracts the gas the address paid; ERC-20 amounts come from `t_transfers`, grouped by `contractAddress` and scaled by `tokenDecimal` (amounts are decimal strings). Contracts the classifier tagged `spam-*` are listed in `spam` with their tags, never in `tokens`. ERC-721 (`t_nfts`, one unit per transfer) and ERC-1155 (`t_multitoken`, `tokenValue`) are counted in `nfts`, outside the monetary total. Decisions: `balance_partial` when a `history_incomplete` decision exists for the address (balances are bounds), `balance_negative` for every real token (or ETH) whose outflow exceeds its inflow, `value_unreadable` when a stored `value`, `gasUsed`, `gasPrice`, `tokenDecimal` or `tokenValue` is a non-empty value that is not a number (it still counts as 0 in every total, same as today); one decision per token contract (or per queried address for ETH-side rows) with `details: {count, fields, hashes}` (`fields` is the sorted list of affected columns, `hashes` up to 5 example tx hashes) and a matching warning log line. A NULL or empty value is common and benign in stored data (e.g. 0-value transactions, some rows left blank) and stays silent, also counted as 0. With `onchain=true` the computed nets are compared with the live chain (`balance`, then `tokenbalance` per real ERC-20, spam never queried) under `onchain: {eth, tokens: {<contract>: {computed, chain, difference}}}`; `max_calls` (default 20, minimum 1, invalid -> 400; values above 200 are clamped to 200 and `onchain.max_calls` reports the value applied) caps those calls and the rest are listed in a `call_cap_reached` decision; each difference leaves `balance_mismatch`; an explorer failure keeps the computed result and adds `upstream_error` (the API key is never echoed). Read-only: decisions are returned, not stored. Missing `address` answers 400; an unreadable database answers 503.

`GET /risk-flows?address=<addr>&network=eth` lists the direct counterparties of the address that are mixers or sanctioned, from the data already collected (`t_transactions`, `t_internals`, `t_transfers`; failed transactions skipped) and the labels in `t_labels`, without calling the explorer: `{address, counterparties: [{counterparty, name, kinds, labels, direction, per_token: [{contract, symbol, amount, count}], first_timeStamp, last_timeStamp, hashes}], decisions}`. `kinds` holds `mixer` (label `ethereum-mixer`, or `tornado-cash` on a deposit contract: a pool such as `Tornado.Cash: 100 ETH`, or the Router, Proxy, Old Proxy and Mixer N entry points; governance, TORN/vTORN, verifiers and vesting are not mixers) and/or `sanctioned` (label `ofac-sdn` or `ofac-sanctions-lists`); when an address has several `t_labels` rows their labels are merged and the first non-empty name is kept. One entry per counterparty and `direction` (`in`/`out`) with every tx hash; native coin amounts use `contract: "eth"`. Each token's `amount` is scaled per row using that row's own stored `tokenDecimal` (not a single value taken from the first row of the contract), so a contract whose rows disagree on `tokenDecimal` still sums correctly and leaves a `decimals_inconsistent` decision (`scope` the contract, `details: {decimals, hashes}` with the sorted distinct decimal values seen and up to 5 example hashes). `per_token[].count` and `flow_risky`'s `details.count` count movements (ERC-20 transfer rows or native-coin transactions/internal transactions), not distinct transaction hashes: several transfers in the same transaction (e.g. two ERC-20 transfers in one hash) add to `count` but the same hash is listed only once in `hashes`, so `count` can exceed the number of hashes. Decisions, stored once per distinct content: `flow_risky` per entry (`details: {direction, kinds, name, count, hashes}` with up to 5 example hashes) `mixer_trail_lost` for an inflow from a mixer (the trail cannot be followed back), `value_unreadable` (same shape as in `/balances`) when a risky flow has a non-numeric `value` or `tokenDecimal`, counted as 0, and `decimals_inconsistent` (S13(e), odd/tasks/backend-backlog-c.md) for a contract whose rows carry different `tokenDecimal` values. `network` only accepts `eth` or `bsc` (400 otherwise, same message as `/balances`); missing `address` answers 400; an unreadable database answers 503.

`GET /calls?address=<addr>|hash=<tx hash>&network=eth&max_calls=<n>` decodes the `input` of stored transactions into named calls, read-only (one of `address` or `hash` is required, 400 otherwise): `{address, hash, max_calls, calls: [{hash, to, kind, methodId, function, candidates, source, params}], decisions}`. A stored row with a NULL or empty `input` is not a call (nothing to decode) and is silently skipped, never a crash. `kind` is `"call"`, `"creation"` (empty `to`) or `"message"` (readable text, same rule as `/messages`); creations and messages are not decoded. Name resolution order (`source`): the stored `functionName` (`"etherscan"`) -> a contract ABI fetched once per contract via Etherscan's `getabi` and cached in `t_abi_cache` (`"abi"`) -> the pinned `data/functionSignatures.json` table, filtered by whether the calldata fits each candidate's shape (`"signatures"`) -> `null` (unknown, raw params, never an invented name). Etherscan's stored `functionName` often carries parameter names (e.g. `transfer(address _to, uint256 _value)`); only the type of each parameter is used to decode, the full text (names included) is still shown as `function`. A `functionName` that is not a well-formed signature (no `(`, or no closing `)`) is not usable and falls back to the next source instead of failing the request. Static ABI/signature types (`address`, `uintN`, `intN` two's complement, `bool`, `bytes1`-`bytes32`) are decoded as `{type, value}`; dynamic ones (`string`, `bytes`, arrays, tuples) are kept as their raw head word with `value_undecoded: true`; without any resolved signature, `params` are the raw 32-byte words. When several pinned signatures share a selector, candidates whose shape cannot match the actual calldata are discarded first (wrong length for an all-static signature, an `address` word with non-zero upper 12 bytes, a `bool` word that is not 0 or 1, a dynamic offset outside the data); if exactly one candidate still fits it is used (`source: "signatures"`), otherwise every fitting candidate is listed in `candidates` and a `selector_ambiguous` decision is recorded (`details: {candidates, discarded, count, hashes}`, `discarded` the real number of candidates the shape filter ruled out), never a guess between tied candidates — e.g. the real `transfer(address,uint256)` selector `0xa9059cbb` also collides in 4byte.directory with `workMyDirefulOwner(uint256,uint256)`, which fits any calldata that fits `transfer` (an address is a valid `uint256`), so that selector is honestly ambiguous rather than silently resolved. `max_calls` (default 20, minimum 1, invalid -> 400; above 200 clamped to 200, `max_calls` in the response reports the value applied) caps `getabi` calls; cache hits are free, and running out leaves the rest unresolved via `call_cap_reached`. An unresolved selector leaves one `selector_unknown` decision per selector (`count`, example `hashes`); a resolved call with any dynamic parameter leaves `value_undecoded` (`types`, `count`, `hashes`); a `getabi` failure (other than "not verified", which is itself cached, including network/transport errors and a non-JSON response body) keeps every already-resolved call and adds `upstream_error` (the API key is never echoed, same `core.api_client.redact` as `/balances`/`/risk-flows`). ABI cache entries age (S13(a)-(b), odd/tasks/backend-backlog-c.md): a verified ABI never expires (immutable once verified), an unverified entry is re-fetched after 7 days (the contract may be verified later), and a failed `getabi` is cached as failed for 10 minutes so repeated queries during a rate limit spend no calls on it; a contract still within that 10-minute window is listed in the `upstream_error` decision's `details.skipped_recent_failure`. A cache table from before this versioning (no `status` column) is migrated idempotently on first use. Read-only over investigation tables: only `t_abi_cache` and `t_decisions` are written (K8).

`t_abi_cache` being unreadable or unwritable (a read-only mount, a corrupt table) never fails `GET /calls`: it answers 200 with every call resolved through the remaining sources (`functionName`, pinned signatures) and a `cache_unavailable` decision. `network` only accepts `eth` or `bsc` (400 otherwise, same message as `/balances`); an unreadable database answers 503.

`GET /decisions?address=<addr>&network=eth[&endpoint=/messages]` returns the automatic decisions recorded for a query address, newest first: `[{created_at, endpoint, code, scope, rule, reason, details}]` with `details` parsed. `endpoint` filters to one endpoint's decisions. Read-only. Missing `address` answers 400; an address with no decisions or a database not created yet answers `[]`; an unreadable database answers 503 with `Investigation database could not be read`.

The internal test routes (`/recreatedb`, `/internal-test-1`, `/internal-test-2`) answer 404 unless you opt in with `TRACKENN_DEV_ROUTES=1`:

``` shell
TRACKENN_DEV_ROUTES=1 python3 app.py
```

<h3 id="tests">Tests</h3>

``` shell
pip install -r requirements-dev.txt
python -m pytest
```

And, finally, [browse](#browse) it.

<h3 id="browser">Browse</h3>

Open the browser in this [url](http://127.0.0.1:4200)

# API Keys

Before start to use this software, you must get a single API key from [etherscan](https://etherscan.io/myapikey). This key is used for both Ethereum and BNB Smart Chain through [Etherscan API V2](https://docs.etherscan.io/v2-migration) — BscScan's own V1 API was shut down. Note: the free Etherscan plan may not cover every chain, so BNB Smart Chain access can require a paid plan. Copy the template and change the 'XXX' for your API key (`config.yaml` is ignored by git, never commit it):

``` shell
cp config.example.yaml config.yaml
```

```
ethscan: XXX
bscscan: XXX  # optional/legacy fallback, only used if ethscan is empty
level: INFO
dbname: default.db
```

A relative `dbname` is created in the working directory. Set `TRACKENN_DB_DIR` to keep the databases in another folder (Docker uses `/app/db`).

The front can also set the explorer keys through `POST /config/keys` (`{"ethscan": "...", "bscscan": "..."}`), which writes them to `config.yaml`; `GET /config/keys` only reports whether each key is set, never its value. Under Docker `config.yaml` is mounted read-only, so `POST /config/keys` answers 409 and changes nothing: edit `config.yaml` on the host and restart. A missing or malformed `config.yaml` answers 500 and is never overwritten.

Upgrading an existing clone: `config.yaml` is no longer tracked, but git still silently overwrites an *ignored* file when it is tracked on the branch you are moving to (old history, stashes). This can happen on `git pull` **and** on `git switch`/`git checkout <branch>`. Always back it up first and restore it afterwards:

``` shell
cp config.yaml config.yaml.bkp && git checkout -- config.yaml && git pull && cp config.yaml.bkp config.yaml
cp config.yaml config.yaml.bkp && git switch main && cp config.yaml.bkp config.yaml
```

Always run the project through `just up` (never `python app.py` or an old native run) — it owns `data-db`/`config.yaml` as bind mounts with the right permissions and fails with a clear message if `config.yaml` is missing; running some other way first can leave root-owned directories that later break `just up`.

<h1 id="sponsor">Support the project</h1>
Whether you use this project, have learned something from it, or just like it, please consider supporting it by buying me a coffee, so I can dedicate more time on open-source projects like this.


<div align="center" style="margin-top: 30px;">
<a href="https://www.buymeacoffee.com/kennbro" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" height="80" ></a>
</div>

# Disclaimer

Anyone who contributes or contributed to the project, including me, is not responsible for the use of the tool (Neither the legal use nor the illegal use, nor the "other" use).

Keep in mind that this software was initially written for educational purposes (to educate ourselves), and now the goal is to collaborate with the community making quality free software, and while the quality is not excellent (sometimes not even good) we strive to pursue excellence.

Please load the necessary APIs before remembering my mother. You can create an issue, contact us by e-mail or by any of the RRSS, with love, but keep in mind that my mother is neither the creator nor Contribute to the project.

We do not refund your money if you are not satisfied.
I hope you enjoy using the tool as much as we enjoy doing it. The effort was and is enormous (Time, knowledge, coding, tests, reviews, etc.) but we would do it again.
Do not use the tool if you cannot read the instructions and / or this disclaimer clearly.

By the way, for those who insist on remembering my mother, she died many years ago but I love her as if she were right here.

