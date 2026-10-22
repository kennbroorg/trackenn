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

