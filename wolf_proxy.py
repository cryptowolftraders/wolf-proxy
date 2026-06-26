"""
🐺 Wolf Proxy — cryptowolfhp.com için veri proxy'si
---------------------------------------------------
Amaç: Tarayıcı (Türkiye) doğrudan Binance Futures'a ulaşamıyor.
Bu sunucu (Railway, yurt dışı) isteği alır, Binance/Bybit/Hyperliquid'den
çeker ve CORS başlıklarıyla tarayıcıya döner. Böylece hiç kimse VPN'e
ihtiyaç duymadan site çalışır.

Yönlendirme (path önekine göre üst kaynak):
  /fapi...      -> https://fapi.binance.com/fapi...        (Binance Futures)
  /futures...   -> https://fapi.binance.com/futures...     (Binance OI/LS/taker verisi)
  /dapi...      -> https://dapi.binance.com/dapi...         (Binance COIN-M)
  /api...       -> https://api.binance.com/api...           (Binance Spot)
  /sapi...      -> https://api.binance.com/sapi...
  /bybit/...    -> https://api.bybit.com/...                (Alpha Predator)
  /hl/...       -> https://api.hyperliquid.xyz/...          (Whale Scanner, POST)
  /coingecko/.. -> https://api.coingecko.com/...
"""

import os
from flask import Flask, request, Response
import requests

app = Flask(__name__)

# (önek, üst kaynak, prefix'i kırp mı?)  — sıra önemli, spesifik olanlar üstte
ROUTES = [
    ("/bybit/",     "https://api.bybit.com",        True),
    ("/hl/",        "https://api.hyperliquid.xyz",   True),
    ("/coingecko/", "https://api.coingecko.com",     True),
    ("/fapi",       "https://fapi.binance.com",      False),
    ("/futures",    "https://fapi.binance.com",      False),
    ("/dapi",       "https://dapi.binance.com",      False),
    ("/api",        "https://api.binance.com",       False),
    ("/sapi",       "https://api.binance.com",       False),
]

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
}

TIMEOUT = 20


@app.route("/", methods=["GET"])
def root():
    return Response('{"status":"wolf-proxy ok"}',
                    mimetype="application/json", headers=CORS)


@app.route("/<path:path>", methods=["GET", "POST", "OPTIONS"])
def proxy(path):
    # CORS preflight
    if request.method == "OPTIONS":
        return Response("", status=204, headers=CORS)

    full = "/" + path
    target = None
    for prefix, base, strip in ROUTES:
        if full.startswith(prefix):
            tail = full[len(prefix):] if strip else full
            if strip and not tail.startswith("/"):
                tail = "/" + tail
            target = base + tail
            break

    if target is None:
        return Response('{"error":"path not allowed"}', status=403,
                        mimetype="application/json", headers=CORS)

    qs = request.query_string.decode()
    if qs:
        target += "?" + qs

    try:
        if request.method == "POST":
            r = requests.post(
                target,
                data=request.get_data(),
                headers={
                    "Content-Type": request.headers.get("Content-Type", "application/json"),
                    "User-Agent": "wolf-proxy",
                },
                timeout=TIMEOUT,
            )
        else:
            r = requests.get(target, headers={"User-Agent": "wolf-proxy"}, timeout=TIMEOUT)

        out = dict(CORS)
        out["Content-Type"] = r.headers.get("Content-Type", "application/json")
        return Response(r.content, status=r.status_code, headers=out)

    except Exception as e:
        err = str(e).replace('"', "'")
        return Response('{"error":"proxy_fetch_failed","detail":"%s"}' % err,
                        status=502, mimetype="application/json", headers=CORS)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
