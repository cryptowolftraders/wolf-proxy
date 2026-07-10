"""
🐺 Wolf Proxy v2 — cryptowolfhp.com için veri proxy'si (CACHE'Lİ)
-----------------------------------------------------------------
Amaç: Tarayıcı (Türkiye) doğrudan Binance Futures'a ulaşamıyor.
Bu sunucu (Railway, yurt dışı) isteği alır, Binance/Bybit/Hyperliquid'den
çeker ve CORS başlıklarıyla tarayıcıya döner.

v2 YENİLİĞİ — BELLEK CACHE:
Aynı veri (örn. BTCUSDT klines) kısa süre içinde 30 kişi tarafından
istense bile Binance'e SADECE 1 kez gidilir; gerisi bellekten döner.
Bu, IP başına istek sayısını yüzlerce kat düşürür ve Binance ban'ını
(rate-limit) engeller. Grup yükü altında ayakta kalmanın anahtarı budur.

Yönlendirme:
  /fapi... -> fapi.binance.com   /futures... -> fapi.binance.com
  /dapi... -> dapi.binance.com   /api... /sapi... -> api.binance.com
  /bybit/... -> api.bybit.com    /hl/... -> api.hyperliquid.xyz (POST)
  /coingecko/.. -> api.coingecko.com
"""

import os
import time
import threading
from flask import Flask, request, Response
import requests

app = Flask(__name__)

ROUTES = [
    ("/bybit/",     "https://api.bybit.com",        True),
    ("/hl/",        "https://api.hyperliquid.xyz",   True),
    ("/coingecko/", "https://api.coingecko.com",     True),
    ("/mexc/",      "https://contract.mexc.com",     True),
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

# CACHE AYARLARI: hedef URL icinde gecen anahtara gore saniye cinsinden sure.
# Ustten alta ilk eslesen kullanilir; hicbiri eslesmezse DEFAULT_TTL.
TTL_RULES = [
    ("ping",              0),     # cache yok (saglik testi)
    ("exchangeInfo",   3600),     # coin listesi nadiren degisir -> 1 saat
    ("klines",           20),     # mumlar -> 20 sn
    ("ticker/24hr",       8),
    ("ticker/price",      6),
    ("premiumIndex",     12),
    ("fundingRate",      30),
    ("openInterest",     15),
    ("longShort",        15),     # globalLongShort / topLongShort
    ("takerlongshort",   15),
    ("coingecko",        30),
]
DEFAULT_TTL = 8
MAX_CACHE_ENTRIES = 4000        # bellek sismesin diye ust sinir

_cache = {}                     # url -> (expire_ts, status, content, ctype)
_lock = threading.Lock()


def ttl_for(target):
    for key, sec in TTL_RULES:
        if key.lower() in target.lower():
            return sec
    return DEFAULT_TTL


def cache_get(url):
    with _lock:
        item = _cache.get(url)
        if item and item[0] > time.time():
            return item[1], item[2], item[3]
        if item:
            _cache.pop(url, None)
    return None


def cache_put(url, status, content, ctype, ttl):
    if ttl <= 0:
        return
    with _lock:
        if len(_cache) >= MAX_CACHE_ENTRIES:
            for k in list(_cache.keys())[:500]:
                _cache.pop(k, None)
        _cache[url] = (time.time() + ttl, status, content, ctype)


@app.route("/", methods=["GET"])
def root():
    return Response('{"status":"wolf-proxy ok","cache":%d}' % len(_cache),
                    mimetype="application/json", headers=CORS)


@app.route("/<path:path>", methods=["GET", "POST", "OPTIONS"])
def proxy(path):
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

    # GET ise once cache'e bak
    if request.method == "GET":
        hit = cache_get(target)
        if hit is not None:
            status, content, ctype = hit
            out = dict(CORS)
            out["Content-Type"] = ctype
            out["X-Wolf-Cache"] = "HIT"
            return Response(content, status=status, headers=out)

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

        ctype = r.headers.get("Content-Type", "application/json")

        # Basarili GET cevabini cache'le (ban/hata cevaplarini cache'leme)
        if request.method == "GET" and r.status_code == 200:
            cache_put(target, r.status_code, r.content, ctype, ttl_for(target))

        out = dict(CORS)
        out["Content-Type"] = ctype
        out["X-Wolf-Cache"] = "MISS"
        return Response(r.content, status=r.status_code, headers=out)

    except Exception as e:
        err = str(e).replace('"', "'")
        return Response('{"error":"proxy_fetch_failed","detail":"%s"}' % err,
                        status=502, mimetype="application/json", headers=CORS)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
