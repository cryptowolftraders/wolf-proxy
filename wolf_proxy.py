"""
🐺 Wolf Proxy v3 — cryptowolfhp.com için veri proxy'si (CACHE'Lİ + HIZ SINIRLI)
-----------------------------------------------------------------
Amaç: Tarayıcı (Türkiye) doğrudan Binance Futures'a ulaşamıyor.
Bu sunucu (Railway, yurt dışı) isteği alır, Binance/Bybit/Hyperliquid'den
çeker ve CORS başlıklarıyla tarayıcıya döner.

v2 — BELLEK CACHE:
Aynı veri (örn. BTCUSDT klines) kısa süre içinde 30 kişi tarafından
istense bile Binance'e SADECE 1 kez gidilir; gerisi bellekten döner.

v3 — HIZ SINIRLAYICI (RATE LIMITER):
Site birden fazla tarayıcı sayfasında (beta-radar, ob-scanner,
candle-range, harmonic-radar, vb.) aynı anda YÜZLERCE FARKLI sembol
için veri çekebiliyor. Bunların her biri cache'te ayrı bir anahtar
olduğundan cache tek başına Binance'e giden toplam isteği sınırlayamaz.
Bu yüzden her upstream (fapi.binance.com, api.bybit.com, ...) için
ayrı bir token-bucket hız sınırlayıcı eklendi: gerçek istek Binance'e
gitmeden hemen önce sıraya girer, güvenli hızın üstüne çıkılmaz.
Bu da Binance'in IP ban'ını (-1003 / 418 / 429) engeller.

Not: Railway'de gunicorn birden fazla worker (process) ile çalışıyor;
her worker'ın kendi belleği (dolayısıyla kendi token bucket'ı) var.
Bu yüzden hedeflenen TOPLAM hız, worker sayısına bölünerek worker
başına düşen hız hesaplanıyor (WEB_CONCURRENCY / PROXY_WORKERS env
değişkeni ile ayarlanabilir, varsayılan 4).

Yönlendirme:
  /fapi...      -> fapi.binance.com   /futures...  -> fapi.binance.com
  /dapi...      -> dapi.binance.com   /api... /sapi... -> api.binance.com
  /bybit/...    -> api.bybit.com      /hl/...      -> api.hyperliquid.xyz (POST)
  /coingecko/.. -> api.coingecko.com
"""

import os
import time
import threading
from flask import Flask, request, Response
import requests

app = Flask(__name__)

ROUTES = [
    ("/bybit/",     "https://api.bybit.com",       True),
    ("/hl/",        "https://api.hyperliquid.xyz", True),
    ("/coingecko/", "https://api.coingecko.com",   True),
    ("/mexc/",      "https://contract.mexc.com",   True),
    ("/wolfdata/",  "https://contract.mexc.com",   True),
    ("/fapi",       "https://fapi.binance.com",    False),
    ("/futures",    "https://fapi.binance.com",    False),
    ("/dapi",       "https://dapi.binance.com",    False),
    ("/api",        "https://api.binance.com",     False),
    ("/sapi",       "https://api.binance.com",     False),
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
    ("ping", 0),             # cache yok (saglik testi)
    ("exchangeInfo", 3600),  # coin listesi nadiren degisir -> 1 saat
    ("klines", 20),          # mumlar -> 20 sn
    ("ticker/24hr", 8),
    ("ticker/price", 6),
    ("premiumIndex", 12),
    ("fundingRate", 30),
    ("openInterest", 15),
    ("longShort", 15),       # globalLongShort / topLongShort
    ("takerlongshort", 15),
    ("coingecko", 30),
]
DEFAULT_TTL = 8
MAX_CACHE_ENTRIES = 4000  # bellek sismesin diye ust sinir

_cache = {}  # url -> (expire_ts, status, content, ctype)
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

# ---------------------------------------------------------------------
# RATE LIMITER (token bucket) — Binance/Bybit/vb'ye giden GERCEK istek
# hizini upstream basina sabit bir tavanin altinda tutar.
# ---------------------------------------------------------------------

WORKERS = int(os.environ.get("WEB_CONCURRENCY", os.environ.get("PROXY_WORKERS", 4)))

# Upstream basina TOPLAM (tum worker'lar toplaminda) hedeflenen guvenli
# hiz (istek/sn) ve burst kapasitesi. Binance futures limiti ~2400
# agirlik/dk (~40 istek/sn) oldugu icin fapi icin 18/sn payi guvenli.
_UPSTREAM_LIMITS = {
    "https://fapi.binance.com":    (18, 30),
    "https://dapi.binance.com":    (10, 20),
    "https://api.binance.com":     (10, 20),
    "https://api.bybit.com":       (10, 20),
    "https://api.hyperliquid.xyz": (10, 20),
    "https://api.coingecko.com":   (4, 8),
    "https://contract.mexc.com":   (8, 16),
}

class TokenBucket:
    """Basit thread-safe token bucket. acquire() gerekirse bekler (sleep)
    ve upstream'e giden hizi sabit bir tavanin altinda tutar."""

    def __init__(self, rate_per_sec, capacity):
        self.rate = max(rate_per_sec, 0.1)
        self.capacity = max(capacity, 1)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(min(wait, 2))

_limiters = {}
for _base, (_rate, _cap) in _UPSTREAM_LIMITS.items():
    # Hedeflenen toplam hizi worker sayisina bolerek worker-basi limit olustur.
    _limiters[_base] = TokenBucket(_rate / WORKERS, max(1, _cap / WORKERS))

def limiter_for(base):
    return _limiters.get(base)

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
    matched_base = None
    for prefix, base, strip in ROUTES:
        if full.startswith(prefix):
            tail = full[len(prefix):] if strip else full
            if strip and not tail.startswith("/"):
                tail = "/" + tail
            target = base + tail
            matched_base = base
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

    # Cache'te yoksa, gercek istegi upstream'e gondermeden once hiz
    # sinirlayiciyi bekle. Boylece ayni anda onlarca farkli sembol
    # istense bile disariya giden gercek istek hizi guvenli sinirin
    # altinda kalir ve Binance ban atmaz.
    lim = limiter_for(matched_base)
    if lim:
        lim.acquire()

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
