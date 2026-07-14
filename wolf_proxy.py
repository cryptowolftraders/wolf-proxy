"""
Wolf Proxy v4 -- cryptowolfhp.com icin veri proxy'si (CACHE'Lİ + HIZ SINIRLI + WS CANLI KLINES)
-----------------------------------------------------------------
Amac: Tarayici (Turkiye) dogrudan Binance Futures'a ulasamiyor.
Bu sunucu (Railway, yurt disi) istegi alir, Binance/Bybit/Hyperliquid'den
ceker ve CORS basliklariyla tarayiciya doner.

v2 -- BELLEK CACHE:
Ayni veri (orn. BTCUSDT klines) kisa sure icinde 30 kisi tarafindan
istense bile Binance'e SADECE 1 kez gidilir; gerisi bellekten doner.

v3 -- HIZ SINIRLAYICI (RATE LIMITER):
Site birden fazla tarayici sayfasinda (beta-radar, ob-scanner,
candle-range, harmonic-radar, vb.) ayni anda YUZLERCE FARKLI sembol
icin veri cekebiliyor. Bunlarin her biri cache'te ayri bir anahtar
oldugundan cache tek basina Binance'e giden toplam istegi sinirlayamaz.
Bu yuzden her upstream (fapi.binance.com, api.bybit.com, ...) icin
ayri bir token-bucket hiz sinirlayici eklendi: gercek istek Binance'e
gitmeden hemen once siraya girer, guvenli hizin ustune cikilmaz.
Bu da Binance'in IP ban'ini (-1003 / 418 / 429) engeller.

v4 -- WEBSOCKET CANLI KLINES:
Loglarda en cok istegi olusturan uc nokta /fapi/.../klines idi (mum
verisi). Bunun icin artik Binance Futures websocket akisina (fstream.
binance.com) baglaniyoruz. Bir sembol/interval ilk kez istendiginde
once REST'ten cekilip cache'lenir ve ayni zamanda o stream'e abone
olunur; sonraki istekler -- veri "canli" (son WS_STALE_AFTER saniye
icinde guncellenmis) oldugu surece -- dogrudan bellekten, Binance'e
HIC REST istegi gitmeden cevaplanir. Boylece hem daha hizli hem daha
guncel veri donulur. Diger tum uc noktalar (ticker, funding, vb.)
degismedi; cache + rate limiter aynen calismaya devam ediyor.

Not: Railway'de gunicorn birden fazla worker (process) ile calisiyor;
her worker'in kendi bellegi (dolayisiyla kendi token bucket'i ve kendi
websocket baglantisi) var. Bu yuzden hedeflenen TOPLAM hiz, worker
sayisina bolunerek worker basina dusen hiz hesaplaniyor (WEB_CONCURRENCY
/ PROXY_WORKERS env degiskeni ile ayarlanabilir, varsayilan 4).

Yonlendirme:
/fapi... -> fapi.binance.com   /futures... -> fapi.binance.com
/dapi... -> dapi.binance.com   /api... /sapi... -> api.binance.com
/bybit/... -> api.bybit.com    /hl/... -> api.hyperliquid.xyz (POST)
/coingecko/.. -> api.coingecko.com
"""

import os
import time
import json
import threading
from collections import deque, OrderedDict
from flask import Flask, request, Response
import requests
import websocket

app = Flask(__name__)

ROUTES = [
    ("/bybit/", "https://api.bybit.com", True),
    ("/hl/", "https://api.hyperliquid.xyz", True),
    ("/coingecko/", "https://api.coingecko.com", True),
    ("/mexc/", "https://contract.mexc.com", True),
    ("/wolfdata/", "https://contract.mexc.com", True),
    ("/fapi", "https://fapi.binance.com", False),
    ("/futures", "https://fapi.binance.com", False),
    ("/dapi", "https://dapi.binance.com", False),
    ("/api", "https://api.binance.com", False),
    ("/sapi", "https://api.binance.com", False),
]

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
}

TIMEOUT = 20

TTL_RULES = [
    ("ping", 0),
    ("exchangeInfo", 3600),
    ("klines", 20),
    ("ticker/24hr", 8),
    ("ticker/price", 6),
    ("premiumIndex", 12),
    ("fundingRate", 30),
    ("openInterest", 15),
    ("longShort", 15),
    ("takerlongshort", 15),
    ("coingecko", 30),
]
DEFAULT_TTL = 8
MAX_CACHE_ENTRIES = 4000

_cache = {}
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

WORKERS = int(os.environ.get("WEB_CONCURRENCY", os.environ.get("PROXY_WORKERS", 4)))

_UPSTREAM_LIMITS = {
    "https://fapi.binance.com": (18, 30),
    "https://dapi.binance.com": (10, 20),
    "https://api.binance.com": (10, 20),
    "https://api.bybit.com": (10, 20),
    "https://api.hyperliquid.xyz": (10, 20),
    "https://api.coingecko.com": (4, 8),
    "https://contract.mexc.com": (8, 16),
}

class TokenBucket:
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
    _limiters[_base] = TokenBucket(_rate / WORKERS, max(1, _cap / WORKERS))

def limiter_for(base):
    return _limiters.get(base)

KLINE_WS_URL = "wss://fstream.binance.com/stream"
MAX_WS_STREAMS = 60
KLINE_BUFFER_LEN = 500
WS_STALE_AFTER = 30

class KlineStreamManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._buffers = OrderedDict()
        self._last_update = {}
        self._ws = None
        self._connected = threading.Event()
        threading.Thread(target=self._run_forever, daemon=True).start()

    def _run_forever(self):
        while True:
            try:
                self._connect_and_run()
            except Exception:
                pass
            self._connected.clear()
            time.sleep(3)

    def _connect_and_run(self):
        def on_open(ws):
            self._connected.set()
            with self._lock:
                streams = list(self._buffers.keys())
            if streams:
                self._send_sub(ws, streams, "SUBSCRIBE")

        def on_message(ws, message):
            try:
                msg = json.loads(message)
            except Exception:
                return
            data = msg.get("data")
            stream = msg.get("stream")
            if not data or not stream or data.get("e") != "kline":
                return
            k = data["k"]
            row = [
                k["t"], k["o"], k["h"], k["l"], k["c"], k["v"],
                k["T"], k["q"], k["n"], k["V"], k["Q"], "0",
            ]
            with self._lock:
                buf = self._buffers.get(stream)
                if buf is None:
                    return
                if buf and buf[-1][0] == row[0]:
                    buf[-1] = row
                else:
                    buf.append(row)
                self._last_update[stream] = time.monotonic()

        def on_error(ws, error):
            pass

        def on_close(ws, *a):
            self._connected.clear()

        self._ws = websocket.WebSocketApp(
            KLINE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        self._ws.run_forever(ping_interval=180, ping_timeout=10)

    def _send_sub(self, ws, streams, method):
        try:
            ws.send(json.dumps({"method": method, "params": streams, "id": int(time.time())}))
        except Exception:
            pass

    def ensure_subscribed(self, symbol, interval):
        stream = "%s@kline_%s" % (symbol.lower(), interval)
        is_new = False
        evicted = None
        with self._lock:
            if stream not in self._buffers:
                is_new = True
                if len(self._buffers) >= MAX_WS_STREAMS:
                    evicted, _ = self._buffers.popitem(last=False)
                    self._last_update.pop(evicted, None)
                self._buffers[stream] = deque(maxlen=KLINE_BUFFER_LEN)
        if self._connected.is_set() and self._ws:
            if evicted:
                self._send_sub(self._ws, [evicted], "UNSUBSCRIBE")
            if is_new:
                self._send_sub(self._ws, [stream], "SUBSCRIBE")
        return stream

    def seed(self, symbol, interval, rows):
        stream = "%s@kline_%s" % (symbol.lower(), interval)
        with self._lock:
            buf = self._buffers.get(stream)
            if buf is not None and not buf:
                for row in rows:
                    buf.append(row)

    def get_live(self, symbol, interval, limit):
        stream = "%s@kline_%s" % (symbol.lower(), interval)
        with self._lock:
            last = self._last_update.get(stream)
            if last is None or (time.monotonic() - last) > WS_STALE_AFTER:
                return None
            buf = self._buffers.get(stream)
            if not buf or len(buf) < min(limit, 2):
                return None
            rows = list(buf)[-limit:]
        return rows

kline_manager = KlineStreamManager()

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

    is_klines = (
        request.method == "GET"
        and matched_base == "https://fapi.binance.com"
        and "/klines" in full
    )
    kl_symbol = request.args.get("symbol") if is_klines else None
    kl_interval = request.args.get("interval") if is_klines else None
    if is_klines and kl_symbol and kl_interval:
        try:
            kl_limit = int(request.args.get("limit", "500"))
        except ValueError:
            kl_limit = 500
        kline_manager.ensure_subscribed(kl_symbol, kl_interval)
        live_rows = kline_manager.get_live(kl_symbol, kl_interval, kl_limit)
        if live_rows is not None:
            out = dict(CORS)
            out["Content-Type"] = "application/json"
            out["X-Wolf-Cache"] = "WS-LIVE"
            return Response(json.dumps(live_rows), status=200, headers=out)

    if request.method == "GET":
        hit = cache_get(target)
        if hit is not None:
            status, content, ctype = hit
            out = dict(CORS)
            out["Content-Type"] = ctype
            out["X-Wolf-Cache"] = "HIT"
            return Response(content, status=status, headers=out)

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

        if request.method == "GET" and r.status_code == 200:
            cache_put(target, r.status_code, r.content, ctype, ttl_for(target))
            if is_klines and kl_symbol and kl_interval:
                try:
                    rows = json.loads(r.content)
                    kline_manager.seed(kl_symbol, kl_interval, rows)
                except Exception:
                    pass

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
