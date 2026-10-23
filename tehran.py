"""
Tehran client + Railway WebSocket relay.

Run locally in Tehran:
    py tehran.py

Run on Railway as the relay service:
    ROLE=server

Commands in Tehran:
    help
    ls [path]
    pwd
    info [path]
    stat <path>
    get <remote-file> [local-name]
    cat <remote-text-file>
    search <folder> <name-fragment>
    ping
    reconnect
    quit
    <anything else>   → run as shell command on the agent
    !<command>        → force shell (useful when name collides with built-in)

Security notes:
- The built-in file actions no longer restrict paths; both PCs are fully exposed
  to an authenticated peer, so treat PAIR_CODE as a root password.
- The shell action (`<anything else>` or `!cmd`) runs with the user's full
  privileges.
- Use a long random PAIR_CODE and keep it identical on Railway and both PCs.
"""
import asyncio
import base64
import json
import collections
import os
import random
import shlex
import time
import uuid
from pathlib import Path

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

PAIR_CODE = os.getenv(
    "PAIR_CODE",
    "ChangeThis-ToYourOwn-PrivateCode-9381",
)
DEFAULT_PAIR_CODE = "ChangeThis-ToYourOwn-PrivateCode-9381"
RELAY_URL = os.getenv(
    "RELAY_URL",
    "wss://vpn.mriranfa.site/ws",
)
ROLE = os.getenv("ROLE", "client").lower()

MAX_FILE_BYTES = 200 * 1024 * 1024
MAX_MESSAGE_BYTES = MAX_FILE_BYTES * 2 + 1024 * 1024
REQUEST_TIMEOUT = 45
CONNECT_TIMEOUT = 20
PING_INTERVAL = 30
PING_TIMEOUT = 90
MAX_SEARCH_RESULTS = 200
# get درخواست بزرگ را با base64 منتقل می‌کند؛ تا ۱۵ دقیقه صبر می‌کنیم
GET_TIMEOUT_SECONDS = 900
# cat فقط برای فایل متنی کوچک است — timeout کوتاه کافی است
CAT_TIMEOUT_SECONDS = 120

KNOWN_COMMANDS = {
    "help", "quit", "exit", "q", "reconnect",
    "ls", "pwd", "info", "stat", "get", "cat", "search", "ping",
    "screen", "camera", "mic", "av", "media",
    "screencam", "screenwithsound",
}


def json_dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def split_command(line):
    """Parse a command line respecting quotes; keep Windows backslashes intact.

    shlex.split with posix=False:
      - keeps "D:\\POCO X3" as a single token, quotes stripped
      - does NOT eat backslashes, so D:\\ stays D:\\
    Falls back to plain split on malformed input (e.g. unbalanced quotes).
    """
    try:
        raw = shlex.split(line, posix=False)
    except ValueError:
        raw = line.split()
    return [t.strip('"\'') for t in raw]


# ---------------- Railway relay mode ----------------
if ROLE == "server":
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import JSONResponse
    import uvicorn

    app = FastAPI(title="Two-file remote relay")
    peers = {"agent": None, "client": None}
    state_lock = asyncio.Lock()

    async def send_to(ws, message):
        await ws.send_text(json_dumps(message))

    @app.get("/")
    async def home():
        return {
            "status": "relay online",
            "connected": {role: peers[role] is not None for role in peers},
            "usage": "Authenticated WebSocket relay at /ws",
        }

    @app.get("/health")
    async def health():
        return {"ok": True, "connected": {role: peers[role] is not None for role in peers}}

    @app.websocket("/ws")
    async def relay(ws: WebSocket):
        await ws.accept()
        role = None
        authenticated = False
        try:
            hello_raw = await asyncio.wait_for(ws.receive_text(), timeout=15)
            hello = json.loads(hello_raw)
            role = hello.get("role")
            if role not in peers or hello.get("code") != PAIR_CODE:
                await ws.close(code=1008, reason="Invalid pairing code or role")
                return
            authenticated = True

            async with state_lock:
                old = peers[role]
                peers[role] = ws
            if old is not None and old is not ws:
                try:
                    await old.close(code=1012, reason="Replaced by a newer connection")
                except Exception:
                    pass

            print(f"[relay] authenticated role={role}")
            await send_to(ws, {"type": "ready", "role": role, "ok": True})

            while True:
                msg = await ws.receive_text()
                try:
                    parsed = json.loads(msg)
                except json.JSONDecodeError:
                    await send_to(ws, {"type": "relay_error", "ok": False, "error": "Invalid JSON"})
                    continue

                other_role = "client" if role == "agent" else "agent"
                async with state_lock:
                    other = peers.get(other_role)

                if other is not None:
                    try:
                        await other.send_text(msg)
                    except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
                        async with state_lock:
                            if peers.get(other_role) is other:
                                peers[other_role] = None
                        if role == "client":
                            await send_to(ws, {
                                "id": parsed.get("id"),
                                "ok": False,
                                "error": "Agent disconnected while forwarding the request.",
                            })
                elif role == "client":
                    await send_to(ws, {
                        "id": parsed.get("id"),
                        "ok": False,
                        "error": "لپ‌تاپ کرمانشاه آنلاین نیست.",
                    })

        except asyncio.TimeoutError:
            try:
                await ws.close(code=1008, reason="Authentication timed out")
            except Exception:
                pass
        except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
            pass
        except Exception as exc:
            print(f"[relay] connection error role={role}: {type(exc).__name__}: {exc}")
        finally:
            if authenticated and role:
                async with state_lock:
                    if peers.get(role) is ws:
                        peers[role] = None
                print(f"[relay] disconnected role={role}")

    if __name__ == "__main__":
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=int(os.getenv("PORT", "8080")),
            ws_max_size=MAX_MESSAGE_BYTES,   # critical for large file transfers
        )
    raise SystemExit


# ---------------- Tehran client mode ----------------
class RemoteClient:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.ws = None
        self.pending = {}
        self.reader_task = None
        self.write_lock = asyncio.Lock()
        self.connected = asyncio.Event()
        self.closed = False
        # Single-slot queue means the UI always consumes the newest frame rather
        # than accumulating seconds of stale desktop images on a slow connection.
        self.screen_frames = asyncio.Queue(maxsize=1)
        self.screen_events = asyncio.Queue(maxsize=8)
        self.screen_dropped_frames = 0
        self.screen_received_bytes = 0
        self.camera_frames = asyncio.Queue(maxsize=1)
        self.camera_events = asyncio.Queue(maxsize=8)
        self.mic_chunks = asyncio.Queue(maxsize=8)
        self.sysaudio_chunks = asyncio.Queue(maxsize=8)   # NEW: system/desktop audio
        self.media_bytes = 0

    async def connect(self):
        self.ws = await websockets.connect(
            self.url,
            max_size=MAX_MESSAGE_BYTES,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            close_timeout=10,
            open_timeout=CONNECT_TIMEOUT,
        )
        await self.ws.send(json_dumps({"role": "client", "code": PAIR_CODE}))
        # Wait briefly for the relay "ready" message so a wrong PAIR_CODE
        # surfaces immediately instead of after the first request.
        try:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=10)
            ready = json.loads(raw)
            if isinstance(ready, dict) and ready.get("type") == "ready" and not ready.get("ok", True):
                raise ConnectionError(ready.get("error", "Authentication failed"))
        except asyncio.TimeoutError:
            # Older relays don't send "ready"; fall through.
            pass
        except (ConnectionClosed, WebSocketException) as exc:
            raise ConnectionError(f"رله اتصال را رد کرد: {exc}") from exc
        except json.JSONDecodeError:
            pass
        self.connected.set()
        self.reader_task = asyncio.create_task(self._reader())
        print(f"به رله وصل شدی: {self.url}")
        print("توجه: این پیام اتصال WebSocket را تأیید می‌کند، نه آنلاین بودن عامل کرمانشاه.")

    async def _reader(self):
        try:
            async for raw in self.ws:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                message_type = data.get("type")
                if message_type == "camera_frame":
                    packet = {"data": data, "wire_bytes": len(raw.encode("utf-8")), "received_at": time.monotonic()}
                    self.media_bytes += packet["wire_bytes"]
                    if self.camera_frames.full():
                        try: self.camera_frames.get_nowait()
                        except asyncio.QueueEmpty: pass
                    try: self.camera_frames.put_nowait(packet)
                    except asyncio.QueueFull: pass
                    continue
                if message_type == "mic_audio":
                    self.media_bytes += len(raw.encode("utf-8"))
                    try:
                        chunk = base64.b64decode(data.get("audio", ""), validate=True)
                        if self.mic_chunks.full(): self.mic_chunks.get_nowait()
                        self.mic_chunks.put_nowait(chunk)
                    except (ValueError, asyncio.QueueEmpty, asyncio.QueueFull): pass
                    continue
                # NEW: system audio chunks
                if message_type == "sysaudio_audio":
                    self.media_bytes += len(raw.encode("utf-8"))
                    try:
                        chunk = base64.b64decode(data.get("audio", ""), validate=True)
                        if self.sysaudio_chunks.full():
                            try: self.sysaudio_chunks.get_nowait()
                            except asyncio.QueueEmpty: pass
                        self.sysaudio_chunks.put_nowait(chunk)
                    except (ValueError, asyncio.QueueEmpty, asyncio.QueueFull):
                        pass
                    continue
                if message_type in ("camera_error", "camera_stopped", "mic_error", "mic_stopped"):
                    if self.camera_events.full():
                        try: self.camera_events.get_nowait()
                        except asyncio.QueueEmpty: pass
                    try: self.camera_events.put_nowait(data)
                    except asyncio.QueueFull: pass
                    if message_type.startswith("mic_"):
                        if self.camera_events.full():
                            try: self.camera_events.get_nowait()
                            except asyncio.QueueEmpty: pass
                        try: self.camera_events.put_nowait(data)
                        except asyncio.QueueFull: pass
                    continue
                if message_type in ("sysaudio_error", "sysaudio_stopped"):
                    if self.screen_events.full():
                        try: self.screen_events.get_nowait()
                        except asyncio.QueueEmpty: pass
                    try: self.screen_events.put_nowait(data)
                    except asyncio.QueueFull: pass
                    continue
                if message_type == "screen_frame":
                    wire_bytes = len(raw.encode("utf-8"))
                    self.screen_received_bytes += wire_bytes
                    packet = {"data": data, "wire_bytes": wire_bytes, "received_at": time.monotonic()}
                    if self.screen_frames.full():
                        try:
                            self.screen_frames.get_nowait()
                            self.screen_dropped_frames += 1
                        except asyncio.QueueEmpty:
                            pass
                    try:
                        self.screen_frames.put_nowait(packet)
                    except asyncio.QueueFull:
                        self.screen_dropped_frames += 1
                    continue
                if message_type in ("screen_error", "screen_stopped"):
                    if self.screen_events.full():
                        try:
                            self.screen_events.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    try:
                        self.screen_events.put_nowait(data)
                    except asyncio.QueueFull:
                        pass
                    continue
                req_id = data.get("id")
                if req_id and req_id in self.pending:
                    future = self.pending.pop(req_id)
                    if not future.done():
                        future.set_result(data)
                elif message_type == "relay_error":
                    print("[خطای رله]", data.get("error", "خطای نامشخص"))
        except (ConnectionClosed, WebSocketException, OSError) as exc:
            print(f"\nاتصال قطع شد: {exc}")
        finally:
            self.connected.clear()
            for fut in list(self.pending.values()):
                if not fut.done():
                    fut.set_exception(ConnectionError("اتصال قطع شد؛ درخواست ناموفق بود."))
            self.pending.clear()

    async def request(self, action, path="", wait_time=None, **extra):
        if not self.connected.is_set() or self.ws is None:
            raise ConnectionError("به رله وصل نیستی؛ دستور reconnect را اجرا کن.")
        req_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[req_id] = future
        message = {"id": req_id, "action": action, "path": path, **extra}
        effective_wait = wait_time if wait_time is not None else REQUEST_TIMEOUT
        try:
            async with self.write_lock:
                await self.ws.send(json_dumps(message))
            return await asyncio.wait_for(future, timeout=effective_wait)
        except asyncio.TimeoutError as exc:
            self.pending.pop(req_id, None)
            raise TimeoutError(
                f"سرور در {effective_wait} ثانیه پاسخ نداد؛ وضعیت اتصال را بررسی کن."
            ) from exc
        except Exception:
            self.pending.pop(req_id, None)
            raise

    async def close(self):
        self.closed = True
        self.connected.clear()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except (asyncio.CancelledError, Exception):
                pass


def show_help():
    print("""
دستورهای موجود:
  help                         نمایش راهنما
  ls [مسیر]                    فهرست فایل‌ها (پیش‌فرض: پوشهٔ کاربر)
  pwd                          مسیر ریشهٔ قابل دسترسی روی لپ‌تاپ مقصد
  info [مسیر]                  اطلاعات کلی پوشه/فایل
  stat <مسیر>                  جزئیات یک فایل یا پوشه
  get <مسیر> [نام محلی]        دانلود فایل به پوشهٔ فعلی تهران (تا ۲۰۰ مگابایت)
  cat <مسیر فایل متنی>         نمایش فایل متنی کوچک
  search <پوشه> <عبارت>        جست‌وجوی نام فایل/پوشه (حداکثر ۲۰۰ نتیجه)
  ping                         تست رفت‌وبرگشت تا عامل کرمانشاه

  screen [480p|720p] [fps=5-60] [quality=20-90] [auto=on|off]
                               نمایش زندهٔ دسکتاپ مقصد
  screenwithsound [480p|720p] [fps=5-60] [quality=20-90] [auto=on|off]
                               اسکرین + صدای سیستم مقصد (WASAPI loopback)
  camera [480p|720p] [fps=1-30] [quality=20-90] [device=0]
                               نمایش زنده دوربین مقصد
  mic                          پخش صدای زنده میکروفون مقصد
  av | media                   دوربین + میکروفون همزمان
  screencam [480p|720p] [fps=X] [quality=X] [device=N] [sound=on]
                               اسکرین + دوربین همزمان (دوربین PiP گوشه، اختیاری با صدا)

  reconnect                    اتصال مجدد به رله
  quit                         خروج

دستور دلخواه (Shell):
  هر چیزی که در لیست بالا نباشد، به‌عنوان دستور shell روی لپ‌تاپ مقصد اجرا می‌شود.
  مثال:  git status
         pip install requests
         echo hello | wc -l

  برای اجبار به اجرا در shell حتی اگر با یک built-in تداخل داشته باشد،
  از پیشوند ! استفاده کن:
         !ls -la /tmp
         !cat /etc/hostname

نکته‌ها:
- مسیرهایی که فاصله دارند را داخل گیومه بگذار، مثلا:
    get "D:\\POCO X3\\Camera\\VID_20251030_224208.mp4"
    ls "D:\\POCO X3"
- get و بقیهٔ دستورهای فایلی دیگر محدود به HOME نیستند — هر مسیری مثل D:\\ باز است.
- سقف get: ۲۰۰ مگابایت. برای فایل‌های بزرگ‌تر از روش تکه‌تکه یا کپی به HOME استفاده کن.
- در پنجرهٔ screen: q توقف، p مکث/ادامه، +/- کیفیت، [ ] نرخ فریم، 1=480p و 2=720p، a=تنظیم خودکار.
- در پنجرهٔ screencam: q توقف، p مکث اسکرین، a قطع/وصل صدا (اگر sound=on باشد).
- دستورهای تعاملی مثل vim/top/less کار نمی‌کنند.
- exec محدود به HOME نیست و همان اختیارات کاربر را دارد.
- نصب لازم برای صدا: روی تهران sounddevice، روی مقصد soundcard + numpy.
""")


async def print_listing(client, path):
    result = await client.request("list", path or ".")
    if not result.get("ok"):
        print("خطا:", result.get("error", "نامشخص"))
        return
    items = result.get("items", [])
    for item in items:
        print(("[DIR] " if item.get("is_dir") else "      ") + item.get("name", ""))
    if result.get("truncated"):
        print("هشدار: فهرست به سقف تعداد آیتم‌ها رسیده؛ همهٔ موارد نمایش داده نشد.")


async def run_shell(client, command, agent_timeout=120):
    result = await client.request(
        "exec",
        command=command,
        wait_time=agent_timeout + 30,
        timeout=agent_timeout,
    )
    if not result.get("ok"):
        print("خطا:", result.get("error", "نامشخص"))
        return
    out = result.get("stdout") or ""
    err = result.get("stderr") or ""
    if out:
        print(out, end="" if out.endswith("\n") else "\n")
    if err:
        print("--- stderr ---")
        print(err, end="" if err.endswith("\n") else "\n")
    rc = result.get("returncode", 0)
    if rc:
        print(f"[exit={rc}]")
    if result.get("truncated"):
        print("[هشدار: خروجی بریده شد؛ برای فایل بزرگ از get استفاده کن.]")


async def run_screen(client, arguments=None, with_sound=False):
    """Start and display a live screen stream, keeping only the newest frame.
    If with_sound is True, also start system audio and play it locally."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        print("روی کامپیوتر تهران نصب کن: py -m pip install opencv-python numpy")
        return

    settings = {"resolution": "480p", "fps": 30, "quality": 55, "adaptive": True, "paused": False}
    for token in (arguments or []):
        token = token.strip().lower()
        if token in ("480p", "720p"):
            settings["resolution"] = token
        elif token.startswith("fps="):
            try:
                settings["fps"] = max(5, min(60, int(token.split("=", 1)[1])))
            except ValueError:
                print("FPS نامعتبر است؛ مقدار پیش‌فرض 30 استفاده می‌شود.")
        elif token.startswith("quality="):
            try:
                settings["quality"] = max(20, min(90, int(token.split("=", 1)[1])))
            except ValueError:
                print("کیفیت نامعتبر است؛ مقدار پیش‌فرض 55 استفاده می‌شود.")
        elif token in ("auto=off", "adaptive=off"):
            settings["adaptive"] = False
        elif token in ("auto=on", "adaptive=on"):
            settings["adaptive"] = True
        elif token in ("sound=on", "audio=on"):
            with_sound = True
        elif token in ("sound=off", "audio=off"):
            with_sound = False
        else:
            print(f"تنظیم ناشناخته نادیده گرفته شد: {token}")

    # Clear any frames/events left by a previous screen session.
    while not client.screen_frames.empty():
        try:
            client.screen_frames.get_nowait()
        except asyncio.QueueEmpty:
            break
    while not client.screen_events.empty():
        try:
            client.screen_events.get_nowait()
        except asyncio.QueueEmpty:
            break
    client.screen_dropped_frames = 0
    bytes_before = client.screen_received_bytes

    try:
        started = await client.request(
            "screen_start", wait_time=15,
            resolution=settings["resolution"], fps=settings["fps"], quality=settings["quality"],
        )
    except Exception as exc:
        print("شروع تصویر زنده ناموفق بود:", exc)
        return
    if not started.get("ok"):
        print("شروع تصویر زنده ناموفق بود:", started.get("error", "خطای نامشخص"))
        return

    # --- optional system audio ---
    audio_out = None
    if with_sound:
        try:
            import sounddevice as sd
        except ImportError:
            print("sounddevice نصب نیست؛ برای صدا: py -m pip install sounddevice")
            with_sound = False
        if with_sound:
            while not client.sysaudio_chunks.empty():
                try: client.sysaudio_chunks.get_nowait()
                except asyncio.QueueEmpty: break
            sysaudio_started = await client.request("sysaudio_start", wait_time=12)
            if not sysaudio_started.get("ok"):
                print("شروع صدای سیستم ناموفق بود:", sysaudio_started.get("error", "نامشخص"))
                with_sound = False
            else:
                try:
                    audio_out = sd.RawOutputStream(
                        samplerate=22050, channels=1, dtype="int16", blocksize=2205
                    )
                    audio_out.start()
                    print("صدای سیستم فعال شد.")
                except Exception as exc:
                    print("خطا در راه‌اندازی خروجی صدا:", exc)
                    audio_out = None
                    with_sound = False
                    try: await client.request("sysaudio_stop", wait_time=4)
                    except Exception: pass

    window = "Remote desktop — q:stop p:pause +/-:quality []:fps 1=480p 2=720p a:auto"
    print("تصویر زنده شروع شد.")
    print("کلیدها: q توقف | p مکث/ادامه | + و - کیفیت | ] و [ نرخ فریم | 1=480p | 2=720p | a=تنظیم خودکار")
    print("نکته: تأخیر روی تصویر از نصف RTT تخمین زده می‌شود و اندازه‌گیری دقیق یک‌طرفه نیست.")

    stop_probe = asyncio.Event()
    stats = {"rtt_ms": None, "last_ping_ok": None}

    async def ping_loop():
        while not stop_probe.is_set():
            begin = time.monotonic()
            try:
                result = await client.request("ping", ".", wait_time=6)
                if result.get("ok"):
                    stats["rtt_ms"] = (time.monotonic() - begin) * 1000
                    stats["last_ping_ok"] = time.monotonic()
            except Exception:
                stats["rtt_ms"] = None
            try:
                await asyncio.wait_for(stop_probe.wait(), timeout=2.5)
            except asyncio.TimeoutError:
                pass

    probe_task = asyncio.create_task(ping_loop())
    last_frame = None
    frame_times = collections.deque()
    byte_samples = collections.deque()
    session_bytes = 0
    shown = 0
    started_at = time.monotonic()
    last_adapt = started_at
    prev_drops = client.screen_dropped_frames
    stream_failed = False

    async def apply_settings(**changes):
        payload = {k: v for k, v in changes.items() if k in ("resolution", "fps", "quality", "paused")}
        if not payload:
            return
        try:
            response = await client.request("screen_config", wait_time=8, **payload)
            if not response.get("ok"):
                print("تغییر تنظیمات اعمال نشد:", response.get("error", "نامشخص"))
                return
            settings.update(payload)
        except Exception as exc:
            print("ارسال تنظیمات تصویر ناموفق بود:", exc)

    try:
        while True:
            # NEW: drain one system-audio chunk per loop so audio plays smoothly
            # even while the screen is paused or no new frame has arrived.
            if audio_out is not None:
                try:
                    chunk = client.sysaudio_chunks.get_nowait()
                    audio_out.write(chunk)
                except asyncio.QueueEmpty:
                    pass
                except Exception:
                    pass

            now = time.monotonic()
            # Process any new stream event without blocking keyboard input while paused.
            while not client.screen_events.empty():
                try:
                    event = client.screen_events.get_nowait()
                except asyncio.QueueEmpty:
                    break
                etype = event.get("type")
                if etype == "screen_error":
                    print("خطای تصویر:", event.get("error", "نامشخص"))
                    stream_failed = True
                elif etype == "screen_stopped":
                    print("جریان تصویر از سمت مقصد متوقف شد.")
                    stream_failed = True
                elif etype == "sysaudio_error":
                    print("خطای صدای سیستم:", event.get("error", "نامشخص"))
                elif etype == "sysaudio_stopped":
                    print("صدای سیستم متوقف شد.")

            packet = None
            try:
                packet = client.screen_frames.get_nowait()
            except asyncio.QueueEmpty:
                pass
            if packet is not None:
                data = packet["data"]
                try:
                    raw_bytes = base64.b64decode(data.get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                except (ValueError, TypeError) as exc:
                    print("دادهٔ تصویر نامعتبر است:", exc)
                    frame = None
                if frame is not None:
                    last_frame = frame
                    shown += 1
                    frame_times.append(packet["received_at"])
                    wire_size = packet["wire_bytes"]
                    session_bytes += wire_size
                    byte_samples.append((packet["received_at"], wire_size))

            # Keep only 2s of frame timestamps for measured display/receive FPS.
            while frame_times and now - frame_times[0] > 2.0:
                frame_times.popleft()
            while byte_samples and now - byte_samples[0][0] > 5.0:
                byte_samples.popleft()
            measured_fps = max(0, len(frame_times) - 1) / max(0.2, (frame_times[-1] - frame_times[0])) if len(frame_times) >= 2 else 0.0
            if byte_samples:
                data_span = max(1.0, now - byte_samples[0][0])
                kb_per_sec = sum(item[1] for item in byte_samples) / data_span / 1024
            else:
                kb_per_sec = 0.0
            rtt = stats["rtt_ms"]

            # Conservative adaptive control: shed load when frames are being dropped,
            # FPS falls behind its target, or RTT rises; recover gradually when stable.
            if settings["adaptive"] and not settings["paused"] and now - last_adapt >= 4.0:
                drop_delta = client.screen_dropped_frames - prev_drops
                poor = (rtt is not None and rtt > 350) or drop_delta >= max(2, int(settings["fps"] * 0.4)) or (measured_fps > 0 and measured_fps < settings["fps"] * 0.62)
                good = (rtt is not None and rtt < 180) and drop_delta == 0 and (measured_fps == 0 or measured_fps >= settings["fps"] * 0.82)
                changes = {}
                if poor:
                    if settings["fps"] > 5:
                        changes["fps"] = settings["fps"] - 1
                    if settings["quality"] > 25:
                        changes["quality"] = max(20, settings["quality"] - 8)
                elif good:
                    if settings["quality"] < 60:
                        changes["quality"] = min(65, settings["quality"] + 2)
                    elif settings["fps"] < 60:
                        changes["fps"] = settings["fps"] + 1
                if changes:
                    await apply_settings(**changes)
                last_adapt = now
                prev_drops = client.screen_dropped_frames

            if last_frame is not None:
                canvas = last_frame.copy()
                rtt_label = "n/a" if rtt is None else f"~{rtt / 2:.0f} ms"
                fps_label = f"FPS {measured_fps:.1f}/{settings['fps']}"
                rate_label = f"{kb_per_sec / 1024:.2f} MB/s"
                total_label = f"payload {session_bytes / (1024 * 1024):.2f} MB"
                drop_label = f"drops {client.screen_dropped_frames}"
                sound_label = " | SOUND" if audio_out is not None else ""
                status = f"{fps_label} | latency {rtt_label} | {rate_label} | {total_label} | {drop_label} | Q={settings['quality']} {settings['resolution']}{sound_label}"
                cv2.putText(canvas, status, (12, 28), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (20, 240, 20), 2, cv2.LINE_AA)
                if settings["paused"]:
                    cv2.putText(canvas, "PAUSED — press p to resume", (12, 58), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 170, 255), 2, cv2.LINE_AA)
                cv2.imshow(window, canvas)
            elif not stream_failed:
                placeholder = np.zeros((360, 640, 3), dtype=np.uint8)
                cv2.putText(placeholder, "Waiting for remote desktop...", (20, 180), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (230, 230, 230), 1, cv2.LINE_AA)
                cv2.imshow(window, placeholder)

            key = cv2.waitKey(1) & 0xFF
            if key in (ord("q"), 27):
                break
            elif key == ord("p"):
                await apply_settings(paused=not settings["paused"])
            elif key in (ord("+"), ord("=")):
                await apply_settings(quality=min(90, settings["quality"] + 5))
            elif key == ord("-"):
                await apply_settings(quality=max(20, settings["quality"] - 5))
            elif key == ord("]"):
                await apply_settings(fps=min(60, settings["fps"] + 1))
            elif key == ord("["):
                await apply_settings(fps=max(5, settings["fps"] - 1))
            elif key == ord("1"):
                await apply_settings(resolution="480p")
            elif key == ord("2"):
                await apply_settings(resolution="720p")
            elif key in (ord("a"), ord("A")):
                settings["adaptive"] = not settings["adaptive"]
                print("تنظیم خودکار:", "روشن" if settings["adaptive"] else "خاموش")

            if stream_failed:
                break
            if not client.connected.is_set():
                print("اتصال رله قطع شد؛ نمایش زنده متوقف می‌شود.")
                break
            await asyncio.sleep(0.008)
    except KeyboardInterrupt:
        pass
    except (ConnectionError, TimeoutError, OSError, WebSocketException) as exc:
        print("اتصال تصویر قطع شد:", exc)
    finally:
        stop_probe.set()
        probe_task.cancel()
        try:
            await probe_task
        except (asyncio.CancelledError, Exception):
            pass
        # NEW: shut down audio output and stop sysaudio on the agent
        if audio_out is not None:
            try:
                audio_out.stop()
                audio_out.close()
            except Exception:
                pass
        if with_sound:
            try:
                await client.request("sysaudio_stop", wait_time=4)
            except Exception:
                pass
        try:
            await client.request("screen_stop", wait_time=5)
        except Exception:
            pass
        cv2.destroyAllWindows()
        elapsed = max(0.1, time.monotonic() - started_at)
        total = client.screen_received_bytes - bytes_before
        print(f"نمایش زنده متوقف شد | مدت {elapsed:.1f}s | نمایش‌داده‌شده {shown} فریم | افت فریم رابط {client.screen_dropped_frames} | دادهٔ دریافتی {(total / (1024 * 1024)):.2f} MB")


async def run_screenwithsound(client, arguments=None):
    """Screen + system/desktop audio from the agent."""
    await run_screen(client, arguments, with_sound=True)


async def run_screencam(client, arguments=None):
    """Screen + camera in one window (screen main, camera PiP). sound=on adds system audio."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        print("روی تهران نصب کن: py -m pip install opencv-python numpy")
        return

    args = arguments or []
    screen_cfg = {"resolution": "480p", "fps": 30, "quality": 55}
    cam_cfg = {"resolution": "480p", "fps": 15, "quality": 60, "device": 0}
    with_sound = False

    for token in args:
        t = token.strip().lower()
        if t in ("480p", "720p"):
            screen_cfg["resolution"] = t
            cam_cfg["resolution"] = t
        elif t.startswith("fps="):
            try: screen_cfg["fps"] = max(5, min(60, int(t.split("=", 1)[1])))
            except ValueError: pass
        elif t.startswith("quality="):
            try: screen_cfg["quality"] = max(20, min(90, int(t.split("=", 1)[1])))
            except ValueError: pass
        elif t.startswith("device="):
            try: cam_cfg["device"] = max(0, min(8, int(t.split("=", 1)[1])))
            except ValueError: pass
        elif t.startswith("camfps="):
            try: cam_cfg["fps"] = max(1, min(30, int(t.split("=", 1)[1])))
            except ValueError: pass
        elif t.startswith("camquality="):
            try: cam_cfg["quality"] = max(20, min(90, int(t.split("=", 1)[1])))
            except ValueError: pass
        elif t in ("sound=on", "audio=on"):
            with_sound = True
        elif t in ("sound=off", "audio=off"):
            with_sound = False
        else:
            print(f"تنظیم ناشناخته نادیده گرفته شد: {token}")

    # Clear stale frames/events
    for q in (client.screen_frames, client.camera_frames, client.screen_events, client.camera_events):
        while not q.empty():
            try: q.get_nowait()
            except asyncio.QueueEmpty: break

    started = await client.request("screen_start", wait_time=15, **screen_cfg)
    if not started.get("ok"):
        print("شروع اسکرین ناموفق بود:", started.get("error", "نامشخص"))
        return

    cam_started = await client.request("camera_start", wait_time=12, **cam_cfg)
    if not cam_started.get("ok"):
        print("شروع دوربین ناموفق بود:", cam_started.get("error", "نامشخص"))
        try: await client.request("screen_stop", wait_time=4)
        except Exception: pass
        return

    audio_out = None
    if with_sound:
        try:
            import sounddevice as sd
            while not client.sysaudio_chunks.empty():
                try: client.sysaudio_chunks.get_nowait()
                except asyncio.QueueEmpty: break
            audio_started = await client.request("sysaudio_start", wait_time=12)
            if audio_started.get("ok"):
                audio_out = sd.RawOutputStream(samplerate=22050, channels=1, dtype="int16", blocksize=2205)
                audio_out.start()
                print("صدای سیستم هم فعال شد.")
            else:
                print("شروع صدا ناموفق بود:", audio_started.get("error", "نامشخص"))
        except Exception as exc:
            print("خطا در صدا:", exc)
            audio_out = None

    sound_label = " + Sound" if audio_out is not None else ""
    window = f"Screen + Camera{sound_label} — q:stop p:pause a:mute"
    print("تصویر اسکرین + دوربین فعال شد. q توقف، p مکث اسکرین، a قطع/وصل صدا.")
    print("نکته: دوربین در گوشهٔ بالا-راست به‌صورت PiP نمایش داده می‌شود.")

    last_screen = None
    last_cam = None
    paused = False
    audio_muted = False
    screen_frames_shown = 0
    session_start = time.monotonic()

    try:
        while client.connected.is_set():
            # Events
            while not client.screen_events.empty():
                try: ev = client.screen_events.get_nowait()
                except asyncio.QueueEmpty: break
                etype = ev.get("type")
                if etype in ("screen_error", "camera_error", "sysaudio_error"):
                    print("خطا:", ev.get("error", "نامشخص"))
                elif etype in ("screen_stopped", "camera_stopped", "sysaudio_stopped"):
                    print(f"{etype} رخ داد.")

            while not client.camera_events.empty():
                try: ev = client.camera_events.get_nowait()
                except asyncio.QueueEmpty: break
                if ev.get("type") in ("camera_error", "mic_error"):
                    print("خطای رسانه:", ev.get("error", "نامشخص"))
                elif ev.get("type") == "camera_stopped":
                    print("دوربین متوقف شد.")

            # Screen frame
            try:
                packet = client.screen_frames.get_nowait()
                raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                if frame is not None:
                    last_screen = frame
                    screen_frames_shown += 1
            except (asyncio.QueueEmpty, ValueError, TypeError):
                pass

            # Camera frame
            try:
                packet = client.camera_frames.get_nowait()
                raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                if frame is not None:
                    last_cam = frame
            except (asyncio.QueueEmpty, ValueError, TypeError):
                pass

            # Audio
            if audio_out is not None:
                if not audio_muted:
                    try:
                        chunk = client.sysaudio_chunks.get_nowait()
                        audio_out.write(chunk)
                    except asyncio.QueueEmpty:
                        pass
                    except Exception:
                        pass
                else:
                    # drain to avoid backlog while muted
                    while True:
                        try: client.sysaudio_chunks.get_nowait()
                        except asyncio.QueueEmpty: break
                        except Exception: break

            # Compose display: screen main, camera PiP top-right
            if last_screen is not None:
                canvas = last_screen.copy()
                if last_cam is not None:
                    try:
                        pip_w = max(160, canvas.shape[1] // 4)
                        pip_h = max(120, int(last_cam.shape[0] * pip_w / last_cam.shape[1]))
                        pip = cv2.resize(last_cam, (pip_w, pip_h))
                        x = max(0, canvas.shape[1] - pip_w - 10)
                        y = 10
                        if y + pip_h <= canvas.shape[0]:
                            canvas[y:y + pip_h, x:x + pip_w] = pip
                            cv2.rectangle(canvas, (x - 1, y - 1), (x + pip_w, y + pip_h), (0, 255, 0), 1)
                    except Exception:
                        pass
                if paused:
                    cv2.putText(canvas, "PAUSED", (12, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 170, 255), 2, cv2.LINE_AA)
                if audio_muted:
                    cv2.putText(canvas, "MUTED", (12, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 80, 255), 2, cv2.LINE_AA)
                cv2.imshow(window, canvas)
            elif last_cam is not None:
                cv2.imshow(window, last_cam)
            else:
                placeholder = np.zeros((360, 640, 3), dtype=np.uint8)
                cv2.putText(placeholder, "Waiting for screen + camera...", (20, 180),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.65, (220, 220, 220), 1, cv2.LINE_AA)
                cv2.imshow(window, placeholder)

            key = cv2.waitKey(1) & 0xFF
            if key in (ord("q"), 27):
                break
            elif key == ord("p"):
                paused = not paused
                try: await client.request("screen_config", wait_time=5, paused=paused)
                except Exception: pass
            elif key in (ord("a"), ord("A")):
                if audio_out is not None:
                    audio_muted = not audio_muted
                    print("صدا:", "قطع" if audio_muted else "وصل")

            await asyncio.sleep(0.008)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print("خطا:", exc)
    finally:
        if audio_out is not None:
            try: audio_out.stop(); audio_out.close()
            except Exception: pass
        for act in ("screen_stop", "camera_stop", "sysaudio_stop"):
            try: await client.request(act, wait_time=5)
            except Exception: pass
        cv2.destroyAllWindows()
        elapsed = max(0.1, time.monotonic() - session_start)
        print(f"جلسه پایان یافت | مدت {elapsed:.1f}s | فریم اسکرین {screen_frames_shown}")


async def run_camera(client, arguments=None):
    try:
        import cv2
        import numpy as np
    except ImportError:
        print("روی تهران نصب کن: py -m pip install opencv-python numpy")
        return
    args = arguments or []
    opts = {"resolution":"480p", "fps":15, "quality":60, "device":0}
    for token in args:
        if token.lower() in ("480p", "720p"): opts["resolution"] = token.lower()
        elif "=" in token:
            k,v = token.split("=",1)
            if k in ("fps","quality","device"):
                try: opts[k] = int(v)
                except ValueError: print(f"مقدار نامعتبر: {token}"); return
    while not client.camera_frames.empty():
        try: client.camera_frames.get_nowait()
        except asyncio.QueueEmpty: break
    while not client.camera_events.empty():
        try: client.camera_events.get_nowait()
        except asyncio.QueueEmpty: break
    result = await client.request("camera_start", wait_time=12, **opts)
    if not result.get("ok"):
        print("شروع دوربین ناموفق بود:", result.get("error", "خطای نامشخص")); return
    print("دوربین زنده شروع شد؛ برای توقف روی پنجره کلیک کن و q بزن. روی مقصد نیز وضعیت فعال بودن در ترمینال نشان داده می‌شود.")
    window = "Remote camera — q:stop"
    shown = 0; start = time.monotonic(); times = collections.deque(); last = None
    try:
        while client.connected.is_set():
            while not client.camera_events.empty():
                event = client.camera_events.get_nowait()
                if event.get("type") == "camera_error": print("خطای دوربین:", event.get("error")); return
                if event.get("type") == "camera_stopped": return
            try: packet = client.camera_frames.get_nowait()
            except asyncio.QueueEmpty: packet = None
            if packet:
                try:
                    raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw,dtype=np.uint8), cv2.IMREAD_COLOR)
                except Exception: frame = None
                if frame is not None:
                    last = frame; shown += 1; times.append(packet["received_at"])
            now=time.monotonic()
            while times and now-times[0]>2: times.popleft()
            fps=(len(times)-1)/max(.1,times[-1]-times[0]) if len(times)>1 else 0
            if last is not None:
                canvas=last.copy(); cv2.putText(canvas,f"Camera FPS {fps:.1f} | {opts['resolution']} | q: stop",(10,25),cv2.FONT_HERSHEY_SIMPLEX,.6,(0,255,0),2,cv2.LINE_AA); cv2.imshow(window,canvas)
            key=cv2.waitKey(1)&0xFF
            if key in (ord('q'),27): break
            await asyncio.sleep(.005)
    except (OSError, WebSocketException, ConnectionError) as exc: print("دوربین قطع شد:",exc)
    finally:
        try: await client.request("camera_stop",wait_time=4)
        except Exception: pass
        cv2.destroyWindow(window)
        print(f"دوربین متوقف شد؛ {shown} فریم در {max(.1,time.monotonic()-start):.1f} ثانیه دریافت شد.")


async def run_microphone(client):
    while not client.camera_events.empty():
        try: client.camera_events.get_nowait()
        except asyncio.QueueEmpty: break
    try: import sounddevice as sd
    except ImportError:
        print("روی تهران نصب کن: py -m pip install sounddevice"); return
    while not client.mic_chunks.empty():
        try: client.mic_chunks.get_nowait()
        except asyncio.QueueEmpty: break
    result = await client.request("mic_start", wait_time=12)
    if not result.get("ok"):
        print("شروع میکروفون ناموفق بود:", result.get("error", "خطای نامشخص")); return
    print("صدای زنده فعال است؛ برای توقف q را در همین ترمینال بزن. روی مقصد وضعیت فعال بودن میکروفون در ترمینال دیده می‌شود.")
    stopped = False; bytes_played = 0
    try:
        with sd.RawOutputStream(samplerate=16000, channels=1, dtype="int16", blocksize=1600) as stream:
            while client.connected.is_set():
                try:
                    import msvcrt
                    if msvcrt.kbhit() and msvcrt.getwch().lower() == "q":
                        break
                except ImportError:
                    pass
                try: chunk = await asyncio.wait_for(client.mic_chunks.get(), timeout=0.1)
                except asyncio.TimeoutError: continue
                stream.write(chunk); bytes_played += len(chunk)
                while not client.camera_events.empty():
                    event=client.camera_events.get_nowait()
                    if event.get("type")=="mic_error": print("خطای میکروفون:",event.get("error")); stopped=True; break
                    if event.get("type")=="mic_stopped": stopped=True; break
                if stopped: break
    except KeyboardInterrupt: pass
    except Exception as exc: print("پخش صوت متوقف شد:",exc)
    finally:
        try: await client.request("mic_stop",wait_time=4)
        except Exception: pass
        print(f"میکروفون متوقف شد؛ {bytes_played/32000:.1f} ثانیه صوت خام پخش شد.")


async def run_av(client, arguments=None):
    """Start camera and microphone together; q closes both streams."""
    try:
        import cv2
        import numpy as np
        import sounddevice as sd
    except ImportError:
        print("روی تهران نصب کن: py -m pip install opencv-python numpy sounddevice")
        return
    opts = {"resolution": "480p", "fps": 15, "quality": 60, "device": 0}
    for token in (arguments or []):
        if token.lower() in ("480p", "720p"):
            opts["resolution"] = token.lower()
        elif "=" in token:
            k, v = token.split("=", 1)
            if k in ("fps", "quality", "device"):
                try: opts[k] = int(v)
                except ValueError:
                    print(f"مقدار نامعتبر: {token}")
                    return
    for q in (client.camera_frames, client.mic_chunks, client.camera_events):
        while not q.empty():
            try: q.get_nowait()
            except asyncio.QueueEmpty: break
    cam = await client.request("camera_start", wait_time=12, **opts)
    if not cam.get("ok"):
        print("شروع دوربین ناموفق بود:", cam.get("error", "خطای نامشخص"))
        return
    mic = await client.request("mic_start", wait_time=12)
    if not mic.get("ok"):
        try: await client.request("camera_stop", wait_time=4)
        except Exception: pass
        print("شروع میکروفون ناموفق بود:", mic.get("error", "خطای نامشخص"))
        return
    print("دوربین + میکروفون فعال‌اند. روی پنجره کلیک کن و q بزن تا هر دو متوقف شوند.")
    window = "Camera + Microphone — q: stop both"
    last = None
    shown = 0
    bytes_played = 0
    started = time.monotonic()
    try:
        with sd.RawOutputStream(samplerate=16000, channels=1, dtype="int16", blocksize=1600) as audio:
            while client.connected.is_set():
                while not client.camera_events.empty():
                    ev = client.camera_events.get_nowait()
                    if ev.get("type") in ("camera_error", "mic_error"):
                        print("خطای رسانه:", ev.get("error", ev.get("type")))
                        return
                try:
                    packet = client.camera_frames.get_nowait()
                    raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                    if frame is not None:
                        last = frame
                        shown += 1
                except asyncio.QueueEmpty:
                    pass
                except Exception:
                    pass
                try:
                    chunk = client.mic_chunks.get_nowait()
                    audio.write(chunk)
                    bytes_played += len(chunk)
                except asyncio.QueueEmpty:
                    pass
                if last is not None:
                    canvas = last.copy()
                    cv2.putText(canvas, "Camera + Microphone ACTIVE | q: stop both",
                                (10, 25), cv2.FONT_HERSHEY_SIMPLEX, .6, (0, 255, 0), 2, cv2.LINE_AA)
                    cv2.imshow(window, canvas)
                else:
                    placeholder = np.zeros((360, 640, 3), dtype=np.uint8)
                    cv2.putText(placeholder, "Waiting for camera...", (20, 180),
                                cv2.FONT_HERSHEY_SIMPLEX, .7, (230, 230, 230), 1, cv2.LINE_AA)
                    cv2.imshow(window, placeholder)
                if (cv2.waitKey(1) & 0xFF) in (ord("q"), 27):
                    break
                await asyncio.sleep(.005)
    except Exception as exc:
        print("جریان رسانه متوقف شد:", exc)
    finally:
        for action in ("camera_stop", "mic_stop"):
            try: await client.request(action, wait_time=4)
            except Exception: pass
        try: cv2.destroyWindow(window)
        except Exception: pass
        print(f"هر دو قابلیت متوقف شدند | {shown} فریم | صوت پخش‌شده حدود {bytes_played/32000:.1f} ثانیه | مدت {time.monotonic()-started:.1f}s")


async def run_client():
    url = RELAY_URL.strip().rstrip("/")
    if not url.startswith("wss://") and not (
        url.startswith("ws://localhost") or url.startswith("ws://127.")
    ):
        print("RELAY_URL باید با wss:// شروع شود (به جز localhost).")
        return
    if PAIR_CODE == DEFAULT_PAIR_CODE:
        print("⚠️  PAIR_CODE پیش‌فرض است؛ یک کد تصادفی طولانی تنظیم کن.")

    client = RemoteClient(url)
    try:
        await client.connect()
        print("برای راهنما `help` را وارد کن.")
        while True:
            try:
                line = await asyncio.to_thread(input, "تهران> ")
            except (EOFError, KeyboardInterrupt):
                break
            line = line.strip()
            if not line:
                continue

            original = line
            # shlex.split(posix=False) both respects quotes and preserves
            # Windows backslashes like D:\POCO X3. This is the only parser
            # used by the client; every command below reads from `tokens`.
            tokens = split_command(line)
            cmd = tokens[0].lower() if tokens else ""
            arg = tokens[1] if len(tokens) > 1 else ""
            extra = tokens[2] if len(tokens) > 2 else ""

            try:
                if cmd in ("quit", "exit", "q"):
                    break
                elif cmd == "help":
                    show_help()
                elif cmd == "reconnect":
                    print("قطع اتصال و تلاش مجدد...")
                    try:
                        await client.close()
                    except Exception:
                        pass
                    try:
                        client = RemoteClient(url)
                        await client.connect()
                    except Exception as exc:
                        print("اتصال مجدد ناموفق:", exc)

                elif cmd.startswith("!"):
                    # Force-shell: take everything after '!' verbatim, keep quotes.
                    shell_cmd = original[1:].strip()
                    if not shell_cmd:
                        print("مثال: !ls -la /tmp")
                    else:
                        await run_shell(client, shell_cmd)

                elif cmd not in KNOWN_COMMANDS:
                    # Fallback shell: pass the whole line to the agent verbatim.
                    await run_shell(client, original)

                elif cmd == "ls":
                    await print_listing(client, arg)
                elif cmd == "pwd":
                    result = await client.request("info", ".")
                    if result.get("ok"):
                        print(result.get("path", "مسیر ریشه روی مقصد"))
                    else:
                        print("خطا:", result.get("error"))
                elif cmd in ("info", "stat"):
                    result = await client.request("info", arg or ".")
                    if result.get("ok"):
                        for key in ("path", "type", "size", "modified", "items"):
                            if key in result:
                                print(f"{key}: {result[key]}")
                    else:
                        print("خطا:", result.get("error"))
                elif cmd == "get":
                    if not arg:
                        print('مثال: get "D:\\POCO X3\\Camera\\video.mp4"')
                        continue
                    # timeout طولانی چون پاسخ base64 برای فایل ۲۰۰MB حدود ۲۶۷MB است
                    result = await client.request(
                        "get", arg, wait_time=GET_TIMEOUT_SECONDS
                    )
                    if not result.get("ok"):
                        print("خطا:", result.get("error"))
                        continue
                    name = extra.strip() if extra else Path(arg).name
                    name = Path(name).name
                    if not name:
                        print("نام فایل محلی معتبر نیست.")
                        continue
                    dest = Path.cwd() / name
                    if dest.exists():
                        confirm = await asyncio.to_thread(input, f"{dest.name} وجود دارد؛ جایگزین شود؟ [y/N] ")
                        if confirm.strip().lower() not in ("y", "yes"):
                            print("دانلود لغو شد.")
                            continue
                    data = base64.b64decode(result["data"], validate=True)
                    if "size" in result and len(data) != result["size"]:
                        print("خطا: حجم فایل دریافتی با اعلام مطابقت ندارد.")
                        continue
                    temp = dest.with_name(dest.name + ".part")
                    await asyncio.to_thread(temp.write_bytes, data)
                    await asyncio.to_thread(os.replace, temp, dest)
                    print(f"دانلود شد: {dest} ({len(data):,} بایت)")
                elif cmd == "cat":
                    if not arg:
                        print('مثال: cat "D:\\POCO X3\\notes.txt"')
                        continue
                    result = await client.request("get", arg, wait_time=CAT_TIMEOUT_SECONDS)
                    if not result.get("ok"):
                        print("خطا:", result.get("error"))
                        continue
                    data = base64.b64decode(result["data"], validate=True)
                    if len(data) > 256 * 1024:
                        print("فایل بزرگ‌تر از ۲۵۶ کیلوبایت است؛ برای جلوگیری از خروجی زیاد، با get دانلودش کن.")
                        continue
                    print(data.decode("utf-8", errors="replace"))
                elif cmd == "search":
                    if not arg or not extra:
                        print('مثال: search "D:\\POCO X3" VID')
                        continue
                    result = await client.request("search", arg, query=extra)
                    if result.get("ok"):
                        for item in result.get("items", []):
                            print(("[DIR] " if item.get("is_dir") else "      ") + item.get("path", item.get("name", "")))
                        if result.get("truncated"):
                            print("نتیجه‌ها محدود شده‌اند.")
                    else:
                        print("خطا:", result.get("error"))
                elif cmd == "ping":
                    started = time.monotonic()
                    result = await client.request("ping", ".")
                    elapsed = (time.monotonic() - started) * 1000
                    print(f"پاسخ عامل: {result.get('ok')} | زمان رفت‌وبرگشت: {elapsed:.0f} ms")
                elif cmd == "screen":
                    await run_screen(client, tokens[1:])
                elif cmd == "screenwithsound":
                    await run_screenwithsound(client, tokens[1:])
                elif cmd == "screencam":
                    await run_screencam(client, tokens[1:])
                elif cmd == "camera":
                    await run_camera(client, tokens[1:])
                elif cmd == "mic":
                    await run_microphone(client)
                elif cmd in ("av", "media"):
                    await run_av(client, tokens[1:])
                else:
                    print("دستور نامعتبر است؛ برای راهنما `help` را وارد کن.")
            except (ConnectionError, TimeoutError, OSError, ValueError, WebSocketException) as exc:
                print("خطا:", exc)
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        if ROLE == "server":
            pass
        else:
            asyncio.run(run_client())
    except KeyboardInterrupt:
        print("\nخروج انجام شد.")
