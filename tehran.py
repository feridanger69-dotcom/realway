"""
Tehran client + Railway WebSocket relay.

Run locally in Tehran:
    py tehran.py

Run on Railway as the relay service:
    ROLE=server

File transfer uses chunked binary WebSocket frames (magic "KFT1"):
    header = b"KFT1" + uuid16 + seq(>Q)  =  28 bytes
    frame  = header + payload
"""
import asyncio
import base64
import collections
import hashlib
import json
import os
import random
import shlex
import shutil
import struct
import time
import uuid
from pathlib import Path

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

PAIR_CODE = "Mrfisgoodboy"
DEFAULT_PAIR_CODE = "ChangeThis-ToYourOwn-PrivateCode-9381"
RELAY_URL = "wss://kermanshah-relay.f2774656.workers.dev/ws"
ROLE = "client"

# Legacy single-shot `get` is only used by `cat` (small text files).
MAX_FILE_BYTES = 1024 * 1024
# Chunked transfer is the primary path; per-frame size stays well under 16 MiB.
MAX_MESSAGE_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT = 45
CONNECT_TIMEOUT = 20
PING_INTERVAL = 30
PING_TIMEOUT = 90
MAX_SEARCH_RESULTS = 200
GET_TIMEOUT_SECONDS = 900
CAT_TIMEOUT_SECONDS = 120

# ---- chunked file transfer protocol ----
FILE_MAGIC = b"KFT1"
FILE_HEADER_SIZE = 28  # 4 magic + 16 uuid + 8 seq
MIN_CHUNK_SIZE = 256 * 1024
MAX_CHUNK_SIZE = 1024 * 1024
DEFAULT_CHUNK_SIZE = max(
    MIN_CHUNK_SIZE,
    min(MAX_CHUNK_SIZE, int(os.getenv("KFT_CHUNK_SIZE", str(512 * 1024)))),
)
FILE_QUEUE_MAX = 8  # 8 × 512 KiB = 4 MiB bounded buffer

KNOWN_COMMANDS = {
    "help", "quit", "exit", "q", "reconnect",
    "ls", "pwd", "info", "stat", "get", "cat", "search", "ping",
    "screen", "camera", "mic", "av", "media",
    "screencam", "screenwithsound",
}


def json_dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def split_command(line):
    try:
        raw = shlex.split(line, posix=False)
    except ValueError:
        raw = line.split()
    return [t.strip('"\'') for t in raw]


def _fmt_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024


def _print_progress(name, received, total, started, last_bytes, last_time):
    if total <= 0:
        return
    now = time.monotonic()
    pct = 100.0 * received / total
    elapsed = max(0.001, now - started)
    avg = received / elapsed
    dt = now - last_time if last_time else 0
    inst = (received - last_bytes) / dt if dt > 0 else avg
    if avg > 0:
        eta = max(0.0, (total - received) / avg)
        eta_str = f"{int(eta)}s" if eta < 3600 else f"{eta/3600:.1f}h"
    else:
        eta_str = "?"
    line = (
        f"\r{name} | {_fmt_bytes(received)}/{_fmt_bytes(total)} "
        f"({pct:5.1f}%) | {_fmt_bytes(inst)}/s | "
        f"avg {_fmt_bytes(avg)}/s | ETA {eta_str}   "
    )
    print(line, end="", flush=True)


# ---------------- Railway relay mode ----------------
if ROLE == "server":
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import JSONResponse
    import uvicorn

    app = FastAPI(title="Two-file remote relay")
    peers = {"agent": None, "client": None}
    state_lock = asyncio.Lock()

    async def send_json(ws, message):
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
            await send_json(ws, {"type": "ready", "role": role, "ok": True})

            other_role = "client" if role == "agent" else "agent"

            while True:
                raw = await ws.receive()
                if raw.get("type") == "websocket.disconnect":
                    break

                text = raw.get("text")
                binary = raw.get("bytes")

                async with state_lock:
                    other = peers.get(other_role)

                if other is None:
                    if role == "client" and text is not None:
                        try:
                            parsed = json.loads(text)
                        except json.JSONDecodeError:
                            parsed = {}
                        try:
                            await send_json(ws, {
                                "id": parsed.get("id"),
                                "ok": False,
                                "error": "لپ‌تاپ کرمانشاه آنلاین نیست.",
                            })
                        except Exception:
                            pass
                    continue

                if text is not None:
                    try:
                        await other.send_text(text)
                    except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
                        async with state_lock:
                            if peers.get(other_role) is other:
                                peers[other_role] = None
                        if role == "client":
                            try:
                                parsed = json.loads(text)
                            except json.JSONDecodeError:
                                parsed = {}
                            try:
                                await send_json(ws, {
                                    "id": parsed.get("id"),
                                    "ok": False,
                                    "error": "Agent disconnected while forwarding the request.",
                                })
                            except Exception:
                                pass
                elif binary is not None:
                    try:
                        await other.send_bytes(binary)
                    except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
                        async with state_lock:
                            if peers.get(other_role) is other:
                                peers[other_role] = None
                        if role == "client":
                            try:
                                await send_json(ws, {
                                    "type": "file_error",
                                    "error": "Agent disconnected during transfer.",
                                })
                            except Exception:
                                pass

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
            ws_max_size=MAX_MESSAGE_BYTES,
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
        self.screen_frames = asyncio.Queue(maxsize=1)
        self.screen_events = asyncio.Queue(maxsize=8)
        self.screen_dropped_frames = 0
        self.screen_received_bytes = 0
        self.camera_frames = asyncio.Queue(maxsize=1)
        self.camera_events = asyncio.Queue(maxsize=8)
        self.mic_chunks = asyncio.Queue(maxsize=8)
        self.sysaudio_chunks = asyncio.Queue(maxsize=8)
        self.media_bytes = 0
        # NEW: chunked file transfer
        self.file_transfer_queue = asyncio.Queue(maxsize=FILE_QUEUE_MAX)
        self.active_transfer_id = None

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
        try:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=10)
            ready = json.loads(raw)
            if isinstance(ready, dict) and ready.get("type") == "ready" and not ready.get("ok", True):
                raise ConnectionError(ready.get("error", "Authentication failed"))
        except asyncio.TimeoutError:
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
                # ---- binary = file chunk ----
                if isinstance(raw, bytes):
                    if len(raw) >= FILE_HEADER_SIZE and raw[:4] == FILE_MAGIC:
                        try:
                            self.file_transfer_queue.put_nowait(("chunk", raw))
                        except asyncio.QueueFull:
                            print("\n[هشدار] صف دریافت پر است؛ یک قطعه دور انداخته شد.")
                    continue

                # ---- text = JSON control ----
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                message_type = data.get("type")

                # NEW: file transfer completion / error
                if message_type == "file_complete":
                    try:
                        self.file_transfer_queue.put_nowait(("file_complete", data))
                    except asyncio.QueueFull:
                        pass
                    continue
                if message_type == "file_error":
                    try:
                        self.file_transfer_queue.put_nowait(("file_error", data))
                    except asyncio.QueueFull:
                        pass
                    continue

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
                        try: self.screen_events.get_nowait()
                        except asyncio.QueueEmpty: pass
                    try: self.screen_events.put_nowait(data)
                    except asyncio.QueueFull: pass
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
  ls [مسیر]                    فهرست فایل‌ها
  pwd                          مسیر ریشه روی لپ‌تاپ مقصد
  info [مسیر]                  اطلاعات کلی پوشه/فایل
  stat <مسیر>                  جزئیات یک فایل یا پوشه
  get <مسیر> [نام‌محلی] [--chunk=N]
                               دانلود فایل با انتقال قطعه‌ای باینری (بدون محدودیت اندازه)
                               --chunk بین 262144 (256 KiB) و 1048576 (1 MiB)
  cat <مسیر فایل متنی>         نمایش فایل متنی کوچک (حداکثر 200 KiB)
  search <پوشه> <عبارت>        جست‌وجو در نام فایل/پوشه
  ping                         تست رفت‌وبرگشت

  screen [480p|720p] [fps=5-60] [quality=20-90] [auto=on|off]
  screenwithsound [480p|720p] ...    اسکرین + صدای سیستم
  camera [480p|720p] [fps=1-30] [quality=20-90] [device=0]
  mic                          پخش صدای زنده میکروفون
  av | media                   دوربین + میکروفون
  screencam [...]              اسکرین + دوربین همزمان

  reconnect                    اتصال مجدد به رله
  quit                         خروج

Shell روی مقصد:
  هر چیزی که در لیست بالا نباشد در shell مقصد اجرا می‌شود.
  برای اجبار به shell، از پیشوند ! استفاده کن:   !ls -la /tmp

نکته‌ها:
- دانلود فایل با پروتکل قطعه‌ای باینری است: دیگر محدودیت 16 MiB وجود ندارد.
- در حین دریافت، پیشرفت، سرعت لحظه‌ای/میانگین و ETA نشان داده می‌شود.
- فایل ابتدا با پسوند .part ذخیره و پس از تأیید SHA-256 تغییر نام داده می‌شود.
- در پنجرهٔ screen: q توقف، p مکث، +/- کیفیت، [ ] نرخ فریم، 1=480p، 2=720p، a=تنظیم خودکار.
""")


async def print_listing(client, path):
    result = await client.request("list", path or ".")
    if not result.get("ok"):
        print("خطا:", result.get("error", "نامشخص"))
        return
    for item in result.get("items", []):
        print(("[DIR] " if item.get("is_dir") else "      ") + item.get("name", ""))
    if result.get("truncated"):
        print("هشدار: فهرست به سقف تعداد آیتم‌ها رسیده.")


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


# ---------------- chunked file download ----------------
async def download_file(client, remote_path, local_name=None, chunk_size=DEFAULT_CHUNK_SIZE):
    """Download `remote_path` from the agent using chunked binary transfer.

    Returns (success: bool, error: str | None).
    """
    chunk_size = max(MIN_CHUNK_SIZE, min(MAX_CHUNK_SIZE, int(chunk_size)))
    remote_name = Path(remote_path).name
    if not remote_name:
        return False, "نام فایل نامعتبر است."
    local_name = local_name or remote_name
    local_path = Path.cwd() / local_name

    if local_path.exists():
        confirm = await asyncio.to_thread(
            input, f"{local_path.name} وجود دارد؛ جایگزین شود؟ [y/N] "
        )
        if confirm.strip().lower() not in ("y", "yes"):
            print("دانلود لغو شد.")
            return False, "cancelled"

    # 1) initiate transfer
    try:
        begin = await client.request(
            "file_begin", remote_path, wait_time=20, chunk_size=chunk_size
        )
    except Exception as exc:
        return False, f"درخواست شروع انتقال ناموفق: {exc}"

    if not begin.get("ok"):
        return False, begin.get("error", "خطای نامشخص")

    tid = begin["transfer_id"]
    total_size = int(begin["size"])

    # 2) disk space check
    try:
        free = shutil.disk_usage(Path.cwd()).free
        if free < total_size + 32 * 1024 * 1024:
            try:
                await client.request("file_abort", transfer_id=tid, wait_time=5)
            except Exception:
                pass
            return False, (
                f"فضای دیسک کافی نیست (نیاز: {_fmt_bytes(total_size + 32*1024*1024)}، "
                f"آزاد: {_fmt_bytes(free)})"
            )
    except OSError:
        pass

    # 3) prepare .part file
    part_path = local_path.with_name(local_path.name + ".part")
    if part_path.exists():
        try:
            await asyncio.to_thread(part_path.unlink)
        except OSError as exc:
            return False, f"حذف .part قبلی ناموفق: {exc}"

    # clear stale chunks
    while not client.file_transfer_queue.empty():
        try:
            client.file_transfer_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

    client.active_transfer_id = tid
    expected_sha = None
    received = 0
    next_seq = 0
    sha = hashlib.sha256()
    started = time.monotonic()
    last_print = started
    last_print_bytes = 0
    success = False
    error_msg = None

    print(f"دریافت {remote_name} ({_fmt_bytes(total_size)}) → {local_path}")

    try:
        f = await asyncio.to_thread(open, part_path, "wb")
    except OSError as exc:
        client.active_transfer_id = None
        try:
            await client.request("file_abort", transfer_id=tid, wait_time=5)
        except Exception:
            pass
        return False, f"باز کردن فایل مقصد ناموفق: {exc}"

    try:
        while True:
            try:
                item = await asyncio.wait_for(
                    client.file_transfer_queue.get(), timeout=120.0
                )
            except asyncio.TimeoutError:
                error_msg = "مدت طولانی داده‌ای دریافت نشد (timeout)."
                break
            except asyncio.CancelledError:
                error_msg = "لغو شد."
                break

            kind = item[0]

            if kind == "chunk":
                _, raw = item
                if len(raw) < FILE_HEADER_SIZE or raw[:4] != FILE_MAGIC:
                    continue
                try:
                    chunk_tid = str(uuid.UUID(bytes=raw[4:20]))
                    seq = struct.unpack(">Q", raw[20:28])[0]
                except (ValueError, struct.error):
                    continue
                if chunk_tid != tid:
                    continue  # stale chunk from a previous transfer
                if seq != next_seq:
                    error_msg = f"ترتیب قطعه نادرست (انتظار {next_seq}، دریافت {seq})."
                    break
                payload = raw[28:]
                try:
                    await asyncio.to_thread(f.write, payload)
                except OSError as exc:
                    error_msg = f"نوشتن در فایل ناموفق: {exc}"
                    break
                sha.update(payload)
                received += len(payload)
                next_seq += 1
                now = time.monotonic()
                if now - last_print >= 0.25 or received == total_size:
                    _print_progress(remote_name, received, total_size,
                                    started, last_print_bytes, last_print)
                    last_print = now
                    last_print_bytes = received

            elif kind == "file_complete":
                _, data = item
                if data.get("transfer_id") != tid:
                    error_msg = "شناسه انتقال نامعتبر در پیام پایانی."
                    break
                expected_sha = data.get("sha256")
                announced = data.get("size")
                if announced is not None and int(announced) != received:
                    error_msg = (
                        f"اندازه دریافت‌شده ({received}) با اعلام ({announced}) فرق دارد."
                    )
                    break
                success = True
                break

            elif kind == "file_error":
                _, data = item
                error_msg = data.get("error", "خطای نامشخص از عامل")
                break
    finally:
        try:
            await asyncio.to_thread(f.close)
        except Exception:
            pass

    client.active_transfer_id = None

    if not success:
        try:
            await asyncio.to_thread(part_path.unlink)
        except OSError:
            pass
        try:
            await client.request("file_abort", transfer_id=tid, wait_time=5)
        except Exception:
            pass
        print()
        return False, error_msg or "انتقال ناموفق بود."

    # 4) verify checksum
    actual_sha = sha.hexdigest()
    if expected_sha and actual_sha != expected_sha:
        try:
            await asyncio.to_thread(part_path.unlink)
        except OSError:
            pass
        print()
        return False, (
            f"ناسازگاری checksum (انتظار {expected_sha[:16]}...، "
            f"دریافت {actual_sha[:16]}...)"
        )

    # 5) finalize
    try:
        await asyncio.to_thread(os.replace, part_path, local_path)
    except OSError as exc:
        return False, f"تغییر نام .part ناموفق: {exc}"

    elapsed = max(0.01, time.monotonic() - started)
    print()
    print(f"✓ دانلود شد: {local_path} ({_fmt_bytes(received)} در {elapsed:.1f}s، "
          f"میانگین {_fmt_bytes(received/elapsed)}/s)")
    print(f"  SHA-256: {actual_sha}")
    return True, None


# ---------------- media commands ----------------
async def run_screen(client, arguments=None, with_sound=False):
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
            try: settings["fps"] = max(5, min(60, int(token.split("=", 1)[1])))
            except ValueError: pass
        elif token.startswith("quality="):
            try: settings["quality"] = max(20, min(90, int(token.split("=", 1)[1])))
            except ValueError: pass
        elif token in ("auto=off", "adaptive=off"):
            settings["adaptive"] = False
        elif token in ("auto=on", "adaptive=on"):
            settings["adaptive"] = True
        elif token in ("sound=on", "audio=on"):
            with_sound = True
        elif token in ("sound=off", "audio=off"):
            with_sound = False

    while not client.screen_frames.empty():
        try: client.screen_frames.get_nowait()
        except asyncio.QueueEmpty: break
    while not client.screen_events.empty():
        try: client.screen_events.get_nowait()
        except asyncio.QueueEmpty: break
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

    audio_out = None
    if with_sound:
        try:
            import sounddevice as sd
            while not client.sysaudio_chunks.empty():
                try: client.sysaudio_chunks.get_nowait()
                except asyncio.QueueEmpty: break
            s = await client.request("sysaudio_start", wait_time=12)
            if s.get("ok"):
                audio_out = sd.RawOutputStream(
                    samplerate=22050, channels=1, dtype="int16", blocksize=2205
                )
                audio_out.start()
                print("صدای سیستم فعال شد.")
            else:
                print("شروع صدای سیستم ناموفق بود:", s.get("error", "نامشخص"))
        except Exception as exc:
            print("خطا در راه‌اندازی صدا:", exc)
            audio_out = None

    window = "Remote desktop — q:stop p:pause +/-:quality []:fps 1=480p 2=720p a:auto"
    print("تصویر زنده شروع شد.")
    print("کلیدها: q توقف | p مکث/ادامه | + - کیفیت | [ ] نرخ فریم | 1=480p | 2=720p | a=تنظیم خودکار")

    stop_probe = asyncio.Event()
    stats = {"rtt_ms": None}

    async def ping_loop():
        while not stop_probe.is_set():
            begin = time.monotonic()
            try:
                result = await client.request("ping", ".", wait_time=6)
                if result.get("ok"):
                    stats["rtt_ms"] = (time.monotonic() - begin) * 1000
            except Exception:
                stats["rtt_ms"] = None
            try: await asyncio.wait_for(stop_probe.wait(), timeout=2.5)
            except asyncio.TimeoutError: pass

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
        payload = {k: v for k, v in changes.items()
                   if k in ("resolution", "fps", "quality", "paused")}
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
            if audio_out is not None:
                try: audio_out.write(client.sysaudio_chunks.get_nowait())
                except (asyncio.QueueEmpty, Exception): pass

            now = time.monotonic()
            while not client.screen_events.empty():
                try: event = client.screen_events.get_nowait()
                except asyncio.QueueEmpty: break
                etype = event.get("type")
                if etype in ("screen_error", "sysaudio_error"):
                    print("خطا:", event.get("error", "نامشخص"))
                    if etype == "screen_error": stream_failed = True
                elif etype in ("screen_stopped", "sysaudio_stopped"):
                    print(f"{etype} رخ داد.")
                    if etype == "screen_stopped": stream_failed = True

            packet = None
            try: packet = client.screen_frames.get_nowait()
            except asyncio.QueueEmpty: pass
            if packet is not None:
                data = packet["data"]
                try:
                    raw_bytes = base64.b64decode(data.get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                except (ValueError, TypeError):
                    frame = None
                if frame is not None:
                    last_frame = frame
                    shown += 1
                    frame_times.append(packet["received_at"])
                    session_bytes += packet["wire_bytes"]
                    byte_samples.append((packet["received_at"], packet["wire_bytes"]))

            while frame_times and now - frame_times[0] > 2.0:
                frame_times.popleft()
            while byte_samples and now - byte_samples[0][0] > 5.0:
                byte_samples.popleft()
            measured_fps = (
                max(0, len(frame_times) - 1) / max(0.2, (frame_times[-1] - frame_times[0]))
                if len(frame_times) >= 2 else 0.0
            )
            kb_per_sec = (
                sum(item[1] for item in byte_samples) / max(1.0, now - byte_samples[0][0]) / 1024
                if byte_samples else 0.0
            )
            rtt = stats["rtt_ms"]

            if settings["adaptive"] and not settings["paused"] and now - last_adapt >= 4.0:
                drop_delta = client.screen_dropped_frames - prev_drops
                poor = (
                    (rtt is not None and rtt > 350)
                    or drop_delta >= max(2, int(settings["fps"] * 0.4))
                    or (measured_fps > 0 and measured_fps < settings["fps"] * 0.62)
                )
                good = (
                    (rtt is not None and rtt < 180)
                    and drop_delta == 0
                    and (measured_fps == 0 or measured_fps >= settings["fps"] * 0.82)
                )
                changes = {}
                if poor:
                    if settings["fps"] > 5: changes["fps"] = settings["fps"] - 1
                    if settings["quality"] > 25: changes["quality"] = max(20, settings["quality"] - 8)
                elif good:
                    if settings["quality"] < 60: changes["quality"] = min(65, settings["quality"] + 2)
                    elif settings["fps"] < 60: changes["fps"] = settings["fps"] + 1
                if changes: await apply_settings(**changes)
                last_adapt = now
                prev_drops = client.screen_dropped_frames

            if last_frame is not None:
                canvas = last_frame.copy()
                rtt_label = "n/a" if rtt is None else f"~{rtt/2:.0f} ms"
                sound_label = " | SOUND" if audio_out is not None else ""
                status = (
                    f"FPS {measured_fps:.1f}/{settings['fps']} | latency {rtt_label} | "
                    f"{kb_per_sec/1024:.2f} MB/s | payload {session_bytes/(1024*1024):.2f} MB | "
                    f"drops {client.screen_dropped_frames} | Q={settings['quality']} "
                    f"{settings['resolution']}{sound_label}"
                )
                cv2.putText(canvas, status, (12, 28), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (20, 240, 20), 2, cv2.LINE_AA)
                if settings["paused"]:
                    cv2.putText(canvas, "PAUSED — press p to resume", (12, 58),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 170, 255), 2, cv2.LINE_AA)
                cv2.imshow(window, canvas)
            elif not stream_failed:
                placeholder = np.zeros((360, 640, 3), dtype=np.uint8)
                cv2.putText(placeholder, "Waiting for remote desktop...", (20, 180),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.65, (230, 230, 230), 1, cv2.LINE_AA)
                cv2.imshow(window, placeholder)

            key = cv2.waitKey(1) & 0xFF
            if key in (ord("q"), 27): break
            elif key == ord("p"): await apply_settings(paused=not settings["paused"])
            elif key in (ord("+"), ord("=")): await apply_settings(quality=min(90, settings["quality"] + 5))
            elif key == ord("-"): await apply_settings(quality=max(20, settings["quality"] - 5))
            elif key == ord("]"): await apply_settings(fps=min(60, settings["fps"] + 1))
            elif key == ord("["): await apply_settings(fps=max(5, settings["fps"] - 1))
            elif key == ord("1"): await apply_settings(resolution="480p")
            elif key == ord("2"): await apply_settings(resolution="720p")
            elif key in (ord("a"), ord("A")):
                settings["adaptive"] = not settings["adaptive"]
                print("تنظیم خودکار:", "روشن" if settings["adaptive"] else "خاموش")

            if stream_failed: break
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
        try: await probe_task
        except (asyncio.CancelledError, Exception): pass
        if audio_out is not None:
            try: audio_out.stop(); audio_out.close()
            except Exception: pass
        if with_sound:
            try: await client.request("sysaudio_stop", wait_time=4)
            except Exception: pass
        try: await client.request("screen_stop", wait_time=5)
        except Exception: pass
        cv2.destroyAllWindows()
        elapsed = max(0.1, time.monotonic() - started_at)
        total = client.screen_received_bytes - bytes_before
        print(f"نمایش زنده متوقف شد | مدت {elapsed:.1f}s | "
              f"{shown} فریم | افت {client.screen_dropped_frames} | "
              f"{(total/(1024*1024)):.2f} MB")


async def run_screenwithsound(client, arguments=None):
    await run_screen(client, arguments, with_sound=True)


async def run_screencam(client, arguments=None):
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
            screen_cfg["resolution"] = t; cam_cfg["resolution"] = t
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
        elif t in ("sound=on", "audio=on"): with_sound = True
        elif t in ("sound=off", "audio=off"): with_sound = False

    for q in (client.screen_frames, client.camera_frames, client.screen_events, client.camera_events):
        while not q.empty():
            try: q.get_nowait()
            except asyncio.QueueEmpty: break

    started = await client.request("screen_start", wait_time=15, **screen_cfg)
    if not started.get("ok"):
        print("شروع اسکرین ناموفق بود:", started.get("error", "نامشخص")); return
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
            s = await client.request("sysaudio_start", wait_time=12)
            if s.get("ok"):
                audio_out = sd.RawOutputStream(
                    samplerate=22050, channels=1, dtype="int16", blocksize=2205
                )
                audio_out.start()
                print("صدای سیستم فعال شد.")
            else:
                print("شروع صدا ناموفق بود:", s.get("error", "نامشخص"))
        except Exception as exc:
            print("خطا در صدا:", exc); audio_out = None

    window = f"Screen + Camera{' + Sound' if audio_out else ''} — q:stop p:pause a:mute"
    print("تصویر اسکرین + دوربین فعال شد. q توقف، p مکث اسکرین، a قطع/وصل صدا.")
    last_screen = None; last_cam = None; paused = False; audio_muted = False
    screen_frames_shown = 0; session_start = time.monotonic()
    try:
        while client.connected.is_set():
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

            try:
                packet = client.screen_frames.get_nowait()
                raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                if frame is not None:
                    last_screen = frame; screen_frames_shown += 1
            except (asyncio.QueueEmpty, ValueError, TypeError): pass
            try:
                packet = client.camera_frames.get_nowait()
                raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                if frame is not None: last_cam = frame
            except (asyncio.QueueEmpty, ValueError, TypeError): pass

            if audio_out is not None:
                if not audio_muted:
                    try: audio_out.write(client.sysaudio_chunks.get_nowait())
                    except (asyncio.QueueEmpty, Exception): pass
                else:
                    while True:
                        try: client.sysaudio_chunks.get_nowait()
                        except (asyncio.QueueEmpty, Exception): break

            if last_screen is not None:
                canvas = last_screen.copy()
                if last_cam is not None:
                    try:
                        pip_w = max(160, canvas.shape[1] // 4)
                        pip_h = max(120, int(last_cam.shape[0] * pip_w / last_cam.shape[1]))
                        pip = cv2.resize(last_cam, (pip_w, pip_h))
                        x = max(0, canvas.shape[1] - pip_w - 10); y = 10
                        if y + pip_h <= canvas.shape[0]:
                            canvas[y:y+pip_h, x:x+pip_w] = pip
                            cv2.rectangle(canvas, (x-1, y-1), (x+pip_w, y+pip_h), (0, 255, 0), 1)
                    except Exception: pass
                if paused:
                    cv2.putText(canvas, "PAUSED", (12, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 170, 255), 2, cv2.LINE_AA)
                if audio_muted:
                    cv2.putText(canvas, "MUTED", (12, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 80, 255), 2, cv2.LINE_AA)
                cv2.imshow(window, canvas)
            elif last_cam is not None:
                cv2.imshow(window, last_cam)

            key = cv2.waitKey(1) & 0xFF
            if key in (ord("q"), 27): break
            elif key == ord("p"):
                paused = not paused
                try: await client.request("screen_config", wait_time=5, paused=paused)
                except Exception: pass
            elif key in (ord("a"), ord("A")):
                if audio_out is not None:
                    audio_muted = not audio_muted
                    print("صدا:", "قطع" if audio_muted else "وصل")
            await asyncio.sleep(0.008)
    except KeyboardInterrupt: pass
    except Exception as exc: print("خطا:", exc)
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
        print("روی تهران نصب کن: py -m pip install opencv-python numpy"); return
    args = arguments or []
    opts = {"resolution": "480p", "fps": 15, "quality": 60, "device": 0}
    for token in args:
        if token.lower() in ("480p", "720p"): opts["resolution"] = token.lower()
        elif "=" in token:
            k, v = token.split("=", 1)
            if k in ("fps", "quality", "device"):
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
    print("دوربین زنده شروع شد؛ q برای توقف.")
    window = "Remote camera — q:stop"
    shown = 0; start = time.monotonic(); times = collections.deque(); last = None
    try:
        while client.connected.is_set():
            while not client.camera_events.empty():
                ev = client.camera_events.get_nowait()
                if ev.get("type") == "camera_error": print("خطای دوربین:", ev.get("error")); return
                if ev.get("type") == "camera_stopped": return
            try: packet = client.camera_frames.get_nowait()
            except asyncio.QueueEmpty: packet = None
            if packet:
                try:
                    raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                except Exception: frame = None
                if frame is not None:
                    last = frame; shown += 1; times.append(packet["received_at"])
            now = time.monotonic()
            while times and now - times[0] > 2: times.popleft()
            fps = (len(times)-1)/max(.1, times[-1]-times[0]) if len(times) > 1 else 0
            if last is not None:
                canvas = last.copy()
                cv2.putText(canvas, f"Camera FPS {fps:.1f} | {opts['resolution']} | q: stop",
                            (10, 25), cv2.FONT_HERSHEY_SIMPLEX, .6, (0, 255, 0), 2, cv2.LINE_AA)
                cv2.imshow(window, canvas)
            if (cv2.waitKey(1) & 0xFF) in (ord('q'), 27): break
            await asyncio.sleep(.005)
    except (OSError, WebSocketException, ConnectionError) as exc: print("دوربین قطع شد:", exc)
    finally:
        try: await client.request("camera_stop", wait_time=4)
        except Exception: pass
        cv2.destroyWindow(window)
        print(f"دوربین متوقف شد؛ {shown} فریم در {max(.1, time.monotonic()-start):.1f}s")


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
    print("صدای زنده فعال است؛ q برای توقف.")
    stopped = False; bytes_played = 0
    try:
        with sd.RawOutputStream(samplerate=16000, channels=1, dtype="int16", blocksize=1600) as stream:
            while client.connected.is_set():
                try:
                    import msvcrt
                    if msvcrt.kbhit() and msvcrt.getwch().lower() == "q": break
                except ImportError: pass
                try: chunk = await asyncio.wait_for(client.mic_chunks.get(), timeout=0.1)
                except asyncio.TimeoutError: continue
                stream.write(chunk); bytes_played += len(chunk)
                while not client.camera_events.empty():
                    ev = client.camera_events.get_nowait()
                    if ev.get("type") == "mic_error":
                        print("خطای میکروفون:", ev.get("error")); stopped = True; break
                    if ev.get("type") == "mic_stopped": stopped = True; break
                if stopped: break
    except KeyboardInterrupt: pass
    except Exception as exc: print("پخش صوت متوقف شد:", exc)
    finally:
        try: await client.request("mic_stop", wait_time=4)
        except Exception: pass
        print(f"میکروفون متوقف شد؛ {bytes_played/32000:.1f} ثانیه صوت پخش شد.")


async def run_av(client, arguments=None):
    try:
        import cv2
        import numpy as np
        import sounddevice as sd
    except ImportError:
        print("روی تهران نصب کن: py -m pip install opencv-python numpy sounddevice"); return
    opts = {"resolution": "480p", "fps": 15, "quality": 60, "device": 0}
    for token in (arguments or []):
        if token.lower() in ("480p", "720p"): opts["resolution"] = token.lower()
        elif "=" in token:
            k, v = token.split("=", 1)
            if k in ("fps", "quality", "device"):
                try: opts[k] = int(v)
                except ValueError: print(f"مقدار نامعتبر: {token}"); return
    for q in (client.camera_frames, client.mic_chunks, client.camera_events):
        while not q.empty():
            try: q.get_nowait()
            except asyncio.QueueEmpty: break
    cam = await client.request("camera_start", wait_time=12, **opts)
    if not cam.get("ok"):
        print("شروع دوربین ناموفق بود:", cam.get("error", "خطای نامشخص")); return
    mic = await client.request("mic_start", wait_time=12)
    if not mic.get("ok"):
        try: await client.request("camera_stop", wait_time=4)
        except Exception: pass
        print("شروع میکروفون ناموفق بود:", mic.get("error", "خطای نامشخص")); return
    print("دوربین + میکروفون فعال؛ q برای توقف هر دو.")
    window = "Camera + Microphone — q: stop both"
    last = None; shown = 0; bytes_played = 0; started = time.monotonic()
    try:
        with sd.RawOutputStream(samplerate=16000, channels=1, dtype="int16", blocksize=1600) as audio:
            while client.connected.is_set():
                while not client.camera_events.empty():
                    ev = client.camera_events.get_nowait()
                    if ev.get("type") in ("camera_error", "mic_error"):
                        print("خطای رسانه:", ev.get("error", ev.get("type"))); return
                try:
                    packet = client.camera_frames.get_nowait()
                    raw = base64.b64decode(packet["data"].get("image", ""), validate=True)
                    frame = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
                    if frame is not None: last = frame; shown += 1
                except (asyncio.QueueEmpty, Exception): pass
                try:
                    chunk = client.mic_chunks.get_nowait()
                    audio.write(chunk); bytes_played += len(chunk)
                except (asyncio.QueueEmpty, Exception): pass
                if last is not None:
                    canvas = last.copy()
                    cv2.putText(canvas, "Camera + Microphone ACTIVE | q: stop both",
                                (10, 25), cv2.FONT_HERSHEY_SIMPLEX, .6, (0, 255, 0), 2, cv2.LINE_AA)
                    cv2.imshow(window, canvas)
                if (cv2.waitKey(1) & 0xFF) in (ord("q"), 27): break
                await asyncio.sleep(.005)
    except Exception as exc: print("جریان رسانه متوقف شد:", exc)
    finally:
        for action in ("camera_stop", "mic_stop"):
            try: await client.request(action, wait_time=4)
            except Exception: pass
        try: cv2.destroyWindow(window)
        except Exception: pass
        print(f"متوقف شد | {shown} فریم | صوت {bytes_played/32000:.1f}s | "
              f"{time.monotonic()-started:.1f}s")


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
                    try: await client.close()
                    except Exception: pass
                    try:
                        client = RemoteClient(url)
                        await client.connect()
                    except Exception as exc:
                        print("اتصال مجدد ناموفق:", exc)

                elif cmd.startswith("!"):
                    shell_cmd = original[1:].strip()
                    if not shell_cmd:
                        print("مثال: !ls -la /tmp")
                    else:
                        await run_shell(client, shell_cmd)

                elif cmd not in KNOWN_COMMANDS:
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
                        print('مثال: get "D:\\POCO X3\\Camera\\video.mp4" [local_name] [--chunk=N]')
                        continue
                    chunk_size = DEFAULT_CHUNK_SIZE
                    local_name = None
                    for tok in tokens[2:]:
                        if tok.startswith("--chunk="):
                            try:
                                chunk_size = int(tok.split("=", 1)[1])
                            except ValueError:
                                print(f"مقدار --chunk نامعتبر: {tok}")
                        else:
                            local_name = tok
                    try:
                        ok, err = await download_file(
                            client, arg, local_name, chunk_size=chunk_size
                        )
                        if not ok and err and err != "cancelled":
                            print("خطا:", err)
                    except KeyboardInterrupt:
                        print("\nدانلود لغو شد توسط کاربر.")
                    except Exception as exc:
                        print(f"خطای دانلود: {type(exc).__name__}: {exc}")

                elif cmd == "cat":
                    if not arg:
                        print('مثال: cat "D:\\POCO X3\\notes.txt"')
                        continue
                    info = await client.request("info", arg)
                    if not info.get("ok"):
                        print("خطا:", info.get("error"))
                        continue
                    if info.get("type") != "file":
                        print("این مسیر فایل نیست.")
                        continue
                    size = info.get("size", 0)
                    if size > 200 * 1024:
                        print(f"فایل {size:,} بایت است؛ برای جلوگیری از بارگذاری زیاد، از get استفاده کن.")
                        continue
                    result = await client.request("get", arg, wait_time=CAT_TIMEOUT_SECONDS)
                    if not result.get("ok"):
                        print("خطا:", result.get("error"))
                        continue
                    data = base64.b64decode(result["data"], validate=True)
                    print(data.decode("utf-8", errors="replace"))

                elif cmd == "search":
                    if not arg or not extra:
                        print('مثال: search "D:\\POCO X3" VID')
                        continue
                    result = await client.request("search", arg, query=extra)
                    if result.get("ok"):
                        for item in result.get("items", []):
                            print(("[DIR] " if item.get("is_dir") else "      ") +
                                  item.get("path", item.get("name", "")))
                        if result.get("truncated"):
                            print("نتیجه‌ها محدود شده‌اند.")
                    else:
                        print("خطا:", result.get("error"))
                elif cmd == "ping":
                    started = time.monotonic()
                    result = await client.request("ping", ".")
                    elapsed = (time.monotonic() - started) * 1000
                    print(f"پاسخ عامل: {result.get('ok')} | RTT: {elapsed:.0f} ms")
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
