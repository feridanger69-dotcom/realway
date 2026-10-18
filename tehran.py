"""Run locally in Tehran as a file-browser client, or on Railway with ROLE=server."""
import asyncio, base64, json, os, sys, uuid
from pathlib import Path
import websockets
from websockets.exceptions import ConnectionClosed

PAIR_CODE = "MyLaptop-7264-private"
RELAY_URL = os.getenv("RELAY_URL", "wss://YOUR-RAILWAY-DOMAIN/ws")
ROLE = os.getenv("ROLE", "client").lower()
MAX_FILE_BYTES = 20 * 1024 * 1024

if ROLE == "server":
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    import uvicorn
    app = FastAPI()
    peers = {"agent": None, "client": None}
    locks = {"agent": asyncio.Lock(), "client": asyncio.Lock()}

    @app.get("/")
    async def home():
        return {"status": "relay online", "usage": "WebSocket relay for the two-file Python remote file tool"}

    @app.websocket("/ws")
    async def relay(ws: WebSocket):
        await ws.accept()
        role = None
        try:
            hello = json.loads(await ws.receive_text())
            role = hello.get("role")
            if role not in peers or hello.get("code") != PAIR_CODE:
                await ws.close(code=1008, reason="Invalid pairing code or role")
                return
            async with locks[role]:
                old = peers[role]
                peers[role] = ws
                if old is not None:
                    try: await old.close(code=1012, reason="Reconnected elsewhere")
                    except Exception: pass
            while True:
                msg = await ws.receive_text()
                other_role = "client" if role == "agent" else "agent"
                other = peers[other_role]
                if other is not None:
                    await other.send_text(msg)
                elif role == "client":
                    try:
                        request_id = json.loads(msg).get("id", "")
                    except Exception:
                        request_id = ""
                    await ws.send_text(json.dumps({"id": request_id, "ok": False, "error": "Laptop in Kermanshah is offline"}))
        except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
            pass
        finally:
            if role and peers.get(role) is ws:
                peers[role] = None

    if __name__ == "__main__":
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
    raise SystemExit

async def request(ws, action, path=""):
    req_id = uuid.uuid4().hex
    await ws.send(json.dumps({"id": req_id, "action": action, "path": path}))
    while True:
        raw = await ws.recv()
        data = json.loads(raw)
        if data.get("id") == req_id:
            return data
        print("[پیام]", data.get("error", "پاسخ نامشخص"))

async def main():
    url = RELAY_URL.rstrip("/")
    if "YOUR-RAILWAY-DOMAIN" in url:
        print("اول RELAY_URL را با آدرس Railway خودت تنظیم کن.")
        return
    async with websockets.connect(url, max_size=MAX_FILE_BYTES * 2) as ws:
        await ws.send(json.dumps({"role": "client", "code": PAIR_CODE}))
        print("وصل شدی. دستورها: ls [مسیر] | get <مسیر فایل مقصد> | quit")
        while True:
            try: line = input("تهران> ").strip()
            except (EOFError, KeyboardInterrupt): break
            if line in ("quit", "exit", "q"): break
            if not line: continue
            parts = line.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""
            if cmd == "ls":
                result = await request(ws, "list", arg or ".")
                if result.get("ok"):
                    for item in result.get("items", []):
                        print(("[DIR] " if item["is_dir"] else "      ") + item["name"])
                else: print("خطا:", result.get("error"))
            elif cmd == "get":
                if not arg: print("مثال: get Documents/report.pdf"); continue
                result = await request(ws, "get", arg)
                if result.get("ok"):
                    name = Path(arg).name
                    dest = Path.cwd() / name
                    dest.write_bytes(base64.b64decode(result["data"]))
                    print("ذخیره شد:", dest)
                else: print("خطا:", result.get("error"))
            else:
                print("دستور نامعتبر. از ls یا get استفاده کن.")

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
