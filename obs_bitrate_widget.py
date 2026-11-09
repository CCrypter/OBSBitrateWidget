# -*- coding: utf-8 -*-
"""
OBS Bitrate Widget
Небольшое окно поверх всех окон, показывающее текущий битрейт стрима в OBS Studio.
Подключение через OBS WebSocket (v5, встроен в OBS 28+).

Зависимости:  pip install websocket-client
Сборка в exe: pip install pyinstaller
              pyinstaller --onefile --noconsole obs_bitrate_widget.py
"""

import base64
import hashlib
import json
import os
import sys
import threading
import time
import tkinter as tk
import uuid
from collections import deque

import websocket  # пакет websocket-client


# --------------------------------------------------------------------------
# Конфиг (config.json лежит рядом со скриптом / exe)
# --------------------------------------------------------------------------

def app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


CONFIG_PATH = os.path.join(app_dir(), "config.json")

DEFAULT_CONFIG = {
    "host": "localhost",
    "port": 4455,
    "password": "",
    "poll_interval_sec": 1.0,   # как часто опрашивать OBS
    "smoothing_samples": 3,     # сколько замеров усреднять
    "target_kbps": 0,           # целевой битрейт (0 = не использовать для цвета)
    "hide_when_idle": False,    # прятать виджет, когда нет трансляции
    "opacity": 0.85,
    "x": 20,
    "y": 20,
    "locked": False,
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    else:
        save_config(cfg)
    return cfg


def save_config(cfg):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=4)
    except Exception:
        pass


# --------------------------------------------------------------------------
# Клиент OBS WebSocket (отдельный поток)
# --------------------------------------------------------------------------

class ObsClient(threading.Thread):
    def __init__(self, cfg, state, lock):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.state = state
        self.lock = lock
        self.stop_event = threading.Event()

    def set_state(self, **kwargs):
        with self.lock:
            self.state.update(kwargs)

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.session()
            except Exception as e:
                self.set_state(
                    connected=False,
                    streaming=False,
                    kbps=None,
                    message=self.last_error or "Нет связи с OBS",
                )
            self.stop_event.wait(2.0)

    # -- один сеанс подключения ------------------------------------------
    last_error = ""

    def session(self):
        self.last_error = ""
        url = "ws://{}:{}".format(self.cfg["host"], self.cfg["port"])
        self.set_state(connected=False, streaming=False, kbps=None, message="Подключение…")

        try:
            ws = websocket.create_connection(url, timeout=5)
        except Exception:
            self.last_error = "OBS не найден (WebSocket выключен?)"
            raise

        identified = False
        try:
            hello = json.loads(ws.recv())
            ident = {"rpcVersion": 1, "eventSubscriptions": 0}
            auth = hello.get("d", {}).get("authentication")
            if auth:
                password = self.cfg.get("password", "")
                secret = base64.b64encode(
                    hashlib.sha256((password + auth["salt"]).encode("utf-8")).digest()
                ).decode("utf-8")
                ident["authentication"] = base64.b64encode(
                    hashlib.sha256((secret + auth["challenge"]).encode("utf-8")).digest()
                ).decode("utf-8")

            ws.send(json.dumps({"op": 1, "d": ident}))
            try:
                reply = json.loads(ws.recv())
            except Exception:
                self.last_error = "Ошибка входа (проверьте пароль)"
                raise
            if reply.get("op") != 2:
                self.last_error = "Ошибка входа (проверьте пароль)"
                raise RuntimeError("identify failed")
            identified = True

            prev_bytes = None
            prev_time = None
            samples = deque(maxlen=max(1, int(self.cfg["smoothing_samples"])))

            while not self.stop_event.is_set():
                data = self.request(ws, "GetStreamStatus")
                now = time.monotonic()

                if not data.get("outputActive"):
                    prev_bytes, prev_time = None, None
                    samples.clear()
                    self.set_state(
                        connected=True, streaming=False, kbps=None,
                        congestion=0.0, dropped_pct=0.0, message="Не в эфире",
                    )
                else:
                    cur_bytes = data.get("outputBytes", 0)
                    kbps = None
                    if prev_bytes is not None and cur_bytes >= prev_bytes and now > prev_time:
                        kbps = (cur_bytes - prev_bytes) * 8.0 / 1000.0 / (now - prev_time)
                        samples.append(kbps)
                        kbps = sum(samples) / len(samples)
                    prev_bytes, prev_time = cur_bytes, now

                    total = data.get("outputTotalFrames", 0) or 0
                    skipped = data.get("outputSkippedFrames", 0) or 0
                    dropped_pct = (skipped / total * 100.0) if total else 0.0

                    self.set_state(
                        connected=True, streaming=True, kbps=kbps,
                        congestion=float(data.get("outputCongestion", 0.0) or 0.0),
                        dropped_pct=dropped_pct,
                        message="В эфире",
                    )

                self.stop_event.wait(float(self.cfg["poll_interval_sec"]))
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def request(self, ws, request_type):
        req_id = uuid.uuid4().hex
        ws.send(json.dumps({
            "op": 6,
            "d": {"requestType": request_type, "requestId": req_id},
        }))
        while True:
            msg = json.loads(ws.recv())
            if msg.get("op") == 7 and msg["d"].get("requestId") == req_id:
                if not msg["d"]["requestStatus"].get("result"):
                    raise RuntimeError("request failed")
                return msg["d"].get("responseData", {}) or {}


# --------------------------------------------------------------------------
# Виджет
# --------------------------------------------------------------------------

BG = "#15171c"
FG_OK = "#4cd964"
FG_WARN = "#ffcc00"
FG_BAD = "#ff453a"
FG_IDLE = "#8a8f98"


class Widget:
    def __init__(self):
        self.cfg = load_config()
        self.lock = threading.Lock()
        self.state = {
            "connected": False, "streaming": False, "kbps": None,
            "congestion": 0.0, "dropped_pct": 0.0, "message": "Подключение…",
        }

        self.root = tk.Tk()
        self.root.title("OBS Bitrate")
        self.root.overrideredirect(True)
        self.root.attributes("-topmost", True)
        self.root.attributes("-alpha", float(self.cfg["opacity"]))
        self.root.configure(bg=BG)
        self.root.geometry("+{}+{}".format(int(self.cfg["x"]), int(self.cfg["y"])))

        frame = tk.Frame(self.root, bg=BG, padx=14, pady=8)
        frame.pack()

        self.main_label = tk.Label(frame, text="— kbps", bg=BG, fg=FG_IDLE,
                                   font=("Segoe UI", 20, "bold"), width=11)
        self.main_label.pack()
        self.sub_label = tk.Label(frame, text="Подключение…", bg=BG, fg=FG_IDLE,
                                  font=("Segoe UI", 9))
        self.sub_label.pack()

        self.locked_var = tk.BooleanVar(value=bool(self.cfg["locked"]))
        self.hide_var = tk.BooleanVar(value=bool(self.cfg["hide_when_idle"]))

        self.menu = tk.Menu(self.root, tearoff=0)
        self.menu.add_checkbutton(label="Закрепить позицию", variable=self.locked_var,
                                  command=self.on_lock_toggle)
        self.menu.add_checkbutton(label="Скрывать, когда нет эфира", variable=self.hide_var,
                                  command=self.on_hide_toggle)
        self.menu.add_separator()
        self.menu.add_command(label="Выход", command=self.quit)

        for w in (self.root, frame, self.main_label, self.sub_label):
            w.bind("<ButtonPress-1>", self.drag_start)
            w.bind("<B1-Motion>", self.drag_move)
            w.bind("<ButtonRelease-1>", self.drag_end)
            w.bind("<Button-3>", self.show_menu)

        self._drag_dx = 0
        self._drag_dy = 0

        self.client = ObsClient(self.cfg, self.state, self.lock)
        self.client.start()

        self.refresh()
        self.keep_on_top()

    # -- перетаскивание ---------------------------------------------------
    def drag_start(self, e):
        if self.locked_var.get():
            return
        self._drag_dx = e.x_root - self.root.winfo_x()
        self._drag_dy = e.y_root - self.root.winfo_y()

    def drag_move(self, e):
        if self.locked_var.get():
            return
        self.root.geometry("+{}+{}".format(e.x_root - self._drag_dx, e.y_root - self._drag_dy))

    def drag_end(self, e):
        if self.locked_var.get():
            return
        self.cfg["x"] = self.root.winfo_x()
        self.cfg["y"] = self.root.winfo_y()
        save_config(self.cfg)

    # -- меню -------------------------------------------------------------
    def show_menu(self, e):
        try:
            self.menu.tk_popup(e.x_root, e.y_root)
        finally:
            self.menu.grab_release()

    def on_lock_toggle(self):
        self.cfg["locked"] = bool(self.locked_var.get())
        save_config(self.cfg)

    def on_hide_toggle(self):
        self.cfg["hide_when_idle"] = bool(self.hide_var.get())
        save_config(self.cfg)

    def quit(self):
        self.client.stop_event.set()
        self.root.destroy()

    # -- обновление -------------------------------------------------------
    def pick_color(self, kbps, congestion):
        color = FG_OK
        target = float(self.cfg.get("target_kbps") or 0)
        if target > 0 and kbps is not None:
            ratio = kbps / target
            if ratio < 0.6:
                color = FG_BAD
            elif ratio < 0.85:
                color = FG_WARN
        if congestion > 0.7:
            color = FG_BAD
        elif congestion > 0.3 and color == FG_OK:
            color = FG_WARN
        return color

    def refresh(self):
        with self.lock:
            s = dict(self.state)

        if s["streaming"]:
            if s["kbps"] is None:
                self.main_label.config(text="… kbps", fg=FG_IDLE)
            else:
                self.main_label.config(
                    text="{:,.0f} kbps".format(s["kbps"]).replace(",", " "),
                    fg=self.pick_color(s["kbps"], s["congestion"]),
                )
            self.sub_label.config(
                text="● LIVE · потеряно кадров {:.1f}%".format(s["dropped_pct"]),
                fg=self.pick_color(s["kbps"], s["congestion"]),
            )
        else:
            self.main_label.config(text="— kbps", fg=FG_IDLE)
            self.sub_label.config(text=s["message"], fg=FG_IDLE)

        # автоскрытие, когда нет эфира
        hide = self.cfg.get("hide_when_idle") and not s["streaming"]
        if hide and self.root.state() != "withdrawn":
            self.root.withdraw()
        elif not hide and self.root.state() == "withdrawn":
            self.root.deiconify()

        self.root.after(250, self.refresh)

    def keep_on_top(self):
        # периодически «подтверждаем» topmost, чтобы другие окна не перекрывали виджет
        try:
            if self.root.state() != "withdrawn":
                self.root.attributes("-topmost", True)
                self.root.lift()
        except tk.TclError:
            return
        self.root.after(2000, self.keep_on_top)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    Widget().run()
