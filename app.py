#!/usr/bin/env python3
"""
Flask app for issue tracer: login, search API, and static SPA.
"""
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    request,
    send_from_directory,
    session,
    stream_with_context,
    url_for,
)

load_dotenv()

app = Flask(__name__, static_folder="static", static_url_path="")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-in-production")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

PROJECT_ROOT = Path(__file__).resolve().parent
TRACER_SCRIPT = PROJECT_ROOT / "graylog_tracer.py"
AI_RESULTS_DIR = PROJECT_ROOT / "ai-results"

# Date/time validation (Asia/Tehran semantics: YYYY-MM-DD, HH:MM or HH:MM:SS)
DATE_FMT = "%Y-%m-%d"
TIME_FMTS = ("%H:%M:%S", "%H:%M")


def _parse_dt(date_str: str, time_str: str) -> datetime | None:
    if not (date_str or "").strip() or not (time_str or "").strip():
        return None
    try:
        datetime.strptime((date_str or "").strip(), DATE_FMT)
    except ValueError:
        return None
    combined = f"{(date_str or '').strip()} {(time_str or '').strip()}"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(combined, fmt)
        except ValueError:
            continue
    return None


def _check_logged_in():
    if not session.get("logged_in"):
        return False
    return True


@app.route("/")
def index():
    if not _check_logged_in():
        return redirect(url_for("login_page"))
    return redirect(url_for("app_page"))


@app.route("/login")
def login_page():
    if _check_logged_in():
        next_url = request.args.get("next", "").strip()
        if next_url and next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("app_page"))
    return send_from_directory("static", "login.html")


@app.route("/app")
def app_page():
    if not _check_logged_in():
        next_url = request.full_path  # e.g. /app?start_date=...
        return redirect(url_for("login_page", next=next_url))
    return send_from_directory("static", "app.html")


ENV_FILE = PROJECT_ROOT / ".env"
# Keys whose values are never sent to the client; user can edit but not see current value
MASKED_KEYS = frozenset({"AI_API_KEY", "GRAYLOG_PASSWORD", "USER_PASSWORD"})


def _read_env_file() -> dict[str, str]:
    """Read .env file and return key/value dict. Preserves order via dict (Python 3.7+)."""
    out = {}
    if not ENV_FILE.is_file():
        return out
    raw = ENV_FILE.read_text(encoding="utf-8", errors="replace")
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, rest = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = rest.strip()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1].replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
        out[key] = value
    return out


def _write_env_file(env: dict[str, str]) -> None:
    """Write key/value dict to .env. Values with special chars are quoted."""
    def escape(v: str) -> str:
        if not v:
            return '""'
        if any(c in v for c in ('"', "\n", "\\", "#", " ")):
            return '"' + v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'
        return v

    lines = [f"{k}={escape(v)}" for k, v in env.items()]
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


@app.route("/admin")
def admin_page():
    if not _check_logged_in():
        return redirect(url_for("login_page", next="/admin"))
    return send_from_directory("static", "admin.html")


@app.route("/api/env", methods=["GET"])
def api_env_get():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        env = _read_env_file()
        # Return as list in .env file order; mask sensitive keys so value is never sent
        env_list = []
        for k, v in env.items():
            if k in MASKED_KEYS:
                env_list.append({"key": k, "value": "", "masked": True})
            else:
                env_list.append({"key": k, "value": v, "masked": False})
        return jsonify({"ok": True, "env": env_list})
    except OSError as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/env", methods=["POST"])
def api_env_save():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    env = data.get("env")
    current_env = _read_env_file()
    normalized = []  # list of (key, value) to preserve order
    if isinstance(env, list):
        for item in env:
            if not isinstance(item, dict):
                continue
            k = item.get("key") if "key" in item else item.get("name")
            v = item.get("value")
            if not isinstance(k, str) or not k.strip():
                continue
            key = k.strip()
            if "=" in key or "\n" in key:
                continue
            if key in MASKED_KEYS and (v is None or (isinstance(v, str) and v.strip() == "")):
                v = current_env.get(key, "")
            else:
                v = str(v) if v is not None else ""
            normalized.append((key, v))
    elif isinstance(env, dict):
        for k, v in env.items():
            if not isinstance(k, str) or not k.strip():
                continue
            key = k.strip()
            if "=" in key or "\n" in key:
                continue
            if key in MASKED_KEYS and (v is None or (isinstance(v, str) and v.strip() == "")):
                v = current_env.get(key, "")
            else:
                v = str(v) if v is not None else ""
            normalized.append((key, v))
    else:
        return jsonify({"ok": False, "error": "Missing or invalid 'env' (object or array)"}), 400
    if not normalized:
        return jsonify({"ok": False, "error": "No valid variables to save"}), 400
    try:
        _write_env_file(dict(normalized))
        load_dotenv(override=True)
        return jsonify({"ok": True})
    except OSError as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    expected_user = (os.getenv("USER_USERNAME") or "").strip()
    expected_pass = (os.getenv("USER_PASSWORD") or "").strip()
    if not expected_user or not expected_pass:
        return jsonify({"ok": False, "error": "Login not configured"}), 500
    if username == expected_user and password == expected_pass:
        session["logged_in"] = True
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Invalid username or password"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return redirect(url_for("login_page"), code=302)


def _parse_words_env(raw: str, default: list[str]) -> list[str]:
    if not (raw or "").strip():
        return default
    try:
        val = json.loads(raw)
        if isinstance(val, list) and all(isinstance(x, str) for x in val):
            return [x.strip() for x in val if x.strip()]
        return default
    except (json.JSONDecodeError, TypeError):
        return default


@app.route("/api/defaults")
def api_defaults():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    load_dotenv()
    default_error = ["error", "fail", "unknown", "not", "err", "exception", "eof", "crash", "fatal", "unexpected"]
    default_warning = ["warning"]
    default_special = []
    default_success = []
    return jsonify({
        "start_date": (os.getenv("START_DATE") or "").strip(),
        "start_time": (os.getenv("START_TIME") or "00:00").strip(),
        "end_date": (os.getenv("END_DATE") or "").strip(),
        "end_time": (os.getenv("END_TIME") or "23:59").strip(),
        "highlight_error_words": _parse_words_env(
            os.getenv("HIGHLIGHT_ERROR_WORDS"), default_error
        ),
        "highlight_warning_words": _parse_words_env(
            os.getenv("HIGHLIGHT_WARNING_WORDS"), default_warning
        ),
        "highlight_special_words": _parse_words_env(
            os.getenv("HIGHLIGHT_SPECIAL_WORDS"), default_special
        ),
        "highlight_success_words": _parse_words_env(
            os.getenv("HIGHLIGHT_SUCCESS_WORDS"), default_success
        ),
    })


@app.route("/api/search", methods=["POST"])
def api_search():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    start_date = (data.get("start_date") or "").strip()
    start_time = (data.get("start_time") or "").strip()
    end_date = (data.get("end_date") or "").strip()
    end_time = (data.get("end_time") or "").strip()

    from_dt = _parse_dt(start_date, start_time)
    to_dt = _parse_dt(end_date, end_time)
    if from_dt is None:
        return jsonify({"ok": False, "error": "Invalid start date/time. Use YYYY-MM-DD and HH:MM or HH:MM:SS."}), 400
    if to_dt is None:
        return jsonify({"ok": False, "error": "Invalid end date/time. Use YYYY-MM-DD and HH:MM or HH:MM:SS."}), 400
    if from_dt >= to_dt:
        return jsonify({"ok": False, "error": "Start must be before end."}), 400

    from_arg = f"{start_date} {start_time}"
    to_arg = f"{end_date} {end_time}"

    try:
        result = subprocess.run(
            [sys.executable, str(TRACER_SCRIPT), "--from", from_arg, "--to", to_arg],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "Search timed out."}), 504
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip() or f"Exit code {result.returncode}"
        return jsonify({"ok": False, "error": err}), 500

    try:
        out = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        return jsonify({"ok": False, "error": f"Invalid tracer output: {e}"}), 500

    return jsonify({"ok": True, "data": out})


def _safe_ai_result_key(key: str) -> str | None:
    """Allow only base64url-style filenames (A-Za-z0-9_-)."""
    if not key or len(key) > 256:
        return None
    if all(c.isalnum() or c in "-_" for c in key):
        return key
    return None


@app.route("/api/ai-result")
def api_ai_result_get():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    key = _safe_ai_result_key((request.args.get("key") or "").strip())
    if not key:
        return jsonify({"ok": False, "error": "Invalid key"}), 400
    path = AI_RESULTS_DIR / f"{key}.txt"
    if not path.is_file():
        return jsonify({"ok": False, "error": "Not found"}), 404
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return jsonify({"ok": False, "error": "Read failed"}), 500
    return jsonify({"ok": True, "content": content})


@app.route("/api/ask-ai", methods=["POST"])
def api_ask_ai():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    load_dotenv()
    host = (os.getenv("AI_HOST") or "").rstrip("/")
    api_key = (os.getenv("AI_API_KEY") or "").strip()
    model = (os.getenv("AI_MODEL") or "").strip()
    system_prompt = (os.getenv("AI_SYSTEM_PROMPT") or "").strip()
    if not host or not api_key or not model:
        return jsonify({"ok": False, "error": "AI_HOST, AI_API_KEY, and AI_MODEL must be set in .env"}), 500

    data = request.get_json() or {}
    content = (data.get("content") or "").strip()
    save_key = _safe_ai_result_key((data.get("key") or "").strip()) if data.get("key") else None
    if not content:
        return jsonify({"ok": False, "error": "Missing content"}), 400

    url = f"{host}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt or "Analyze this error and provide RCA."},
            {"role": "user", "content": content},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=120)
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": str(e)}), 502

    if resp.status_code != 200:
        return jsonify({"ok": False, "error": f"AI API error {resp.status_code}: {resp.text[:500]}"}), 502

    try:
        body = resp.json()
    except Exception:
        return jsonify({"ok": False, "error": "Invalid AI API response"}), 502

    choices = body.get("choices") or []
    if not choices:
        return jsonify({"ok": False, "error": "No response from AI"}), 502
    message = choices[0].get("message") or {}
    text = (message.get("content") or "").strip()

    if save_key and text:
        AI_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = AI_RESULTS_DIR / f"{save_key}.txt"
        try:
            out_path.write_text(text, encoding="utf-8")
        except OSError:
            pass

    return jsonify({"ok": True, "content": text})


@app.route("/api/ask-ai-stream", methods=["POST"])
def api_ask_ai_stream():
    if not _check_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    load_dotenv()
    host = (os.getenv("AI_HOST") or "").rstrip("/")
    api_key = (os.getenv("AI_API_KEY") or "").strip()
    model = (os.getenv("AI_MODEL") or "").strip()
    system_prompt = (os.getenv("AI_SYSTEM_PROMPT") or "").strip()
    if not host or not api_key or not model:
        return jsonify({"ok": False, "error": "AI_HOST, AI_API_KEY, and AI_MODEL must be set in .env"}), 500

    data = request.get_json() or {}
    content = (data.get("content") or "").strip()
    save_key = _safe_ai_result_key((data.get("key") or "").strip()) if data.get("key") else None
    if not content:
        return jsonify({"ok": False, "error": "Missing content"}), 400

    url = f"{host}/chat/completions"
    payload = {
        "model": model,
        "stream": True,
        "messages": [
            {"role": "system", "content": system_prompt or "Analyze this error and provide RCA."},
            {"role": "user", "content": content},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def generate():
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=120, stream=True)
        except requests.RequestException as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return
        if resp.status_code != 200:
            err_msg = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
            try:
                err_body = resp.json()
                err_msg = err_body.get("error", {}).get("message", err_msg) if isinstance(err_body.get("error"), dict) else err_body.get("error", err_msg)
            except Exception:
                pass
            yield f"data: {json.dumps({'error': err_msg})}\n\n"
            return
        full_text = []
        for line in resp.iter_lines():
            if line is None:
                break
            try:
                line_str = line.decode("utf-8", errors="replace").strip()
            except Exception:
                continue
            if not line_str or line_str == "data: [DONE]":
                continue
            if line_str.startswith("data: "):
                try:
                    chunk_data = json.loads(line_str[6:])
                    choices = chunk_data.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta") or {}
                    part = delta.get("content")
                    if part:
                        full_text.append(part)
                        yield f"data: {json.dumps({'content': part})}\n\n"
                except (json.JSONDecodeError, KeyError, IndexError):
                    pass
        if save_key and full_text:
            full_str = "".join(full_text).strip()
            if full_str:
                AI_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
                out_path = AI_RESULTS_DIR / f"{save_key}.txt"
                try:
                    out_path.write_text(full_str, encoding="utf-8")
                except OSError:
                    pass
        yield f"data: {json.dumps({'done': True})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
