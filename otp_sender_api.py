from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel


def _load_dotenv_file(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                os.environ[key] = value.strip().strip('"').strip("'")
    except Exception:
        return


_load_dotenv_file(os.path.join(os.path.dirname(__file__), ".env"))
_load_dotenv_file(os.path.join(os.path.dirname(__file__), "otp_sender.env"))


class OtpMailRequest(BaseModel):
    token: str = ""
    to_email: str
    subject: str
    text_body: str
    html_body: str = ""


app = FastAPI(title="Tool Evidence OTP Sender", version="1.0.0")


def _bridge_token() -> str:
    return str(os.getenv("OTP_BRIDGE_TOKEN", "")).strip()


def _smtp_config() -> dict[str, str | int | bool]:
    gmail_email = str(os.getenv("GMAIL_SMTP_EMAIL", "") or os.getenv("GMAIL_EMAIL", "")).strip()
    gmail_password = str(os.getenv("GMAIL_SMTP_APP_PASSWORD", "") or os.getenv("GMAIL_APP_PASSWORD", "")).strip().replace(" ", "")
    gmail_from = str(os.getenv("GMAIL_SMTP_FROM_EMAIL", "")).strip() or gmail_email
    if gmail_email and gmail_password:
        return {
            "host": "smtp.gmail.com",
            "port": 587,
            "username": gmail_email,
            "password": gmail_password,
            "from_email": gmail_from,
            "use_ssl": False,
            "use_tls": True,
        }
    raise HTTPException(status_code=500, detail="Thiếu GMAIL_SMTP_EMAIL hoặc GMAIL_SMTP_APP_PASSWORD")


def _send_via_smtp(to_email: str, subject: str, text_body: str, html_body: str = "") -> None:
    config = _smtp_config()
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(("Evidence Security", str(config["from_email"])))
    msg["To"] = to_email
    msg.set_content(text_body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")
    context = ssl.create_default_context()
    if bool(config["use_ssl"]):
        with smtplib.SMTP_SSL(str(config["host"]), int(config["port"]), timeout=20, context=context) as server:
            if config["username"]:
                server.login(str(config["username"]), str(config["password"]))
            server.send_message(msg)
    else:
        with smtplib.SMTP(str(config["host"]), int(config["port"]), timeout=20) as server:
            server.ehlo()
            if bool(config["use_tls"]):
                server.starttls(context=context)
                server.ehlo()
            if config["username"]:
                server.login(str(config["username"]), str(config["password"]))
            server.send_message(msg)


@app.get("/health")
def health():
    return {"ok": True, "service": "otp-sender"}


@app.post("/send-otp")
def send_otp(payload: OtpMailRequest, x_bridge_token: str = Header(default="")):
    expected = _bridge_token()
    provided = str(payload.token or x_bridge_token or "").strip()
    if expected and provided != expected:
        raise HTTPException(status_code=401, detail="OTP bridge token không hợp lệ")
    _send_via_smtp(
        to_email=str(payload.to_email or "").strip(),
        subject=str(payload.subject or "").strip(),
        text_body=str(payload.text_body or ""),
        html_body=str(payload.html_body or ""),
    )
    return {"ok": True}
