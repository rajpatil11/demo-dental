#!/usr/bin/env python3
"""
Send an HTML email via SMTP or SendGrid.
Usage: python tools/send_email.py --to email@example.com --subject "Subject" --html path/to/file.html

Required .env variables (choose one method):
  SMTP method:    EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, EMAIL_SMTP_USER, EMAIL_SMTP_PASS, EMAIL_FROM
  SendGrid method: SENDGRID_API_KEY, EMAIL_FROM
"""

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")


def send_via_smtp(to: str, subject: str, html_body: str) -> None:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    host = os.environ["EMAIL_SMTP_HOST"]
    port = int(os.environ.get("EMAIL_SMTP_PORT", 587))
    user = os.environ["EMAIL_SMTP_USER"]
    password = os.environ["EMAIL_SMTP_PASS"]
    from_addr = os.environ.get("EMAIL_FROM", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(host, port) as server:
        server.ehlo()
        server.starttls()
        server.login(user, password)
        server.sendmail(from_addr, [to], msg.as_string())
    print(f"[send_email] Sent via SMTP to {to}")


def send_via_sendgrid(to: str, subject: str, html_body: str) -> None:
    import urllib.request
    import json

    api_key = os.environ["SENDGRID_API_KEY"]
    from_addr = os.environ["EMAIL_FROM"]

    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status == 202:
            print(f"[send_email] Sent via SendGrid to {to}")
        else:
            raise RuntimeError(f"SendGrid error {resp.status}: {resp.read()}")


def main():
    parser = argparse.ArgumentParser(description="Send an HTML email")
    parser.add_argument("--to", required=True, help="Recipient email address")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--html", required=True, help="Path to HTML file")
    args = parser.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        print(f"Error: HTML file not found: {html_path}", file=sys.stderr)
        sys.exit(1)

    html_body = html_path.read_text(encoding="utf-8")

    if os.environ.get("SENDGRID_API_KEY"):
        send_via_sendgrid(args.to, args.subject, html_body)
    elif os.environ.get("EMAIL_SMTP_HOST"):
        send_via_smtp(args.to, args.subject, html_body)
    else:
        print(
            "Error: No email credentials found in .env\n"
            "Add either SENDGRID_API_KEY or EMAIL_SMTP_HOST/EMAIL_SMTP_USER/EMAIL_SMTP_PASS",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
