import json
import os
import sys

import requests
from dotenv import load_dotenv


def str2bool(s):
    return s.lower() in ("true", "1", "yes", "y")


def send_teams_message(webhook_url: str, title: str, message: str, is_success: bool):
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium", "color": ("good" if is_success else "attention")},
                        {"type": "TextBlock", "text": message, "wrap": True},
                    ],
                },
            }
        ],
    }
    response = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
    response.raise_for_status()


if __name__ == "__main__":
    load_dotenv()
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")

    # Usage: uv run notify_teams.py "Title" "Message"
    title = sys.argv[1]
    message = sys.argv[2]
    status = str2bool(sys.argv[3])

    send_teams_message(webhook_url, title, message, status)
