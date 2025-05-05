#!/usr/bin/env python3
import os
import sys
import json
import tempfile
import datetime
import asyncio
import logging
import shutil
import base64
import time
from io import BytesIO

import pytz
import feedparser

from telegram import Bot, InputFile
from telegram.error import RetryAfter, NetworkError, TimedOut

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

# ==== LOGGER ====

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==== ENV & CONST ====

TOKEN             = os.environ.get("TELEGRAM_BOT_TOKEN")
GROUP_ID_STR      = os.environ.get("TELEGRAM_GROUP_ID")
YTDLP_COOKIES_B64 = os.environ.get("YTDLP_COOKIES_B64", "")
COOKIES_FILE      = "cookies.txt"
HISTORY_FILE      = os.getenv("HIST_FILE", "/data/history.json")

TIMEZONE          = pytz.timezone("Pacific/Kiritimati")
INTERVAL_SECONDS  = 15 * 60    # 15 minutes
MAX_RETRIES       = 3
RETRY_DELAY       = 10  # seconds

print(f"Using history file: {HISTORY_FILE}")

def exit_fatal(msg):
    logger.error(f"FATAL: {msg}")
    sys.exit(1)

if not TOKEN or not GROUP_ID_STR:
    exit_fatal("TELEGRAM_BOT_TOKEN or TELEGRAM_GROUP_ID missing")
try:
    GROUP_ID = int(GROUP_ID_STR)
except ValueError:
    exit_fatal("TELEGRAM_GROUP_ID is not numeric")

YOUTUBE_CHANNELS = [
   "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",
    "https://www.youtube.com/channel/UCV_CsAy5CNBX_uwDQ7RMe1Q",
    "https://www.youtube.com/channel/UC982yfxBCeh5WI9GRRlciww",
    "https://www.youtube.com/channel/UCZ0YtLAC8H_jzj_7DlUolRA",
    "https://www.youtube.com/channel/UCEaQBiiuwbn_UG64vCq04dA",
    "https://www.youtube.com/channel/UCnBeOXkvCydq1dY2XAOJ5nw",
    "https://www.youtube.com/channel/UCevzdl0zA0PecmG504ZMDLQ",
    "https://www.youtube.com/channel/UC07OXjeAKhswIPEnoDj8qRQ",
    "https://www.youtube.com/channel/UCVaWi8F2WIuV9Qk8ckvvlKg",
    "https://www.youtube.com/channel/UCcU8Xk_PAVc7meXeCO_3jSA",
    "https://www.youtube.com/channel/UCqz855ARgHtme-TxvHYQnCg",
    "https://www.youtube.com/channel/UCIvyL_xpsPJoBsQshhrD0uQ",
    "https://www.youtube.com/channel/UC0HVsMa3aau5tL3b4Cj3tHg",
    "https://www.youtube.com/channel/UCZYI04uqZ8zeh993rG3-3VA",
    "https://www.youtube.com/channel/UCyGtqW7TfOToaMY0A8GnXyA",
    "https://www.youtube.com/channel/UC7ZuMv7r60Cwn7mRNlPC0LA",
    "https://www.youtube.com/channel/UCtZbx-4oPQPt9UzFZ0svxbA",
    "https://www.youtube.com/channel/UC9MM5kyom9q_bgwgIG72aig",
    "https://www.youtube.com/channel/UC0X1a2gk3bq4v7j5r6x8Y9w",
    "https://www.youtube.com/channel/UC-GI5LST5T3Gw93yZxjdFaw",
    "https://www.youtube.com/channel/UCVrtt9YyQ7RaAe_cnn-bWWQ",
    "https://www.youtube.com/channel/UCGDawZyaXbMbcr15My67wmw",
    "https://www.youtube.com/channel/UCgvLPnUn2PfKsdbqGVZAMPw",
    "https://www.youtube.com/channel/UCyB6xP6_c6ZCqC2b4wYAsLw",
    "https://www.youtube.com/channel/UCCB1Byx5yTbLpQaV-rlfmtA",
    "https://www.youtube.com/channel/UCwKKwo3yWoVfPjlKN62KqQA",
    "https://www.youtube.com/channel/UCZU5ofyBsEmVuKYrijLFxrg",
    "https://www.youtube.com/channel/UCtAhIlz3P9mzJ0jWxaZ8RYA",
    "https://www.youtube.com/channel/UCL8aaObaUA14kpqkztGfBYA",
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",
    "https://www.youtube.com/channel/UCWcQRCPPW4qxa3OCUBBsuFw",
    "https://www.youtube.com/channel/UCtPSFgBQPsM7NW3iJpuqFuQ",
    "https://www.youtube.com/channel/UCmMHQBby2vt2Qd9StcmBXXQ",
    "https://www.youtube.com/channel/UC1Vribmny1eI62yYWux3rdQ",
    "https://www.youtube.com/channel/UC0XcAvzmW91qsWor3Qg6r3g",
    "https://www.youtube.com/channel/UCWWn6dtJhc5JcchjSzuEL8g",
    "https://www.youtube.com/channel/UC0XcAvzmW91qsWor3Qg6r3g",
    "https://www.youtube.com/channel/UCucdEPn-auvaUmBOJYuX7Og",
    "https://www.youtube.com/channel/UC7JpmOJscJcm-VsF6XnvYWg",
    "https://www.youtube.com/channel/UC8bEqtGJEUoYdWnti8k3R2Q",
    "https://www.youtube.com/channel/UC-bIUmH8gFA_lFkV2rJ6GMQ",
    "https://youtube.com/channel/UCCYT3uqgB08dh5mS3h421kA",
    "https://youtube.com/channel/UCLbji6FWNYfRiD9Un4v9jiA",
    "https://youtube.com/channel/UCOCMlsYEf9Bj70R_tXtvJEg",
    "https://youtube.com/channel/UCtPSFgBQPsM7NW3iJpuqFuQ"
]

# ==== Historique ====

def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        return {"ytm": []}

def save_history(hist):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

# ==== Utilitaire date/heure ====

def now_kiritimati():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(TIMEZONE)

# ==== YouTube ====

def list_new_youtube_videos(hist):
    new = []
    now_dt = now_kiritimati()
    for url in YOUTUBE_CHANNELS:
        cid = url.rstrip("/").split("/")[-1]
        feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}")
        if feed.bozo:
            # flux malformé → on ignore
            continue
        for e in feed.entries:
            vid = getattr(e, "yt_videoid", None)
            if not vid or vid in hist["ytm"]:
                continue
            if not e.get("published_parsed"):
                continue
            pub = datetime.datetime(*e.published_parsed[:6], tzinfo=pytz.utc).astimezone(TIMEZONE)
            # vidéo publiée dans la dernière semaine
            if 0 <= (now_dt - pub).total_seconds() < 7 * 24 * 3600:
                new.append((vid, e.link, e.title))
                logger.info(f"→ New video found: {e.title}")
    return new

def fetch_youtube_mp3(video_url):
    logger.info(f"Downloading YT audio: {video_url}")
    with tempfile.TemporaryDirectory() as td:
        opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(td, "%(id)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192"
            }],
            "ffmpeg_location": shutil.which("ffmpeg") or "ffmpeg",
            "retries": 3,
            "sleep_interval_requests": 5,
            "quiet": True,
            "no_warnings": True,
        }
        if os.path.isfile(COOKIES_FILE):
            opts["cookiefile"] = COOKIES_FILE

        with YoutubeDL(opts) as ydl:
            try:
                info = ydl.extract_info(video_url, download=False)
            except DownloadError as e:
                msg = str(e)
                if any(phrase in msg for phrase in (
                    "Premieres in", "HTTP Error 401",
                    "Sign in to confirm you're not a bot"
                )):
                    return None
                raise
            if (info.get("release_timestamp") or 0) > time.time():
                return None
            try:
                ydl.download([video_url])
            except DownloadError as e:
                msg = str(e)
                if any(phrase in msg for phrase in (
                    "Premieres in", "HTTP Error 401",
                    "Sign in to confirm you're not a bot"
                )):
                    return None
                raise

        files = [f for f in os.listdir(td) if f.endswith(".mp3")]
        if not files:
            return None
        return BytesIO(open(os.path.join(td, files[0]), "rb").read())

# ==== Envoi Telegram ====

bot = Bot(TOKEN)

async def send_audio(buf, title):
    fn = "".join(c if c.isalnum() or c in " *-" else "*" for c in title)[:60] + ".mp3"
    buf.name = fn
    buf.seek(0)
    for _ in range(MAX_RETRIES):
        try:
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(buf, filename=fn),
                caption=title,
                read_timeout=60, write_timeout=60, connect_timeout=30
            )
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut):
            await asyncio.sleep(RETRY_DELAY)
        finally:
            buf.seek(0)
    return False

# ==== Boucle principale ====

async def run_checks():
    if YTDLP_COOKIES_B64:
        with open(COOKIES_FILE, "wb") as f:
            f.write(base64.b64decode(YTDLP_COOKIES_B64))

    hist = load_history()

    # YouTube uniquement
    for vid, url, title in list_new_youtube_videos(hist):
        if vid not in hist["ytm"]:
            buf = fetch_youtube_mp3(url)
            if buf and await send_audio(buf, title):
                hist["ytm"].append(vid)
                save_history(hist)
            if buf:
                buf.close()
        await asyncio.sleep(3)

async def main():
    while True:
        logger.info("=== New check round ===")
        try:
            await run_checks()
        except Exception as e:
            logger.exception(f"Error during check: {e}")
        logger.info(f"Sleeping {INTERVAL_SECONDS//60} min")
        await asyncio.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
