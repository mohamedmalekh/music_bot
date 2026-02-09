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

import requests

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
USER_AGENT        = os.environ.get("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
COOKIES_FILE      = "cookies.txt"
HISTORY_FILE      = os.getenv("HIST_FILE", "/data/history.json")

TIMEZONE          = pytz.timezone("Pacific/Kiritimati")
INTERVAL_SECONDS  = 2 * 60    # 2 minutes (faster for dedicated server)
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
    "https://youtube.com/channel/UCtPSFgBQPsM7NW3iJpuqFuQ",
    "https://youtube.com/channel/UCeBYRgPhy8kcRmIGQWKuqdQ",
    "https://youtube.com/channel/UC5IkSn-EFsUu3XANYklXc8g",
    "https://youtube.com/channel/UCanUjmTDohFr8OMpfk5xWBQ",
    "https://youtube.com/channel/UC3foXd7OMbut1i4zKDmLS5Q",
    "https://youtube.com/channel/UCQwOAHCemYyMN48LD1QE4VQ",
    "https://youtube.com/channel/UCGknrJk5kJypRpbWdqQOhXQ",
    "https://youtube.com/channel/UCFmW9YSajN0XrhLlHEN0OOA",
    "https://youtube.com/channel/UC1_liDR4fRFJgH4HoJeV8cw",
    "https://youtube.com/channel/UC5IkSn-EFsUu3XANYklXc8g",
    "https://youtube.com/channel/UCN6LpjCbqjY6OM9aqFEWXIQ",
    "https://youtube.com/channel/UCvoVZJeYGWVOzEHfDBiRr5g",
    "https://youtube.com/channel/UCL8aaObaUA14kpqkztGfBYA",
    "https://youtube.com/channel/UCZY-YIF6R9oHuk5YIjDkunA",
    "https://youtube.com/channel/UCoHMUugeU6PWB9ePTOV7WJw",
    "https://youtube.com/channel/UCuLNQOC5m2_aKf9_rXWmMFA",
    "https://youtube.com/channel/UCkD8FdHTwzIo2lJGoSeXSXQ",
    "https://youtube.com/channel/UC2ISePqOr39OQ90kNN1WLjA",
    "https://youtube.com/channel/UCSNL1Dz6CfYzmfXFpSG24Aw",
    "https://youtube.com/channel/UCl0tfz41M64qoo64WS_Ce7g",
    "https://youtube.com/channel/UCSx2kcfRgmDovMzbw3lfmAA",
    "https://youtube.com/channel/UC-r6UYEn_VJTcSeHLo9kUYQ",
    "https://youtube.com/channel/UCjfB7ooJY7C43vBAuuCub_A",
    "https://www.youtube.com/channel/UCu3fBixH_mo-2wNXHecQvSg",
    "https://music.youtube.com/channel/UCyjudLd_atOJpaPcaD4zTpw",
    "https://youtube.com/channel/UCnv05DfbpK8T79_W_LT84NQ",
"https://www.youtube.com/channel/UC4m5L8brApVSVe_AoD_Lw4w",
"https://www.youtube.com/channel/UCwKKwo3yWoVfPjlKN62KqQA",
"https://www.youtube.com/channel/UCU6cE7pdJPc6DU2jSrKEsdQ",
"https://www.youtube.com/channel/UCByOQJjav0CUDwxCk-jVNRQ",
"https://www.youtube.com/channel/UCJWZKkiJalectBzxwpHm1Yg",
"https://youtube.com/channel/UCerASQL8J6FAiSi-ivCLsWw",
"https://www.youtube.com/channel/UCFsI9RqD2JbWBlbPmYdCO-Q",
"https://youtube.com/channel/UC02NBzPa9c0-xkTSlIBIZXA",
"https://youtube.com/channel/UCVUSovN1Bqc9AlsypegNoQw",
    # ===== TUNISIE =====
    "https://www.youtube.com/channel/UCV9x1Bo83ByXbqJga1ZxaJg",  # Balti
    "https://www.youtube.com/channel/UCIY5O-yLf1sNyQ0AKXXQMjQ",  # Kafon
    "https://www.youtube.com/channel/UCI6ER2eyvJ098z_qrHZiZdA",  # Klay BBJ
    "https://www.youtube.com/channel/UCmi3NLdmjBRbzVqooFlHOkw",  # Hamzaoui Med Amine
    "https://www.youtube.com/channel/UCEFjoZ6dQajhDeLdqfb941Q",  # Akram Mag
    # ===== MAROC =====
    "https://www.youtube.com/channel/UCTdY7Fw0YMHCJ09jKTGy9Xw",  # 7liwa
    "https://www.youtube.com/channel/UCT1V9yrD20tpY39d-cnAU1g",  # Madd
    "https://www.youtube.com/channel/UCgckq95ZcUeCZWlY7UlxgWA",  # Inkonnu
    "https://www.youtube.com/channel/UCSj3fnfKHCI5G2VYUcx3DHg",  # Lbenj
    "https://www.youtube.com/channel/UC0k5ulMTxHlUzzJRnOtBRMA",  # Gnawi
    # ===== ALGERIE =====
    "https://www.youtube.com/channel/UCz6JjQtnK9XjMwKuqlEkRxw",  # Soolking
    "https://www.youtube.com/channel/UCzbsyyZ05INu78miPEAUDUQ",  # Didine Canon 16
    "https://www.youtube.com/channel/UCylPPp7MGoNmK6Gx9WBpjdA",  # Lacrim
    "https://www.youtube.com/channel/UCxdVRT5-dLcp4junZpcdLQg",  # Rim'k
    # ===== FRANCE =====
    "https://www.youtube.com/channel/UCNtD7oDVld6YeaYV7tCXo1A",  # Booba
    "https://www.youtube.com/channel/UCL6gZm742-Xhxp4Cwa6kqpw",  # PLK
    "https://www.youtube.com/channel/UC-69vhXlCa3XHbF8JHCQHfg",  # Aya Nakamura
    "https://www.youtube.com/channel/UC3nGUU-1Vmx17Oh7vVhC4Lg",  # SDM
    "https://www.youtube.com/channel/UC8pflMI7DubRMqdqP768p4w",  # Maes
    "https://www.youtube.com/channel/UCCSFseZ6DfCtVO8giGp7GfA",  # Koba LaD
    "https://www.youtube.com/channel/UCtjvAqI4o-aWtGDYFzkGvRw",  # Freeze Corleone
    "https://www.youtube.com/channel/UCeBu6AFsaPVElhFzlj5hetg",  # SCH
    "https://www.youtube.com/channel/UCIA9xFRtkDVBGH_BHvmDK6g",  # Werenoi
    "https://www.youtube.com/channel/UCBAkHJepWR2HTPE65iruihw",  # Ziak
    "https://www.youtube.com/channel/UCelI_bl2bVS1AFKaZEx44Vw",  # Kaaris
    "https://www.youtube.com/channel/UCdurFHA7O22wzSKpQCcW4eg",  # Laylow
    "https://www.youtube.com/channel/UCP9zhgBhlIwgF5UEf3Pmj8Q",  # Leto
    # ===== USA =====
    "https://www.youtube.com/channel/UCtxdfwb9wfkoGocVUAJ-Bmg",  # Travis Scott
    "https://www.youtube.com/channel/UCOjEHmBKwdS7joWpW0VrXkg",  # 21 Savage
    "https://www.youtube.com/channel/UCKC11MOR51CLg4JpYj8jb4g",  # Metro Boomin
    "https://www.youtube.com/channel/UC3lBXcrKFnFAFkfVk5WuKcQ",  # Kendrick Lamar
    "https://www.youtube.com/channel/UCnc6db-y3IU7CkT_yeVXdVg",  # J. Cole
    "https://www.youtube.com/channel/UCVS88tG_NYgxF6Udnx2815Q",  # Lil Baby
    "https://www.youtube.com/channel/UCAkIMkEaa9sZmjcy7mfd5lQ",  # Gunna
    "https://www.youtube.com/channel/UC652oRUvX1onwrrZ8ADJRPw",  # Playboi Carti
    "https://www.youtube.com/channel/UCV4UK9LNNLViFP4qZA_Wmfw",  # Yeat
    "https://www.youtube.com/channel/UCq0Hi7HpCBCNeKpdKKcQqGQ",  # Baby Keem
    # ===== ESPAGNE / LATIN =====
    "https://www.youtube.com/channel/UCmBA_wu8xGg1OfOkfW13Q0Q",  # Bad Bunny
    "https://www.youtube.com/channel/UCLk8IJ1TwI7Xl7UUfAD8xPQ",  # Myke Towers
    "https://www.youtube.com/channel/UCRI7hheejBbWS6etTNwMT0g",  # Anuel AA
    "https://www.youtube.com/channel/UC_Av98lDjf5KvFib5elhpYg",  # Rauw Alejandro
    "https://www.youtube.com/channel/UCjIA3wwhi0QjSOXAZwOXbPA",  # Ozuna
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
            # vidéo publiée dans les dernières 48h (plus réactif)
            if 0 <= (now_dt - pub).total_seconds() < 2 * 24 * 3600:
                new.append((vid, e.link, e.title))
                logger.info(f"→ New video found: {e.title}")
    return new

def get_po_token():
    """
    Attempt to extract a PO token from the cookies file
    """
    if not os.path.isfile(COOKIES_FILE):
        return None
        
    try:
        with open(COOKIES_FILE, 'r') as f:
            for line in f:
                if 'PREF' in line and 'po=' in line:
                    parts = line.split()
                    for part in parts:
                        if part.startswith('po='):
                            return part.split('=')[1].strip()
    except Exception as e:
        logger.error(f"Error extracting PO token: {e}")
    return None

def fetch_youtube_mp3(video_url):
    """Download YouTube audio using Cobalt API (works from datacenters)"""
    import requests
    
    # Skip YouTube Shorts
    if '/shorts/' in video_url:
        logger.info(f"Skipping YouTube Short: {video_url}")
        return None
        
    logger.info(f"Downloading via Cobalt API: {video_url}")
    
    # Cobalt API endpoint
    COBALT_API = "https://api.cobalt.tools/api/json"
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    
    payload = {
        "url": video_url,
        "aFormat": "mp3",
        "isAudioOnly": True,
        "audioBitrate": "192",
    }
    
    try:
        # Request download URL from Cobalt
        response = requests.post(COBALT_API, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "error":
            logger.warning(f"Cobalt error for {video_url}: {data.get('text', 'Unknown error')}")
            return None
        
        # Get the audio URL
        audio_url = data.get("url")
        if not audio_url:
            logger.warning(f"No audio URL in Cobalt response for {video_url}")
            return None
        
        logger.info(f"Got audio URL from Cobalt, downloading...")
        
        # Download the audio file
        audio_response = requests.get(audio_url, timeout=120)
        audio_response.raise_for_status()
        
        logger.info(f"Downloaded {len(audio_response.content) / 1024 / 1024:.2f} MB")
        return BytesIO(audio_response.content)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Cobalt API error for {video_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Download error for {video_url}: {e}")
        return None

# ==== Envoi Telegram ====

bot = Bot(TOKEN)

async def send_audio(buf, title):
    if not buf:
        logger.warning(f"Cannot send empty audio buffer for: {title}")
        return False
        
    fn = "".join(c if c.isalnum() or c in " *-" else "*" for c in title)[:60] + ".mp3"
    buf.name = fn
    buf.seek(0)
    
    for attempt in range(MAX_RETRIES):
        try:
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(buf, filename=fn),
                caption=title,
                read_timeout=60, write_timeout=60, connect_timeout=30
            )
            logger.info(f"Successfully sent to Telegram: {title}")
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited by Telegram, waiting {e.retry_after}s: {title}")
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut):
            logger.warning(f"Network error, retrying after {RETRY_DELAY}s (attempt {attempt+1}/{MAX_RETRIES}): {title}")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Error sending to Telegram: {e} - {title}")
            await asyncio.sleep(RETRY_DELAY)
        finally:
            buf.seek(0)
    
    logger.error(f"Failed to send to Telegram after {MAX_RETRIES} attempts: {title}")
    return False

# ==== Boucle principale ====

async def run_checks():
    if YTDLP_COOKIES_B64:
        try:
            with open(COOKIES_FILE, "wb") as f:
                f.write(base64.b64decode(YTDLP_COOKIES_B64))
            logger.info("Successfully loaded cookies from environment variable")
        except Exception as e:
            logger.error(f"Failed to decode cookies: {e}")

    hist = load_history()

    # YouTube uniquement
    new_videos = list_new_youtube_videos(hist)
    logger.info(f"Found {len(new_videos)} new videos to process")
    
    for vid, url, title in new_videos:
        if vid not in hist["ytm"]:
            try:
                buf = fetch_youtube_mp3(url)
                if buf and await send_audio(buf, title):
                    hist["ytm"].append(vid)
                    save_history(hist)
                    logger.info(f"Successfully processed and added to history: {title}")
                else:
                    logger.warning(f"Could not process video: {title}")
                if buf:
                    buf.close()
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
            # Add delay between downloads to avoid rate limits
            await asyncio.sleep(3)

async def main():
    """Run bot continuously with interval"""
    logger.info("=== Bot started (continuous mode) ===")
    while True:
        try:
            await run_checks()
            logger.info(f"Sleeping for {INTERVAL_SECONDS} seconds...")
            await asyncio.sleep(INTERVAL_SECONDS)
        except Exception as e:
            logger.exception(f"Error in run_checks: {e}")
            await asyncio.sleep(60)  # Wait 1 min on error

if __name__ == "__main__":
    # Check if running on GitHub Actions (single run) or server (continuous)
    if os.environ.get("GITHUB_ACTIONS"):
        logger.info("=== Running single check (GitHub Actions) ===")
        try:
            asyncio.run(run_checks())
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected, exiting...")
        except Exception as e:
            logger.exception(f"Fatal error: {e}")
            sys.exit(1)
        logger.info("=== Check complete ===")
    else:
        # Continuous mode for Railway/Render
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
