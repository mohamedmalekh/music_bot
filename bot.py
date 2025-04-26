#!/usr/bin/env python3
# main_bot.py

import os
import sys
import json
import subprocess
import tempfile
import datetime
import asyncio
import logging
from io import BytesIO

import pytz
import feedparser
from urllib.parse import urlparse, parse_qs

from telegram import Bot, InputFile
from telegram.error import RetryAfter, NetworkError, TimedOut

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotdl import Spotdl

from yt_dlp import YoutubeDL

# ==== LOGGING CONFIGURATION ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==== CONSTANTS & CONFIGURATION ====
TOKEN            = os.environ.get("TELEGRAM_BOT_TOKEN")
GROUP_ID_STR     = os.environ.get("TELEGRAM_GROUP_ID")
SPOTIFY_ID       = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET   = os.environ.get("SPOTIFY_CLIENT_SECRET")

if not TOKEN:
    logger.error("FATAL: TELEGRAM_BOT_TOKEN not set")
    sys.exit(1)
if not GROUP_ID_STR:
    logger.error("FATAL: TELEGRAM_GROUP_ID not set")
    sys.exit(1)
try:
    GROUP_ID = int(GROUP_ID_STR)
except ValueError:
    logger.error(f"FATAL: TELEGRAM_GROUP_ID ('{GROUP_ID_STR}') is not an integer")
    sys.exit(1)

if not SPOTIFY_ID or not SPOTIFY_SECRET:
    logger.warning("Spotify credentials missing; Spotify functionality may fail")

YOUTUBE_CHANS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",
    "https://www.youtube.com/@Mootjeyek",
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",
    "https://www.youtube.com/@M.M.Hofficial"
]

SPOTIFY_ARTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"
]

KIRI_TZ     = pytz.timezone("Pacific/Kiritimati")
HIST_FILE   = "processed.json"
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds

# ==== HISTORY HANDLING ====
def load_history():
    try:
        with open(HIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "ytm": data.get("ytm", []),
            "spotify": data.get("spotify", [])
        }
    except Exception:
        return {"ytm": [], "spotify": []}

processed = load_history()

def save_history():
    try:
        with open(HIST_FILE, "w", encoding="utf-8") as f:
            json.dump(processed, f, indent=2)
        logger.debug("History saved")
    except Exception as e:
        logger.error(f"Failed to save history: {e}")

def now_kiri():
    return datetime.datetime.now(pytz.utc).astimezone(KIRI_TZ)

# ==== YOUTUBE VIA RSS ====
def list_new_videos(channel_url):
    logger.info(f"Checking YouTube RSS: {channel_url}")
    # Determine channel ID
    if "/channel/" in channel_url:
        channel_id = channel_url.rstrip("/").split("/")[-1]
    else:
        logger.error(f"Cannot parse channel_id from URL: {channel_url}")
        return []

    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    feed = feedparser.parse(feed_url)
    if feed.bozo:
        logger.error(f"RSS parse error for {feed_url}: {feed.bozo_exception}")
        return []

    new_entries = []
    now_dt = now_kiri()
    for entry in feed.entries:
        # published_parsed is UTC
        pub = datetime.datetime(*entry.published_parsed[:6], tzinfo=pytz.utc)
        pub_kiri = pub.astimezone(KIRI_TZ)
        vid_id = parse_qs(urlparse(entry.link).query).get("v", [""])[0]
        if not vid_id or vid_id in processed["ytm"]:
            continue
        delta = now_dt - pub_kiri
        if 0 <= delta.total_seconds() < datetime.timedelta(days=7).total_seconds():
            new_entries.append((vid_id, entry.link, entry.title))
            logger.info(f"Found new video: {entry.title} ({vid_id}), published {delta}")
    return new_entries

def fetch_ytm_mp3(url):
    logger.info(f"Downloading YouTube audio: {url}")
    with tempfile.TemporaryDirectory() as tmpdir:
        outtpl = os.path.join(tmpdir, "audio.%(ext)s")
        ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "outtmpl": outtpl,
            "quiet": True,
            "no_warnings": True,
        }
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        # find mp3
        mp3_files = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
        if not mp3_files:
            raise FileNotFoundError("No MP3 file found after yt-dlp")
        path = os.path.join(tmpdir, mp3_files[0])
        return BytesIO(open(path, "rb").read())

# ==== SPOTIFY ====
sp = None
spdl = None
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))
    spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, headless=True)
    logger.info("Initialized Spotify clients")
except Exception as e:
    logger.warning(f"Spotify init failed: {e}")

def list_new_spotify(artist_url):
    if not sp:
        return []
    logger.info(f"Checking Spotify artist: {artist_url}")
    artist_id = artist_url.rstrip("/").split("/")[-1].split("?")[0]
    now_dt = now_kiri()
    new_tracks = []
    try:
        results = sp.artist_albums(artist_id, album_type="album,single", country="US", limit=30)
        for alb in results.get("items", []):
            rd = alb.get("release_date")
            precision = alb.get("release_date_precision", "day")
            try:
                if precision == "day":
                    d = datetime.datetime.fromisoformat(rd)
                elif precision == "month":
                    d = datetime.datetime.strptime(rd, "%Y-%m")
                else:
                    d = datetime.datetime.strptime(rd, "%Y")
                pub_kiri = KIRI_TZ.localize(d)
            except Exception:
                continue
            delta = now_dt - pub_kiri
            if 0 <= delta.total_seconds() < datetime.timedelta(days=7).total_seconds():
                tracks = sp.album_tracks(alb["id"]).get("items", [])
                for tr in tracks:
                    tid = tr["id"]
                    if tid in processed["spotify"]:
                        continue
                    title = f\"{', '.join(a['name'] for a in tr['artists'])} - {tr['name']}\"
                    new_tracks.append((tid, tr["external_urls"]["spotify"], title))
                    logger.info(f"Found new Spotify track: {title} ({tid}), published {delta}")
    except Exception as e:
        logger.warning(f"Spotify API error for {artist_url}: {e}")
    return new_tracks

def fetch_spotify_mp3(url):
    logger.info(f"Downloading Spotify track: {url}")
    with tempfile.TemporaryDirectory() as tmpdir:
        songs = spdl.search([url])
        if not songs:
            raise Exception("Spotdl: no song found")
        _, path = spdl.download(songs[0], output=f"{tmpdir}/%(title)s - %(artist)s.mp3")
        return BytesIO(open(path, "rb").read())

# ==== TELEGRAM SENDER ====
bot = Bot(TOKEN)

async def send_audio(io_data: BytesIO, title: str) -> bool:
    safe_name = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_"))[:50] + ".mp3"
    io_data.name = safe_name
    for attempt in range(MAX_RETRIES + 1):
        try:
            io_data.seek(0)
            await bot.send_audio(chat_id=GROUP_ID, audio=InputFile(io_data), caption=title)
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited, retry after {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Network error: {e}, retrying in {RETRY_DELAY}s")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Failed to send audio: {e}")
            return False
    return False

# ==== MAIN ORCHESTRATION ====
async def main():
    logger.info("=== Bot start ===")
    sent = 0

    # YouTube
    for ch in YOUTUBE_CHANS:
        for vid, url, title in list_new_videos(ch):
            if vid in processed["ytm"]:
                continue
            try:
                data = fetch_ytm_mp3(url)
                if await send_audio(data, title):
                    processed["ytm"].append(vid)
                    sent += 1
                data.close()
            except Exception as e:
                logger.error(f"YouTube error ({vid}): {e}")

    # Spotify
    for art in SPOTIFY_ARTS:
        for tid, url, title in list_new_spotify(art):
            if tid in processed["spotify"]:
                continue
            try:
                data = fetch_spotify_mp3(url)
                if await send_audio(data, title):
                    processed["spotify"].append(tid)
                    sent += 1
                data.close()
            except Exception as e:
                logger.error(f"Spotify error ({tid}): {e}")

    if sent:
        save_history()
    logger.info(f"=== Bot done, sent {sent} tracks ===")

if __name__ == "__main__":
    # verify ffmpeg
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.DEVNULL)
        logger.info("ffmpeg found")
    except Exception:
        logger.warning("ffmpeg not found; audio conversion may fail")
    asyncio.run(main())
