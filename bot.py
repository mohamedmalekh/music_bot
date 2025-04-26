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
import re
import requests
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
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GROUP_ID_STR = os.environ.get("TELEGRAM_GROUP_ID")
SPOTIFY_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
YTDLP_COOKIES = os.environ.get("YTDLP_COOKIES")  # Path to cookies.txt for yt-dlp

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
    logger.warning("Spotify credentials missing; Spotify functionality will fail.")

YOUTUBE_CHANNELS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",  # Mootjeyek - Topic
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",  # A.L.A - Topic
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",  # A.L.A
    "https://www.youtube.com/@Mootjeyek",                        # moot jeyek
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",  # El Castro
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",  # El Castro - Topic
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",  # ElGrandeToto - Topic
    "https://www.youtube.com/@M.M.Hofficial"                     # M.M.H
]

SPOTIFY_ARTISTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",  # MOOTJEYEK
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",  # A.L.A
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",  # El Castro
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"   # ElGrandeToto
]

TIMEZONE = pytz.timezone("Pacific/Kiritimati")
HISTORY_FILE = "processed.json"
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds

# ==== HISTORY HANDLING ====
def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ytm": [], "spotify": []}

processed = load_history()

def save_history():
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2)
    logger.info("History saved")

def now_kiritimati():
    return datetime.datetime.now(pytz.utc).astimezone(TIMEZONE)

# ==== YOUTUBE FUNCTIONS ====
def list_new_youtube_videos(channel_url):
    logger.info(f"Checking YouTube channel: {channel_url}")

    # Resolve either /channel/ID or an @handle to a real channel ID
    if "/channel/" in channel_url:
        channel_id = channel_url.split("/channel/")[-1]
    elif "@" in channel_url:
        resp = requests.get(channel_url)
        if resp.status_code != 200:
            logger.error(f"Could not fetch handle page: {channel_url} → {resp.status_code}")
            return []
        m = re.search(r'"channelId"\s*:\s*"([A-Za-z0-9_-]+)"', resp.text)
        if not m:
            logger.error(f"Could not resolve channel handle to ID: {channel_url}")
            return []
        channel_id = m.group(1)
    else:
        logger.error(f"Invalid YouTube channel URL: {channel_url}")
        return []

    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    feed = feedparser.parse(feed_url)

    if feed.bozo:
        logger.error(f"Error parsing feed: {feed.bozo_exception}")
        return []

    new_entries = []
    now_dt = now_kiritimati()

    for entry in feed.entries:
        video_id = entry.yt_videoid
        pub_time = datetime.datetime(*entry.published_parsed[:6], tzinfo=pytz.utc).astimezone(TIMEZONE)
        delta = now_dt - pub_time

        if video_id not in processed["ytm"] and delta.days < 7:
            new_entries.append((video_id, entry.link, entry.title))
            logger.info(f"Found new video: {entry.title}")

    return new_entries

def fetch_youtube_mp3(video_url):
    logger.info(f"Downloading YouTube MP3: {video_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "audio.%(ext)s")
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": output_path,
            "quiet": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192"
            }],
            # include cookies.txt if provided
            **({"cookiefile": YTDLP_COOKIES} if YTDLP_COOKIES else {})
        }

        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        mp3_files = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
        if not mp3_files:
            raise Exception("No MP3 file found")

        with open(os.path.join(tmpdir, mp3_files[0]), "rb") as f:
            return BytesIO(f.read())

# ==== SPOTIFY FUNCTIONS ====
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))
    spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, headless=True)
except Exception as e:
    sp = None
    spdl = None
    logger.warning(f"Failed to initialize Spotify: {e}")

def list_new_spotify_tracks(artist_url):
    if not sp:
        return []

    logger.info(f"Checking Spotify artist: {artist_url}")
    artist_id = artist_url.rstrip("/").split("/")[-1].split("?")[0]
    new_tracks = []
    now_dt = now_kiritimati()

    try:
        albums = sp.artist_albums(artist_id, album_type="single,album", country="US", limit=30)
        for album in albums.get("items", []):
            rd = album.get("release_date")
            precision = album.get("release_date_precision", "day")

            try:
                if precision == "day":
                    d = datetime.datetime.strptime(rd, "%Y-%m-%d")
                elif precision == "month":
                    d = datetime.datetime.strptime(rd, "%Y-%m")
                else:
                    d = datetime.datetime.strptime(rd, "%Y")
            except ValueError as e:
                logger.error(f"Date parsing error: {e} for {rd} with precision {precision}")
                continue

            pub_time = TIMEZONE.localize(d)
            delta = now_dt - pub_time

            if delta.days < 7:
                tracks = sp.album_tracks(album["id"]).get("items", [])
                for tr in tracks:
                    tid = tr["id"]
                    if tid not in processed["spotify"]:
                        title = f"{', '.join(a['name'] for a in tr['artists'])} - {tr['name']}"
                        url = tr["external_urls"]["spotify"]
                        new_tracks.append((tid, url, title))
                        logger.info(f"Found new track: {title}")
    except Exception as e:
        logger.error(f"Spotify API error: {e}")

    return new_tracks

def fetch_spotify_mp3(track_url):
    if not spdl:
        raise Exception("Spotify downloader not initialized")

    logger.info(f"Downloading Spotify MP3: {track_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        songs = spdl.search([track_url])
        if not songs:
            raise Exception("No songs found")

        _, path = spdl.download(songs[0], output=os.path.join(tmpdir, "%(title)s - %(artist)s.mp3"))
        with open(path, "rb") as f:
            return BytesIO(f.read())

# ==== TELEGRAM SENDER ====
bot = Bot(TOKEN)

async def send_audio(data: BytesIO, title: str):
    safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:50] + ".mp3"
    data.name = safe_title

    for attempt in range(MAX_RETRIES):
        try:
            data.seek(0)
            await bot.send_audio(chat_id=GROUP_ID, audio=InputFile(data), caption=title)
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limit hit. Sleeping {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Network error: {e}. Retrying in {RETRY_DELAY}s")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Failed to send audio: {e}")
            if attempt == MAX_RETRIES - 1:
                return False
    return False

# ==== MAIN ORCHESTRATION ====
async def main():
    logger.info("=== Bot started ===")
    sent = 0

    # YouTube
    for channel in YOUTUBE_CHANNELS:
        for vid, url, title in list_new_youtube_videos(channel):
            if vid in processed["ytm"]:
                continue
            try:
                data = fetch_youtube_mp3(url)
                if await send_audio(data, title):
                    processed["ytm"].append(vid)
                    sent += 1
                    save_history()
                data.close()
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"YouTube error ({vid}): {e}")

    # Spotify
    if sp and spdl:
        for artist in SPOTIFY_ARTISTS:
            for tid, url, title in list_new_spotify_tracks(artist):
                if tid in processed["spotify"]:
                    continue
                try:
                    data = fetch_spotify_mp3(url)
                    if await send_audio(data, title):
                        processed["spotify"].append(tid)
                        sent += 1
                        save_history()
                    data.close()
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Spotify error ({tid}): {e}")
    else:
        logger.warning("Skipping Spotify checks due to missing credentials")

    if sent:
        save_history()
    logger.info(f"=== Bot finished: {sent} tracks sent ===")

if __name__ == "__main__":
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.DEVNULL)
        logger.info("ffmpeg is available")
    except Exception:
        logger.error("ffmpeg not found; audio conversions will fail")
        sys.exit(1)

    asyncio.run(main())
