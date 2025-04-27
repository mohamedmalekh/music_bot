#!/usr/bin/env python3
# bot.py

import os
import sys
import json
import subprocess
import tempfile
import datetime
import asyncio
import logging
import shutil
from io import BytesIO

import pytz
import feedparser

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
# Fichier de cookies exporté (Netscape format)
COOKIES_FILE = os.environ.get("YTDLP_COOKIES_FILE", "cookies.txt")

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
    logger.warning("Spotify credentials missing; Spotify functionality will be disabled.")

YOUTUBE_CHANNELS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",
]

SPOTIFY_ARTISTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"
]

TIMEZONE = pytz.timezone("Pacific/Kiritimati")
HISTORY_FILE = "processed.json"
MAX_RETRIES = 3
RETRY_DELAY = 10  # secondes
MAX_HISTORY_SIZE = 500  # Nombre maximum d'entrées à conserver dans l'historique

# ==== HISTORY HANDLING ====
def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning(f"History file '{HISTORY_FILE}' missing or invalid, starting fresh.")
        return {"ytm": [], "spotify": []}
    except Exception as e:
        logger.error(f"Error loading history: {e}, starting fresh.")
        return {"ytm": [], "spotify": []}

processed = load_history()

def trim_history(hist_dict):
    """Limite la taille de l'historique pour éviter une croissance infinie."""
    for key in hist_dict:
        if len(hist_dict[key]) > MAX_HISTORY_SIZE:
            hist_dict[key] = hist_dict[key][-MAX_HISTORY_SIZE:]
    return hist_dict

def save_history():
    try:
        # Trim history before saving
        global processed
        processed = trim_history(processed)
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(processed, f, indent=2, ensure_ascii=False)
        logger.info("History saved")
    except Exception as e:
        logger.error(f"Failed to save history: {e}")

def now_kiritimati():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(TIMEZONE)

# ==== YOUTUBE FUNCTIONS ====
def list_new_youtube_videos(channel_url):
    logger.info(f"Checking YouTube channel: {channel_url}")
    if "/channel/" in channel_url:
        channel_id = channel_url.split("/channel/")[-1].split('/')[0]
    else:
        channel_id = channel_url.rstrip('/').split('/')[-1]
        logger.warning("Unknown YouTube URL format, using last path segment as channel ID")
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        logger.error(f"Error fetching/parsing feed for {channel_url}: {e}")
        return []

    if feed.bozo:
        err = feed.bozo_exception
        logger.error(f"RSS parse error for {channel_url}: {type(err).__name__} -- {err}")
        return []

    new_entries = []
    now_dt = now_kiritimati()
    for entry in feed.entries:
        vid = getattr(entry, "yt_videoid", None)
        if not vid or vid in processed["ytm"]:
            continue
        if not entry.get("published_parsed"):
            continue

        pub_utc = datetime.datetime(*entry.published_parsed[:6], tzinfo=pytz.utc)
        pub_local = pub_utc.astimezone(TIMEZONE)
        delta = now_dt - pub_local
        if 0 <= delta.total_seconds() < 7 * 24 * 3600:
            new_entries.append((vid, entry.link, entry.title))
            logger.info(f"New YouTube video: {entry.title}")
    return new_entries

def fetch_youtube_mp3(video_url):
    logger.info(f"Downloading YouTube MP3: {video_url}")
    with tempfile.TemporaryDirectory() as tmpdir:
        ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
        cookies_path = os.path.join(tmpdir, "cookies.txt")
        
        # Vérifier et préparer les cookies
        try:
            if os.path.exists(COOKIES_FILE):
                # Copier le fichier cookies dans le dossier temporaire
                shutil.copy2(COOKIES_FILE, cookies_path)
                logger.info(f"Copied cookies file to temp dir: {cookies_path}")
            else:
                logger.warning(f"Cookie file '{COOKIES_FILE}' not found")
                cookies_path = None
        except Exception as e:
            logger.error(f"Error handling cookies file: {e}")
            cookies_path = None

        # Configuration yt-dlp
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(tmpdir, "%(title)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192"
            }],
            "ffmpeg_location": ffmpeg_path,
            "quiet": True,
            "no_warnings": True,
            "geo_bypass": True,
            "geo_bypass_country": "US",
            "socket_timeout": 30,
            "extractor_retries": 3,
            "fragment_retries": 3,
            "retries": 3,
            "no_check_certificate": True,
            "force_ipv4": True
        }

        if cookies_path:
            ydl_opts["cookiefile"] = cookies_path

        methods = [
            lambda: download_with_opts(video_url, ydl_opts, tmpdir),
            lambda: download_with_ytdl_direct(video_url, tmpdir, cookies_path)
        ]

        for method in methods:
            try:
                result = method()
                if result:
                    return result
            except Exception as e:
                logger.error(f"Download method failed: {str(e)}")
                continue

        raise RuntimeError(f"All download methods failed for {video_url}")

def download_with_opts(video_url, ydl_opts, tmpdir):
    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            
        mp3s = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
        if not mp3s:
            logger.error("No MP3 produced by yt-dlp")
            return None
            
        path = os.path.join(tmpdir, mp3s[0])
        with open(path, "rb") as f:
            return BytesIO(f.read())
    except Exception as e:
        logger.error(f"Error in primary download method: {e}")
        return None

def download_with_ytdl_direct(video_url, tmpdir, cookies_path=None):
    output_template = os.path.join(tmpdir, "audio.%(ext)s")
    cmd = [
        "yt-dlp",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "192K",
        "--output", output_template,
        "--geo-bypass",
        "--no-check-certificate",
        "--force-ipv4",
        "--no-warnings"
    ]
    
    if cookies_path:
        cmd.extend(["--cookies", cookies_path])
    
    cmd.append(video_url)
    
    try:
        logger.info(f"Executing direct command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        mp3s = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
        if not mp3s:
            logger.error("No MP3 produced by direct command")
            return None
            
        path = os.path.join(tmpdir, mp3s[0])
        with open(path, "rb") as f:
            return BytesIO(f.read())
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"Error in alternative download method: {e}")
        return None