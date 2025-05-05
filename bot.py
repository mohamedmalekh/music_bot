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

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotdl.download.downloader import Downloader
from spotdl.types.song import Song

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
SPOTIFY_ID        = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET    = os.environ.get("SPOTIFY_CLIENT_SECRET")
YTDLP_COOKIES_B64 = os.environ.get("YTDLP_COOKIES_B64", "")
COOKIES_FILE      = "cookies.txt"
HISTORY_FILE      = os.getenv("HIST_FILE", "/data/history.json")
TIMEZONE          = pytz.timezone("Pacific/Kiritimati")
INTERVAL_SECONDS  = 15 * 60  # 15 minutes
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

if not SPOTIFY_ID or not SPOTIFY_SECRET:
    logger.warning("Spotify credentials manquantes -- fonctionnalités Spotify désactivées")

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
    "https://www.youtube.com/channel/UCWcQRCPPW4qxa3OCUBBsuFw",
    "https://www.youtube.com/channel/UCtPSFgBQPsM7NW3iJpuqFuQ",
    "https://www.youtube.com/channel/UCmMHQBby2vt2Qd9StcmBXXQ",
    "https://www.youtube.com/channel/UC1Vribmny1eI62yYWux3rdQ",
    "https://www.youtube.com/channel/UC0XcAvzmW91qsWor3Qg6r3g",
    "https://www.youtube.com/channel/UCWWn6dtJhc5JcchjSzuEL8g",
    "https://www.youtube.com/channel/UCucdEPn-auvaUmBOJYuX7Og",
    "https://www.youtube.com/channel/UC7JpmOJscJcm-VsF6XnvYWg",
    "https://www.youtube.com/channel/UC8bEqtGJEUoYdWnti8k3R2Q",
    "https://www.youtube.com/channel/UC-bIUmH8gFA_lFkV2rJ6GMQ",
    "https://youtube.com/channel/UCCYT3uqgB08dh5mS3h421kA",
    "https://youtube.com/channel/UCLbji6FWNYfRiD9Un4v9jiA",
    "https://youtube.com/channel/UCOCMlsYEf9Bj70R_tXtvJEg",
    "https://youtube.com/channel/UCtPSFgBQPsM7NW3iJpuqFuQ"
]

SPOTIFY_ARTISTS = [
    "https://open.spotify.com/artist/18QlLaFDdsOhib17zPVVsU?si=i9ZJsv62RLqmOEwdH4woAw",
    "https://open.spotify.com/intl-fr/artist/2BBnFUgIaLHqoRYPfshoPb",
    "https://open.spotify.com/intl-fr/artist/3MKpGPhBp9KeXjGooKHNDX",
    "https://open.spotify.com/intl-fr/artist/3IW7ScrzXmPvZhB27hmfgy",
    "https://open.spotify.com/intl-fr/artist/06z6NBx0H2PDzZqw8mPTDz",
    "https://open.spotify.com/intl-fr/artist/6J3OrlKMbWMx60M7QuDJsf",
    "https://open.spotify.com/intl-fr/artist/3Ofbm810VXiC3VaO76oMPP",
    "https://open.spotify.com/intl-fr/artist/0GOx72r5AAEKRGQFn3xqXK",
    "https://open.spotify.com/intl-fr/artist/5aj6jIshzpUh4WQvQ5EzKO",
    "https://open.spotify.com/intl-fr/artist/5KrsMlfx8tbhq2GjZo0KP5",
    "https://open.spotify.com/intl-fr/artist/5gs4Sm2WQUkcGeikMcVHbh",
    "https://open.spotify.com/intl-fr/artist/6jGMq4yGs7aQzuGsMgVgZR",
    "https://open.spotify.com/intl-fr/artist/0C8ZW7ezQVs4URX5aX7Kqx",
    "https://open.spotify.com/intl-fr/artist/0VRj0yCOv2FXJNP47XQnx5",
    "https://open.spotify.com/intl-fr/artist/1RyvyyTE3xzB2ZywiAwp0i"
]

# ==== Historique ====
def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        return {"ytm": [], "spotify": []}

def save_history(hist):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

# ==== Utilitaires ====
def now_kiritimati():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(TIMEZONE)

# ==== YouTube functions ====
def list_new_youtube_videos(hist):
    new = []
    now_dt = now_kiritimati()
    for url in YOUTUBE_CHANNELS:
        cid = url.rstrip("/").split("/")[-1]
        feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}")
        if feed.bozo:
            logger.error(f"RSS parse error: {feed.bozo_exception}")
            continue
        for e in feed.entries:
            vid = getattr(e, "yt_videoid", None)
            if not vid or vid in hist["ytm"]: 
                continue
            if not e.get("published_parsed"): 
                continue
            pub = datetime.datetime(*e.published_parsed[:6], tzinfo=pytz.utc).astimezone(TIMEZONE)
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
                    logger.info(f"Skipping unavailable video: {msg}")
                    return None
                raise
            if (info.get("release_timestamp") or 0) > time.time():
                logger.info("Skipping future premiere")
                return None
            try:
                ydl.download([video_url])
            except DownloadError as e:
                msg = str(e)
                if any(phrase in msg for phrase in (
                    "Premieres in", "HTTP Error 401",
                    "Sign in to confirm you're not a bot"
                )):
                    logger.info(f"Skipping after download error: {msg}")
                    return None
                raise

        files = [f for f in os.listdir(td) if f.endswith(".mp3")]
        if not files:
            logger.error("No MP3 generated")
            return None
        return BytesIO(open(os.path.join(td, files[0]), "rb").read())

# ==== Spotify ====
spotify_client = None
if SPOTIFY_ID and SPOTIFY_SECRET:
    try:
        spotify_client = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_ID, 
            client_secret=SPOTIFY_SECRET
        ))
    except Exception as e:
        logger.error(f"Erreur d'initialisation Spotify: {e}")

def list_new_spotify_tracks(hist):
    if not spotify_client:
        return []
    new = []
    now_dt = now_kiritimati()
    for url in SPOTIFY_ARTISTS:
        aid = url.rstrip("/").split("/")[-1].split("?")[0]
        try:
            albums = spotify_client.artist_albums(aid, album_type="album,single", country="US", limit=20)
        except Exception as e:
            logger.error(f"Spotify API error: {e}")
            continue
        for alb in albums.get("items", []):
            rd, prec = alb.get("release_date"), alb.get("release_date_precision", "day")
            fmt_map = {"year":"%Y", "month":"%Y-%m", "day":"%Y-%m-%d"}
            try:
                d = datetime.datetime.strptime(rd, fmt_map[prec])
            except:
                continue
            pub = TIMEZONE.localize(d)
            if 0 <= (now_dt - pub).total_seconds() < 7*24*3600:
                try:
                    for tr in spotify_client.album_tracks(alb["id"]).get("items", []):
                        tid = tr.get("id")
                        link = tr["external_urls"]["spotify"]
                        title = f"{', '.join(a['name'] for a in tr['artists'])} - {tr['name']}"
                        if tid and link and tid not in hist["spotify"]:
                            new.append((tid, link, title))
                            logger.info(f"→ New track: {title}")
                except Exception as e:
                    logger.error(f"Error getting album tracks: {e}")
                    continue
    return new

def fetch_spotify_mp3(track_url):
    if not spotify_client:
        return None

    logger.info(f"Téléchargement Spotify: {track_url}")
    
    with tempfile.TemporaryDirectory() as td:
        try:
            config = {
                "output": td,
                "format": "mp3",
                "bitrate": "320k",
                "ffmpeg": shutil.which("ffmpeg") or "ffmpeg",
                "sponsor_block": False,
                "preserve_original_audio": False,
                "threads": 4
            }

            downloader = Downloader(config)
            
            song = Song.from_url(track_url)
            if not song:
                raise ValueError("URL Spotify invalide")

            download_result = downloader.download_song(song)
            
            if download_result and os.path.exists(download_result):
                if os.path.getsize(download_result) < 1024:
                    raise ValueError("Fichier trop petit")

                with open(download_result, "rb") as f:
                    return BytesIO(f.read())

        except Exception as e:
            logger.error(f"Erreur de téléchargement Spotify: {str(e)}")
            return None

# ==== Telegram Sender ====
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
                read_timeout=60,
                write_timeout=60,
                connect_timeout=30,
            )
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut):
            await asyncio.sleep(RETRY_DELAY)
        finally:
            buf.seek(0)
    return False

async def run_checks():
    if YTDLP_COOKIES_B64:
        with open(COOKIES_FILE, "wb") as f:
            f.write(base64.b64decode(YTDLP_COOKIES_B64))

    hist = load_history()

    # YouTube (inchangé)
    for vid, url, title in list_new_youtube_videos(hist):
        if vid not in hist["ytm"]:
            buf = fetch_youtube_mp3(url)
            if buf and await send_audio(buf, title):
                hist["ytm"].append(vid)
                save_history(hist)
            if buf:
                buf.close()
        await asyncio.sleep(3)

    # Spotify modifié
    if spotify_client:
        for tid, url, title in list_new_spotify_tracks(hist):
            if tid not in hist["spotify"]:
                try:
                    buf = fetch_spotify_mp3(url)
                    if buf and await send_audio(buf, title):
                        hist["spotify"].append(tid)
                        save_history(hist)
                    if buf:
                        buf.close()
                except Exception as e:
                    logger.exception(f"Erreur Spotify {url}: {e}")
            await asyncio.sleep(3)

# ... (Le reste du code reste inchangé) ...

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Erreur fatale: {e}")
        sys.exit(1)
