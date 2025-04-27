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
from spotdl import Spotdl

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
HISTORY_FILE      = os.getenv('HIST_FILE', 'history.json')
TIMEZONE          = pytz.timezone("Pacific/Kiritimati")
INTERVAL_SECONDS  = 2 * 3600   # 2 heures
MAX_RETRIES       = 3
RETRY_DELAY       = 10  # secondes

print(f"Le fichier d'historique est : {HISTORY_FILE}")

def exit_fatal(msg):
    logger.error(f"FATAL: {msg}")
    sys.exit(1)

if not TOKEN or not GROUP_ID_STR:
    exit_fatal("TELEGRAM_BOT_TOKEN ou TELEGRAM_GROUP_ID manquants")
try:
    GROUP_ID = int(GROUP_ID_STR)
except ValueError:
    exit_fatal("TELEGRAM_GROUP_ID non numérique")

if not SPOTIFY_ID or not SPOTIFY_SECRET:
    logger.warning("Spotify credentials manquants — Spotify ignoré")

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
]

SPOTIFY_ARTISTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"
]

# ==== History management ====
def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"ytm": [], "spotify": []}

def save_history(hist):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

# ==== Utils ====
def now_kiritimati():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(TIMEZONE)

# ==== YouTube ====
def list_new_youtube_videos(hist):
    new = []
    now_dt = now_kiritimati()
    for url in YOUTUBE_CHANNELS:
        logger.info(f"Check YT channel: {url}")
        cid = url.split("/channel/")[-1].split("/")[0] if "/channel/" in url else url.rstrip("/").split("/")[-1]
        feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}")
        if feed.bozo:
            logger.error(f"RSS error: {feed.bozo_exception}")
            continue
        for e in feed.entries:
            vid = getattr(e, "yt_videoid", None)
            if not vid or vid in hist["ytm"]:
                continue
            if not e.get("published_parsed"):
                continue
            pub = datetime.datetime(*e.published_parsed[:6], tzinfo=pytz.utc).astimezone(TIMEZONE)
            if 0 <= (now_dt - pub).total_seconds() < 7*24*3600:
                new.append((vid, e.link, e.title))
                logger.info(f"→ New video: {e.title}")
    return new

def fetch_youtube_mp3(video_url):
    """
    Télécharge l'audio en MP3 et retourne un BytesIO,
    ou None si la vidéo est programmée/premiere ou bloquée.
    """
    logger.info(f"Download YT MP3: {video_url}")
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
            # 1) extraire les méta sans télécharger
            try:
                info = ydl.extract_info(video_url, download=False)
            except DownloadError as e:
                msg = str(e)
                if "Premieres in" in msg or "HTTP Error 401" in msg:
                    logger.info(f"Skip vidéo programmée/401: {msg}")
                    return None
                raise

            # 2) skip si release_timestamp futur
            release_ts = info.get("release_timestamp") or 0
            if release_ts > time.time():
                logger.info(f"Skip future premiere (timestamp {release_ts} > now)")
                return None

            # 3) télécharger
            try:
                ydl.download([video_url])
            except DownloadError as e:
                msg = str(e)
                if "Premieres in" in msg or "HTTP Error 401" in msg:
                    logger.info(f"Skip après échec de download: {msg}")
                    return None
                raise

        # Récupérer le MP3
        files = [f for f in os.listdir(td) if f.endswith(".mp3")]
        if not files:
            logger.error("Aucun MP3 généré malgré un download réussi.")
            return None

        path = os.path.join(td, files[0])
        return BytesIO(open(path, "rb").read())

# ==== Spotify ====
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))
    spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, headless=True)
except Exception:
    sp = spdl = None

def list_new_spotify_tracks(hist):
    if not sp:
        return []
    new = []
    now_dt = now_kiritimati()
    for url in SPOTIFY_ARTISTS:
        logger.info(f"Check SP artist: {url}")
        aid = url.rstrip("/").split("/")[-1]
        try:
            albums = sp.artist_albums(aid, album_type="album,single", country="US", limit=20)
        except Exception as e:
            logger.error(f"Spotify API error: {e}")
            continue
        for alb in albums.get("items", []):
            rd, prec = alb.get("release_date"), alb.get("release_date_precision","day")
            try:
                fmt = {"year":"%Y","month":"%Y-%m","day":"%Y-%m-%d"}[prec]
                d = datetime.datetime.strptime(rd, fmt)
            except:
                continue
            pub = TIMEZONE.localize(d)
            if 0 <= (now_dt - pub).total_seconds() < 7*24*3600:
                tracks = sp.album_tracks(alb["id"]).get("items",[])
                for tr in tracks:
                    tid = tr.get("id")
                    link = tr.get("external_urls",{}).get("spotify")
                    name = tr.get("name")
                    artists = ", ".join(a["name"] for a in tr.get("artists",[]))
                    if tid and link and tid not in hist["spotify"]:
                        new.append((tid, link, f"{artists} - {name}"))
                        logger.info(f"→ New SP: {artists} - {name}")
    return new

def fetch_spotify_mp3(track_url):
    if not spdl:
        raise RuntimeError("spotdl not initialized")
    logger.info(f"Download SP MP3: {track_url}")
    with tempfile.TemporaryDirectory() as td:
        songs = spdl.search([track_url])
        out = os.path.join(td, "t.mp3")
        res = spdl.download_songs(songs, output=out)
        path = res[0][1] if res and res[0] else None
        return BytesIO(open(path, "rb").read())

# ==== Telegram sender ====
bot = Bot(TOKEN)

async def send_audio(buf, title):
    fn = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:60] + ".mp3"
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

# ==== Main loop ====
async def run_checks():
    # Decode cookies
    if YTDLP_COOKIES_B64:
        with open(COOKIES_FILE, "wb") as f:
            f.write(base64.b64decode(YTDLP_COOKIES_B64))

    hist = load_history()

    # YouTube
    for video_id, url, title in list_new_youtube_videos(hist):
        if video_id in hist["ytm"]:
            logger.info(f"Vidéo déjà envoyée: {title}")
            continue
        buf = fetch_youtube_mp3(url)
        if buf:
            if await send_audio(buf, title):
                hist["ytm"].append(video_id)
                save_history(hist)
            buf.close()
        await asyncio.sleep(3)

    # Spotify
    for track_id, url, title in list_new_spotify_tracks(hist):
        if track_id in hist["spotify"]:
            logger.info(f"Morceau déjà envoyé: {title}")
            continue
        buf = fetch_spotify_mp3(url)
        if buf:
            if await send_audio(buf, title):
                hist["spotify"].append(track_id)
                save_history(hist)
            buf.close()
        await asyncio.sleep(3)

async def main():
    while True:
        logger.info("=== Start checks ===")
        await run_checks()
        logger.info(f"Sleeping {INTERVAL_SECONDS//3600}h")
        await asyncio.sleep(INTERVAL_SECONDS)

# à remplacer dans bot.py, à la fin du fichier :

if __name__ == "__main__":
    try:
        # on ne boucle plus : on appelle juste une passe de run_checks()
        import asyncio
        asyncio.run(run_checks())
    except Exception as e:
        logger.exception(f"Fatal: {e}")
        sys.exit(1)

