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
        
        # Configuration améliorée pour yt-dlp
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(tmpdir, "%(title)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192"
            }],
            "ffmpeg_location": ffmpeg_path,
            "quiet": False,
            "verbose": True,  # Afficher plus de détails pour le débogage
            "geo_bypass": True,  # Contourner les restrictions géographiques
            "geo_bypass_country": "US", 
            "socket_timeout": 30,  # Augmenter le délai d'attente
            "extractor_retries": 5,  # Plus de tentatives
            "fragment_retries": 5,
            "retries": 5
        }
        
        # Vérifier et utiliser les cookies si disponibles
        if os.path.isfile(COOKIES_FILE):
            logger.info(f"Using cookies file: {COOKIES_FILE}")
            # Vérifier le contenu du fichier de cookies
            with open(COOKIES_FILE, 'r') as f:
                cookie_content = f.read().strip()
                if cookie_content and not cookie_content.startswith("Error:"):
                    ydl_opts["cookiefile"] = COOKIES_FILE
                else:
                    logger.error(f"Cookie file is empty or invalid")
        else:
            logger.warning(f"Cookie file '{COOKIES_FILE}' not found; YouTube may ask for sign-in.")

        # Tentatives avec différentes méthodes si nécessaire
        methods = [
            lambda: download_with_opts(video_url, ydl_opts, tmpdir),
            lambda: download_with_ytdl_direct(video_url, tmpdir)
        ]
        
        for method in methods:
            try:
                result = method()
                if result:
                    return result
            except Exception as e:
                logger.error(f"Method failed: {str(e)}")
                continue
                
        raise RuntimeError(f"All download methods failed for {video_url}")

def download_with_opts(video_url, ydl_opts, tmpdir):
    """Télécharge avec les options yt-dlp spécifiées"""
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

def download_with_ytdl_direct(video_url, tmpdir):
    """Méthode alternative utilisant une commande système directe"""
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
        video_url
    ]
    
    if os.path.isfile(COOKIES_FILE):
        cmd.extend(["--cookies", COOKIES_FILE])
    
    try:
        logger.info(f"Executing direct command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"Command output: {result.stdout}")
        
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

# ==== SPOTIFY FUNCTIONS ====
try:
    if SPOTIFY_ID and SPOTIFY_SECRET:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))
        spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, headless=True)
        logger.info("Spotify clients initialized.")
    else:
        sp = spdl = None
        logger.warning("Spotify credentials missing.")
except Exception as e:
    sp = spdl = None
    logger.warning(f"Failed to init Spotify clients: {e}")

def list_new_spotify_tracks(artist_url):
    if not sp:
        return []
    logger.info(f"Checking Spotify artist: {artist_url}")
    aid = artist_url.rstrip("/").split("/")[-1]
    new = []
    now_dt = now_kiritimati()
    try:
        albums = sp.artist_albums(aid, album_type="album,single", country="US", limit=20)
        for alb in albums.get("items", []):
            rd = alb.get("release_date")
            prec = alb.get("release_date_precision", "day")
            try:
                fmt = {"year":"%Y", "month":"%Y-%m", "day":"%Y-%m-%d"}[prec]
                d = datetime.datetime.strptime(rd, fmt)
            except Exception:
                continue
            pub = TIMEZONE.localize(d)
            delta = now_dt - pub
            if 0 <= delta.total_seconds() < 7 * 24 * 3600:
                tracks = sp.album_tracks(alb["id"]).get("items", [])
                for tr in tracks:
                    tid = tr.get("id"); turl = tr.get("external_urls", {}).get("spotify")
                    name = tr.get("name"); arts = tr.get("artists", [])
                    if not (tid and turl and name): continue
                    if tid not in processed["spotify"]:
                        title = ", ".join(a.get("name","") for a in arts) + " - " + name
                        new.append((tid, turl, title))
                        logger.info(f"New Spotify track: {title}")
    except Exception as e:
        logger.error(f"Error checking artist {aid}: {e}")
    return new

def fetch_spotify_mp3(track_url):
    if not spdl:
        raise RuntimeError("spotdl not initialized")
    logger.info(f"Downloading Spotify MP3: {track_url}")
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # Première tentative avec spotdl
            songs = spdl.search([track_url])
            if not songs:
                raise FileNotFoundError(f"No song found for {track_url}")
            out = os.path.join(tmpdir, "track.mp3")
            results = spdl.download_songs(songs, output=out)
            path = results[0][1] if results and results[0] else None
            if not path or not os.path.exists(path):
                raise FileNotFoundError("spotdl download failed")
            with open(path, "rb") as f:
                return BytesIO(f.read())
        except Exception as e:
            logger.error(f"Error with spotdl: {e}")
            
            # Méthode alternative avec commandes système
            try:
                cmd = ["spotdl", "--output", tmpdir, track_url]
                subprocess.run(cmd, check=True, capture_output=True)
                
                mp3s = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
                if not mp3s:
                    raise FileNotFoundError("No MP3 produced by spotdl command")
                    
                path = os.path.join(tmpdir, mp3s[0])
                with open(path, "rb") as f:
                    return BytesIO(f.read())
            except Exception as e2:
                logger.error(f"Alternative spotdl method failed: {e2}")
                raise RuntimeError(f"All Spotify download methods failed for {track_url}")

# ==== TELEGRAM SENDER & MAIN ====
bot = Bot(TOKEN)

async def send_audio(data: BytesIO, title: str):
    filename = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:60] + ".mp3"
    data.name = filename
    data.seek(0)
    for i in range(MAX_RETRIES):
        try:
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(data, filename=filename),
                caption=title,
                read_timeout=60, write_timeout=60, connect_timeout=30
            )
            return True
        except RetryAfter as e:
            logger.info(f"Rate limited. Retrying after {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after + 1)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Network error: {e}. Retrying after {RETRY_DELAY} seconds")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Error sending audio: {e}")
            if i == MAX_RETRIES - 1:
                return False
            await asyncio.sleep(RETRY_DELAY)
        finally:
            data.seek(0)
    return False

async def main():
    logger.info("=== Bot starting run ===")
    sent = 0
    run_hist = {"ytm": [], "spotify": []}

    # Vérifier que les fichiers nécessaires existent
    if os.path.isfile(COOKIES_FILE):
        logger.info(f"Cookies file exists: {COOKIES_FILE}")
        cookie_size = os.path.getsize(COOKIES_FILE)
        logger.info(f"Cookies file size: {cookie_size} bytes")
    else:
        logger.warning(f"Cookies file missing: {COOKIES_FILE}")

    logger.info("--- YouTube checks ---")
    for ch in YOUTUBE_CHANNELS:
        for vid, url, title in list_new_youtube_videos(ch):
            if vid in processed["ytm"] or vid in run_hist["ytm"]:
                continue
            try:
                logger.info(f"Processing YouTube video: {title} ({vid})")
                buf = fetch_youtube_mp3(url)
                if buf and await send_audio(buf, title):
                    processed["ytm"].append(vid)
                    run_hist["ytm"].append(vid)
                    sent += 1
                    save_history()
                    logger.info(f"Successfully processed and sent: {title}")
                if buf:
                    buf.close()
            except Exception as e:
                logger.error(f"YouTube error {title}: {e}")
            await asyncio.sleep(3)

    logger.info("--- Spotify checks ---")
    if sp and spdl:
        for art in SPOTIFY_ARTISTS:
            for tid, url, title in list_new_spotify_tracks(art):
                if tid in processed["spotify"] or tid in run_hist["spotify"]:
                    continue
                try:
                    logger.info(f"Processing Spotify track: {title} ({tid})")
                    buf = fetch_spotify_mp3(url)
                    if buf and await send_audio(buf, title):
                        processed["spotify"].append(tid)
                        run_hist["spotify"].append(tid)
                        sent += 1
                        save_history()
                        logger.info(f"Successfully processed and sent: {title}")
                    if buf:
                        buf.close()
                except Exception as e:
                    logger.error(f"Spotify error {title}: {e}")
                await asyncio.sleep(3)
    else:
        logger.warning("Skipping Spotify (credentials missing).")

    logger.info(f"=== Bot finished run: {sent} new tracks ===")

if __name__ == "__main__":
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True)
        logger.info("ffmpeg is available")
    except Exception as e:
        logger.error(f"FATAL: ffmpeg missing or broken ({e})")
        sys.exit(1)

    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        sys.exit(1)