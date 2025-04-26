import os
import json
import subprocess
import tempfile
import datetime
import asyncio
from io import BytesIO
import pytz
import yt_dlp
from telegram import Bot, InputFile, constants
from telegram.error import NetworkError, RetryAfter, TimedOut
from yt_dlp import YoutubeDL
from spotdl import Spotdl
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import logging
import time # Import time for sleep

# ==== CONFIGURATION LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)

# ==== CONSTANTES ====

# --- Telegram ---
# !! REPLACE WITH YOUR ACTUAL BOT TOKEN !!
TOKEN = "7887523979:AAEoIhCgY7ksuKL1AHo-p81mj4byWlaoNXw"
# !! REPLACE WITH YOUR ACTUAL GROUP/CHANNEL ID (must start with -100 for channels) !!
GROUP_ID = -1001628331527

# --- Spotify ---
# !! Optional: Replace with your Spotify API Credentials if needed, otherwise Spotdl might use defaults !!
SPOTIFY_ID = "325ea6f4f8114707811076e24f83b514"
SPOTIFY_SECRET = "17c9c497e32f494a906e4d030a4d8838"

# --- YouTube Channels ---
# !! REPLACE THESE PLACEHOLDERS WITH ACTUAL YOUTUBE CHANNEL URLS !!
# Examples: "https://www.youtube.com/channel/UCxxxxxxxxxxxxxxx" or "https://www.youtube.com/@ChannelName"
YOUTUBE_CHANS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",  # Example: Mootjeyek - Topic -> Find actual URL
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",  # Example: A.L.A - Topic -> Find actual URL
    "https://www.youtube.com/@alaofficiel",                     # Example: A.L.A
    "https://www.youtube.com/@Mootjeyek",                       # Example: Moot jeyek
    "https://www.youtube.com/@ElCastroOfficial",                # Example: El Castro
    "https://www.youtube.com/channel/UC77MxBM3K2j6Mxx0aVf5QOQ",  # Example: El Castro - Topic -> Find actual URL
    "https://www.youtube.com/channel/UCWgt7F3y3Q4sa_zT5z0AYaQ",  # Example: ElGrandeToto - Topic -> Find actual URL
    # Add more actual channel URLs here
]

# --- Spotify Artists ---
# !! REPLACE THESE PLACEHOLDERS WITH ACTUAL SPOTIFY ARTIST URLS !!
# Example: "https://open.spotify.com/artist/xxxxxxxxxxxxxxxxxxxxxx"
SPOTIFY_ARTS = [
    "https://open.spotify.com/artist/0iQMIM3AN8N3Cvykjtdr3A",  # MOOTJEYEK
    "https://open.spotify.com/artist/4r812N35QRdy3h46O547mS",  # A.L.A
    "https://open.spotify.com/artist/3O3fI5QOvQ2799ZZYfhYYM",  # El Castro
    "https://open.spotify.com/artist/0nAaQeopHzCRSFCTOAkZwX"   # ElGrandeToto
    # Add more actual artist URLs here
]

# --- Other Settings ---
KIRI_TZ = pytz.timezone("Pacific/Kiritimati") # Keep this if you specifically need Kiritimati time
HIST_FILE = "processed.json"
MAX_RETRIES = 3 # Max retries for Telegram sending
RETRY_DELAY = 10 # Seconds to wait between retries

# --- Dependencies Note ---
# This script requires 'ffmpeg' to be installed and accessible in your system's PATH
# for yt-dlp to convert videos to MP3.

# ==== HISTORY HANDLING ====
try:
    with open(HIST_FILE, "r") as f:
        processed = json.load(f)
        # Ensure keys exist
        if "ytm" not in processed:
            processed["ytm"] = []
        if "spotify" not in processed:
            processed["spotify"] = []
except (FileNotFoundError, json.JSONDecodeError):
    logger.warning(f"{HIST_FILE} not found or invalid. Creating a new one.")
    processed = {"ytm": [], "spotify": []}
    # Create the file if it doesn't exist or is empty/invalid
    with open(HIST_FILE, "w") as f:
        json.dump(processed, f)

# ==== UTILITIES ====

def save_hist():
    """Saves the current state of processed IDs to the history file."""
    try:
        with open(HIST_FILE, "w") as f:
            json.dump(processed, f, indent=2)
        logger.debug("History saved successfully.")
    except Exception as e:
        logger.error(f"Error saving history to {HIST_FILE}: {e}")

def now_kiri():
    """Returns the current datetime in the Kiritimati timezone."""
    return datetime.datetime.now(KIRI_TZ)

# ==== YOUTUBE MUSIC ====

def list_new_videos(channel_url):
    """
    Lists videos from a YouTube channel URL published within the last 7 days.
    Handles regular videos and Shorts found on the main channel page/uploads.
    """
    logger.info(f"Checking YouTube channel: {channel_url}")

    # Use options to get metadata directly, avoid flat extract initially
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'ignoreerrors': True,
        'extract_flat': False, # Get more metadata initially
        'force_generic_extractor': False,
        'playlistend': 20 # Limit checks to the latest 20 items for efficiency
    }

    all_entries = []
    try:
        with YoutubeDL(ydl_opts) as ydl:
            # Directly extract info from the channel URL
            # yt-dlp usually defaults to the 'Uploads' playlist
            info = ydl.extract_info(channel_url, download=False)

        if info and 'entries' in info:
            all_entries = info['entries']
        else:
             logger.warning(f"No entries found directly for channel {channel_url}. Info: {info}")

    except yt_dlp.utils.DownloadError as e:
         # Catch specific yt-dlp errors, e.g., channel not found or private
         logger.warning(f"yt-dlp could not extract info for {channel_url}: {e}")
         return []
    except Exception as e:
        logger.error(f"Unexpected error extracting video list for {channel_url}: {e}")
        return []

    if not all_entries:
        logger.info(f"No videos/shorts listed for {channel_url}")
        return []

    new_videos = []
    current_time_kiri = now_kiri()

    for entry in all_entries:
        if entry is None:
            continue

        vid = entry.get("id")
        if not vid or vid in processed["ytm"]:
            # logger.debug(f"Skipping already processed/invalid video ID: {vid}")
            continue

        video_url = entry.get("webpage_url") or f"https://www.youtube.com/watch?v={vid}"
        title = entry.get("title", f"Unknown Title - {vid}")

        # Try to get timestamp directly from the entry first
        ts = entry.get("release_timestamp") or entry.get("timestamp")

        # If timestamp not found in initial data, try a specific lookup (less efficient)
        if not ts:
             logger.debug(f"Timestamp not in initial entry for {vid}. Performing detailed lookup.")
             detail_opts = {
                 'quiet': True,
                 'no_warnings': True,
                 'skip_download': True,
                 'ignoreerrors': True,
             }
             try:
                 with YoutubeDL(detail_opts) as ydl_detail:
                     video_info = ydl_detail.extract_info(video_url, download=False)
                     ts = video_info.get("release_timestamp") or video_info.get("timestamp")
             except Exception as e:
                 logger.warning(f"Could not fetch details for {video_url}: {e}. Skipping timestamp check.")
                 # Decide if you want to include videos without a timestamp check
                 # For now, we'll skip them if we can't verify the date
                 continue

        if ts:
            try:
                # Convert timestamp to Kiritimati time
                upload_dt_utc = datetime.datetime.fromtimestamp(ts, tz=pytz.utc)
                upload_dt_kiri = upload_dt_utc.astimezone(KIRI_TZ)

                time_diff = current_time_kiri - upload_dt_kiri
                # Check if within the last 7 days
                if time_diff.days <= 7 and time_diff.total_seconds() >= 0: # Ensure it's not in the future
                    logger.info(f"Found new YouTube video: {title} ({vid}) published {time_diff.days} days ago")
                    new_videos.append((vid, video_url, title))
                # else:
                    # logger.debug(f"Skipping old video: {title} ({vid}) published {time_diff.days} days ago")

            except Exception as e:
                logger.warning(f"Error processing timestamp ({ts}) for video {vid} ({title}): {e}")
        else:
            # If after all attempts, timestamp is still missing, decide what to do.
            # Option 1: Skip (safer to avoid processing very old videos)
            logger.warning(f"Timestamp missing for video {vid} ({title}) after lookup. Skipping.")
            # Option 2: Process anyway (might send old videos)
            # logger.warning(f"Timestamp missing for video {vid} ({title}). Processing anyway.")
            # new_videos.append((vid, video_url, title))

    return new_videos


def fetch_ytm_mp3(url):
    """Downloads audio from a YouTube URL as MP3 into BytesIO."""
    logger.info(f"Downloading YouTube audio: {url}")
    temp_path = None # Ensure temp_path is defined for the finally block

    # Create a temporary file path *without* creating the file immediately
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as temp_file:
        temp_path = temp_file.name

    # Ensure the temporary file handle is closed before yt-dlp writes to it
    # temp_file object is closed here, but temp_path still holds the name

    try:
        # Define options for yt-dlp
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192', # Or '320' etc.
            }],
            'outtmpl': os.path.splitext(temp_path)[0], # Pass path without extension
            'quiet': True,
            'no_warnings': True,
            'ffmpeg_location': 'ffmpeg' # Optional: specify if not in PATH
        }

        # Download using yt-dlp
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        # yt-dlp adds the .mp3 extension automatically based on postprocessor
        actual_downloaded_path = os.path.splitext(temp_path)[0] + '.mp3'

        if not os.path.exists(actual_downloaded_path):
             raise FileNotFoundError(f"Expected MP3 file not found after download: {actual_downloaded_path}")

        # Read the downloaded file into memory
        with open(actual_downloaded_path, 'rb') as f:
            data = f.read()

        logger.info(f"Successfully downloaded and read: {actual_downloaded_path}")
        return BytesIO(data)

    except Exception as e:
        logger.error(f"Error during YouTube download/conversion for {url}: {e}")
        # Re-raise the exception so the main loop knows it failed
        raise
    finally:
        # --- Cleanup ---
        # Construct the potential output path again
        actual_downloaded_path = os.path.splitext(temp_path)[0] + '.mp3'
        # Delete the temporary file(s) if they exist
        if os.path.exists(actual_downloaded_path):
            try:
                os.unlink(actual_downloaded_path)
                logger.debug(f"Cleaned up temporary file: {actual_downloaded_path}")
            except Exception as e_unlink:
                logger.error(f"Error deleting temporary file {actual_downloaded_path}: {e_unlink}")
        # Also try deleting the original placeholder path in case download failed early
        if os.path.exists(temp_path) and temp_path != actual_downloaded_path:
             try:
                 os.unlink(temp_path)
                 logger.debug(f"Cleaned up initial temporary file: {temp_path}")
             except Exception as e_unlink:
                 logger.error(f"Error deleting initial temporary file {temp_path}: {e_unlink}")


# ==== SPOTIFY ====

try:
    # Initialize Spotipy client
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET)
    )
    # Initialize Spotdl client (used for downloading)
    spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, ffmpeg='ffmpeg')
except Exception as e:
    logger.error(f"Error initializing Spotify clients: {e}")
    sp = None
    spdl = None # Ensure spdl is also None if init fails

def list_new_spotify(artist_url):
    """Lists tracks from a Spotify artist released within the last 7 days."""
    if sp is None:
        logger.error("Spotipy client not initialized. Skipping Spotify check.")
        return []

    logger.info(f"Checking Spotify artist: {artist_url}")
    try:
        # Extract artist ID from URL
        artist_id = artist_url.rstrip("/").split("/")[-1].split("?")[0] # Handle potential query params
    except IndexError:
        logger.error(f"Could not parse artist ID from URL: {artist_url}")
        return []

    new_tracks = []
    processed_track_ids_in_run = set() # Avoid processing duplicates within the same run/album
    current_time_kiri = now_kiri()

    try:
        # Get artist's albums and singles
        # Increase limit slightly to catch recent releases if many happened
        results = sp.artist_albums(artist_id, album_type="album,single", country="US", limit=30)
        items = results["items"] if results else []
    except Exception as e:
        logger.warning(f"Spotify API error fetching albums for {artist_url} (ID: {artist_id}): {e}")
        return []

    for album in items:
        album_id = album.get("id")
        release_date_str = album.get("release_date")
        release_precision = album.get("release_date_precision") # 'day', 'month', 'year'

        if not release_date_str or not album_id:
            logger.debug(f"Skipping album with missing ID or release date: {album.get('name')}")
            continue

        # --- Date Check ---
        try:
            release_dt = None
            # Handle different precisions
            if release_precision == "day":
                release_dt = datetime.datetime.strptime(release_date_str, "%Y-%m-%d")
            elif release_precision == "month":
                release_dt = datetime.datetime.strptime(release_date_str, "%Y-%m")
            elif release_precision == "year":
                release_dt = datetime.datetime.strptime(release_date_str, "%Y")
            else: # Should not happen, but fallback
                release_dt = datetime.datetime.fromisoformat(release_date_str)

            # Make timezone-aware using Kiritimati time (assuming release dates are ~midnight UTC/local)
            # We compare against Kiri time, so make the release date Kiri time too for fair comparison.
            # This assumes release date is roughly midnight in *some* timezone, assign Kiri.
            release_dt_kiri = KIRI_TZ.localize(release_dt)

            time_diff = current_time_kiri - release_dt_kiri

            # Check if the *album's* release date is within the 7-day window (+ a buffer for track processing)
            if time_diff.days <= 7 and time_diff.total_seconds() >= 0:
                 logger.info(f"Album '{album.get('name')}' released recently ({time_diff.days} days ago). Checking tracks.")
                 # --- Get Tracks for this Album ---
                 try:
                     tracks_result = sp.album_tracks(album_id, limit=50) # Get all tracks
                     album_tracks = tracks_result["items"] if tracks_result else []

                     for track in album_tracks:
                         track_id = track.get("id")
                         track_url = track.get("external_urls", {}).get("spotify")

                         if not track_id or not track_url:
                             logger.debug("Skipping track with missing ID or URL.")
                             continue

                         # Check if processed globally or within this run
                         if track_id in processed["spotify"] or track_id in processed_track_ids_in_run:
                             # logger.debug(f"Skipping already processed/queued Spotify track ID: {track_id}")
                             continue

                         # Construct title
                         track_name = track.get("name", "Unknown Track")
                         artist_names = ", ".join([a["name"] for a in track.get("artists", [])])
                         full_title = f"{artist_names} - {track_name}"

                         logger.info(f"Found new Spotify track: {full_title} ({track_id})")
                         new_tracks.append((track_id, track_url, full_title))
                         processed_track_ids_in_run.add(track_id) # Mark as processed for this run

                 except Exception as e_track:
                      logger.warning(f"Error fetching tracks for album {album_id} ('{album.get('name')}'): {e_track}")
            # else:
                 # logger.debug(f"Skipping older album: '{album.get('name')}' released {time_diff.days} days ago.")

        except (ValueError, TypeError) as e_date:
            logger.warning(f"Could not parse release date '{release_date_str}' for album {album_id}: {e_date}")
        except Exception as e_outer:
             logger.error(f"Unexpected error processing album {album_id}: {e_outer}")


    return new_tracks


def fetch_spotify_mp3(url):
    """Downloads a Spotify track using Spotdl into BytesIO."""
    if spdl is None:
         raise Exception("Spotdl client not initialized.")

    logger.info(f"Downloading Spotify track via Spotdl: {url}")
    temp_dir = None # Define for finally block
    download_path = None # Define for finally block

    try:
        # Create a temporary directory for Spotdl output
        temp_dir = tempfile.mkdtemp()
        logger.debug(f"Created temporary directory: {temp_dir}")

        # Search for the song using Spotdl (returns Song objects)
        songs = spdl.search([url])
        if not songs:
            raise Exception(f"Spotdl could not find the track for URL: {url}")

        # Spotdl's download method can take a Song object
        # It returns a tuple: (Song, Path | None)
        # We specify the output directory using {output-dir} template
        # Note: Spotdl's internal filename generation will be used within temp_dir
        song_object, downloaded_file_path = spdl.download(songs[0], output=f"{temp_dir}/{{title}} - {{artist}}.mp3")

        if not downloaded_file_path or not os.path.exists(downloaded_file_path):
            # Sometimes download path might be None even if successful but file exists, check dir
            potential_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".mp3")]
            if potential_files:
                 downloaded_file_path = potential_files[0] # Assume first mp3 is the one
                 logger.warning(f"Spotdl returned no path, but found MP3: {downloaded_file_path}")
            else:
                 raise Exception(f"Spotdl download failed or file not found for {url}. Check logs above.")

        logger.info(f"Spotdl downloaded to: {downloaded_file_path}")
        download_path = downloaded_file_path # Store for cleanup

        # Read the downloaded file into memory
        with open(downloaded_file_path, "rb") as f:
            data = f.read()

        return BytesIO(data)

    except Exception as e:
        logger.error(f"Error during Spotdl download for {url}: {e}")
        raise # Re-raise for main loop handling
    finally:
        # --- Cleanup ---
        if download_path and os.path.exists(download_path):
             try:
                 os.unlink(download_path)
                 logger.debug(f"Cleaned up temporary file: {download_path}")
             except Exception as e_unlink:
                 logger.error(f"Error deleting temporary file {download_path}: {e_unlink}")
        if temp_dir and os.path.exists(temp_dir):
            try:
                # Remove the directory itself after attempting to delete the file
                # Use shutil.rmtree if directory might contain other spotdl files/folders
                import shutil
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e_rmdir:
                logger.error(f"Error deleting temporary directory {temp_dir}: {e_rmdir}")


# ==== TELEGRAM ====

bot = Bot(TOKEN)

async def send_audio(io_data: BytesIO, title: str) -> bool:
    """Sends the audio file (BytesIO) to the Telegram group with retries."""
    # Sanitize filename for Telegram
    safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_'))[:60] # Limit length too
    io_data.name = f"{safe_filename}.mp3"
    logger.info(f"Attempting to send to Telegram: {io_data.name}")

    retries = 0
    while retries <= MAX_RETRIES:
        try:
            io_data.seek(0) # Reset stream position before sending
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(io_data, filename=io_data.name),
                caption=title, # Use original title in caption
                read_timeout=120, # Increase timeout for large files/slow network
                write_timeout=120,
                connect_timeout=60,
                pool_timeout=120,
                # Consider adding parse_mode=constants.ParseMode.HTML or MARKDOWN if needed
            )
            logger.info(f"Successfully sent to Telegram: {io_data.name}")
            return True # Success
        except RetryAfter as e:
            # Telegram flood control
            wait_time = e.retry_after + 1 # Add a buffer second
            logger.warning(f"Telegram Flood Control: Waiting {wait_time} seconds before retrying ({retries+1}/{MAX_RETRIES})...")
            await asyncio.sleep(wait_time)
            retries += 1
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Telegram Network Error/Timeout sending '{io_data.name}': {e}. Retrying in {RETRY_DELAY}s ({retries+1}/{MAX_RETRIES})...")
            await asyncio.sleep(RETRY_DELAY)
            retries += 1
        except Exception as e:
            # Catch other potential errors (e.g., invalid token, chat not found, file too large)
            logger.error(f"Unexpected Telegram error sending '{io_data.name}': {e}")
            # No retry for unexpected errors
            return False # Failure

    logger.error(f"Failed to send '{io_data.name}' to Telegram after {MAX_RETRIES} retries.")
    return False # Failure after retries


# ==== MAIN ====

async def main():
    """Main function to check sources and send new tracks."""
    logger.info("=== Bot Execution Started ===")
    success_count = 0
    error_count = 0

    # --- YouTube Check ---
    logger.info("--- Checking YouTube Channels ---")
    for channel_url in YOUTUBE_CHANS:
        if not channel_url or not channel_url.startswith("https://www.youtube.com/"):
             logger.warning(f"Skipping invalid or placeholder YouTube URL: {channel_url}")
             continue
        try:
            new_youtube_videos = list_new_videos(channel_url.strip())
            for vid, url, title in new_youtube_videos:
                if vid in processed["ytm"]: # Double check
                    continue
                try:
                    logger.info(f"Processing YouTube: {title} ({url})")
                    audio_data = fetch_ytm_mp3(url)
                    if await send_audio(audio_data, title):
                        processed["ytm"].append(vid)
                        save_hist() # Save history immediately after successful send
                        success_count += 1
                        await asyncio.sleep(2) # Small delay between sends
                    else:
                        error_count += 1
                        logger.error(f"Failed to send YouTube track: {title}")
                except Exception as e:
                    logger.error(f"Error processing YouTube video {vid} ('{title}'): {e}")
                    error_count += 1
                finally:
                    # Ensure buffer is closed if audio_data was created
                    if 'audio_data' in locals() and audio_data:
                         audio_data.close()

        except Exception as e:
            logger.error(f"Error processing YouTube channel {channel_url}: {e}")
            error_count += 1 # Count channel processing errors too

    # --- Spotify Check ---
    logger.info("--- Checking Spotify Artists ---")
    for artist_url in SPOTIFY_ARTS:
         if not artist_url or not artist_url.startswith("https://open.spotify.com/"):
             logger.warning(f"Skipping invalid or placeholder Spotify URL: {artist_url}")
             continue
         try:
            new_spotify_tracks = list_new_spotify(artist_url.strip())
            for tid, url, title in new_spotify_tracks:
                 if tid in processed["spotify"]: # Double check
                     continue
                 try:
                     logger.info(f"Processing Spotify: {title} ({url})")
                     audio_data = fetch_spotify_mp3(url)
                     if await send_audio(audio_data, title):
                         processed["spotify"].append(tid)
                         save_hist() # Save history immediately
                         success_count += 1
                         await asyncio.sleep(2) # Small delay
                     else:
                         error_count += 1
                         logger.error(f"Failed to send Spotify track: {title}")
                 except Exception as e:
                     logger.error(f"Error processing Spotify track {tid} ('{title}'): {e}")
                     error_count += 1
                 finally:
                    if 'audio_data' in locals() and audio_data:
                         audio_data.close()

         except Exception as e:
            logger.error(f"Error processing Spotify artist {artist_url}: {e}")
            error_count += 1


    logger.info(f"=== Bot Execution Finished: {success_count} sent, {error_count} errors ===")

if __name__ == "__main__":
    # Ensure FFmpeg is available (simple check, might need refinement)
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("ffmpeg found.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FATAL: ffmpeg not found or not executable. Please install ffmpeg and ensure it's in your PATH.")
        exit(1) # Exit if ffmpeg is missing

    asyncio.run(main())