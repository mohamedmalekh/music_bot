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
# Removed unused imports: urlparse, parse_qs

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

# *** IMPORTANT: Set your browser name here (e.g., 'firefox', 'chrome', 'edge', 'brave', 'opera') ***
# This browser MUST be logged into YouTube for cookie authentication to work.
COOKIES_BROWSER = 'firefox' # CHANGE THIS TO YOUR BROWSER

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

# *** NOTE: Removed URLs causing 'handle not supported for RSS' error in logs ***
# If you need these channels, find their Channel ID (starts with UC...)
# and use the format: http://www.youtube.com/watch?v=2yilMAEG9Xw
YOUTUBE_CHANNELS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",  # Mootjeyek - Topic
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",  # A.L.A - Topic
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",  # A.L.A
    # "https://www.youtube.com/@Mootjeyek",  # moot jeyek (@handle) - REMOVED, caused RSS error
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",  # El Castro
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",  # El Castro - Topic
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",  # ElGrandeToto - Topic
    # "https://www.youtube.com/@M.M.Hofficial"   # M.M.H (@handle) - REMOVED, caused RSS error
]

SPOTIFY_ARTISTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",  # MOOTJEYEK
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",  # A.L.A
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",  # El Castro
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"  # ElGrandeToto
]

TIMEZONE = pytz.timezone("Pacific/Kiritimati") # Consider using a more relevant timezone if needed
HISTORY_FILE = "processed.json"
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds

# ==== HISTORY HANDLING ====
def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"History file '{HISTORY_FILE}' not found, starting fresh.")
        return {"ytm": [], "spotify": []}
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from '{HISTORY_FILE}', starting fresh.")
        return {"ytm": [], "spotify": []}
    except Exception as e:
        logger.error(f"Error loading history: {e}, starting fresh.")
        return {"ytm": [], "spotify": []}

processed = load_history()

def save_history():
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(processed, f, indent=2, ensure_ascii=False) # ensure_ascii=False for non-latin chars in titles
        logger.info("History saved")
    except Exception as e:
        logger.error(f"Failed to save history: {e}")

def now_kiritimati():
    # Using UTC now and converting is generally safer
    return datetime.datetime.now(datetime.timezone.utc).astimezone(TIMEZONE)

# ==== YOUTUBE FUNCTIONS ====
def list_new_youtube_videos(channel_url):
    logger.info(f"Checking YouTube channel: {channel_url}")
    feed_url = None

    # Construct RSS feed URL based on channel URL type
    # NOTE: Handles (@username) might not reliably provide RSS feeds.
    # If a channel with a handle fails, try finding its Channel ID (starts with UC...)
    # and use the format: http://www.youtube.com/watch?v=2yilMAEG9Xw
    if "/channel/" in channel_url:
        channel_id = channel_url.split("/channel/")[-1].split('/')[0] # Handle potential extra paths
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    elif "/@" in channel_url:
        # This method is less reliable as YouTube doesn't officially support RSS for handles
        username = channel_url.split("/@")[-1].split('/')[0]
        # The URL format used before caused errors, trying the documented one:
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={username}"
        logger.warning(f"Attempting RSS feed for handle @{username}. This may not work. Consider using Channel ID URL instead.")
    elif "/user/" in channel_url:
        # Handle legacy /user/ URLs
        username = channel_url.split("/user/")[-1].split('/')[0]
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={username}"
    else:
        # Assuming it might be a channel ID directly or some other format yt supports for RSS
        # Extract potential ID - this is a guess
        possible_id = channel_url.split('/')[-1]
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={possible_id}"
        logger.warning(f"Unknown YouTube URL format: {channel_url}. Attempting generic RSS feed URL.")


    if not feed_url:
         logger.error(f"Could not determine RSS feed URL for: {channel_url}")
         return []

    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        logger.error(f"Error fetching/parsing feed for {channel_url}: {e}")
        return []


    if feed.bozo:
        # Log bozo errors more informatively
        error_type = type(feed.bozo_exception).__name__
        error_msg = str(feed.bozo_exception)
        if "404" in error_msg:
             logger.error(f"Error parsing feed for {channel_url}: 404 Not Found. Check if the URL or handle is correct and supports RSS.")
        elif "resolv" in error_msg:
             logger.error(f"Error parsing feed for {channel_url}: DNS resolution failed. Check network connection and URL.")
        else:
             logger.error(f"Error parsing feed for {channel_url}: {error_type} - {error_msg}")
        return []

    new_entries = []
    now_dt = now_kiritimati()

    for entry in feed.entries:
        try:
            video_id = entry.yt_videoid
            video_link = entry.link
            video_title = entry.title

            # Ensure published_parsed exists and is valid
            if not hasattr(entry, 'published_parsed') or not entry.published_parsed:
                logger.warning(f"Video '{video_title}' ({video_id}) has no valid publish date in feed. Skipping.")
                continue

            pub_time_utc = datetime.datetime(*entry.published_parsed[:6], tzinfo=pytz.utc)
            pub_time = pub_time_utc.astimezone(TIMEZONE)
            delta = now_dt - pub_time

            # Check within the last 7 days and not already processed
            if video_id not in processed["ytm"] and delta.days < 7 and delta.total_seconds() >= 0:
                new_entries.append((video_id, video_link, video_title))
                logger.info(f"Found new video: {video_title}")
        except AttributeError as e:
            logger.warning(f"Skipping entry due to missing attribute: {e}. Entry data: {entry}")
        except Exception as e:
            logger.error(f"Error processing feed entry: {e}. Entry data: {entry}")


    return new_entries

def fetch_youtube_mp3(video_url):
    logger.info(f"Downloading YouTube MP3: {video_url}")

    # Check if browser name is set
    if not COOKIES_BROWSER:
        logger.error("COOKIES_BROWSER is not set in the script. Cannot use cookie authentication.")
        raise ValueError("COOKIES_BROWSER variable is not set.")

    # Using a temporary directory for downloads
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "%(title)s.%(ext)s") # Use video title for filename
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": output_path,
            "quiet": False, # Set to False for more detailed download logs from yt-dlp
            "no_warnings": True, # Suppress yt-dlp warnings if needed, but errors will still show
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192" # Bitrate for MP3
            }],
            "ffmpeg_location": "ffmpeg", # Explicitly point to ffmpeg if needed
            # *** ADDED: Use cookies from the specified browser ***
            # Ensure you are logged into YouTube in this browser
            "cookiesfrombrowser": (COOKIES_BROWSER,),
            # Add other options if needed, e.g., geo-bypass
            # 'geo_bypass': True,
            # 'geo_bypass_country': 'US', # Example
        }

        try:
            with YoutubeDL(ydl_opts) as ydl:
                logger.debug(f"yt-dlp options: {ydl_opts}") # Log options for debugging
                logger.info(f"Starting download for {video_url} with cookie auth from '{COOKIES_BROWSER}'...")
                ydl.download([video_url])
                logger.info(f"Finished downloading {video_url}.")

            # Find the downloaded MP3 file (yt-dlp might slightly change the name)
            mp3_files = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
            if not mp3_files:
                logger.error(f"No MP3 file found in {tmpdir} after download attempt for {video_url}")
                raise Exception("No MP3 file found after download.")
            if len(mp3_files) > 1:
                 logger.warning(f"Multiple MP3 files found in {tmpdir}, using the first one: {mp3_files[0]}")

            mp3_file_path = os.path.join(tmpdir, mp3_files[0])
            logger.info(f"MP3 file ready: {mp3_file_path}")

            # Read the file into BytesIO
            with open(mp3_file_path, "rb") as f:
                return BytesIO(f.read())

        except Exception as e:
            # Catch yt-dlp specific errors or general exceptions
            logger.error(f"Error downloading YouTube video ({video_url}): {e}")
            # Re-raise the exception to be handled in the main loop
            raise

# ==== SPOTIFY FUNCTIONS ====
# Initialize Spotify clients, handle potential errors
try:
    if SPOTIFY_ID and SPOTIFY_SECRET:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))
        # Ensure spotdl uses the same credentials and is headless
        spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, headless=True, output=None) # output=None prevents default file creation
        logger.info("Spotify clients initialized successfully.")
    else:
        sp = None
        spdl = None
        logger.warning("Spotify credentials missing; Spotify functionality disabled.")
except Exception as e:
    sp = None
    spdl = None
    logger.warning(f"Failed to initialize Spotify clients: {e}. Spotify functionality disabled.")


def list_new_spotify_tracks(artist_url):
    if not sp:
        logger.debug("Spotify client (sp) not initialized, skipping check.")
        return []

    logger.info(f"Checking Spotify artist: {artist_url}")
    try:
        artist_id = artist_url.rstrip("/").split("/")[-1].split("?")[0]
    except IndexError:
        logger.error(f"Could not extract Spotify artist ID from URL: {artist_url}")
        return []

    new_tracks = []
    now_dt = now_kiritimati()

    try:
        # Fetch recent albums and singles
        albums = sp.artist_albums(artist_id, album_type="album,single", country="US", limit=20) # Limit to reasonable number
        if not albums or "items" not in albums:
            logger.warning(f"No albums found for artist ID {artist_id}.")
            return []

        for album in albums["items"]:
            rd = album.get("release_date")
            precision = album.get("release_date_precision", "day")

            if not rd:
                logger.warning(f"Album '{album.get('name')}' ({album.get('id')}) has no release date. Skipping.")
                continue

            # Parse release date based on precision
            try:
                if precision == "day":
                    d = datetime.datetime.strptime(rd, "%Y-%m-%d")
                elif precision == "month":
                    d = datetime.datetime.strptime(rd, "%Y-%m")
                elif precision == "year":
                    d = datetime.datetime.strptime(rd, "%Y")
                else:
                    logger.warning(f"Unknown release date precision '{precision}' for album {album.get('id')}. Skipping.")
                    continue
            except ValueError as e:
                logger.error(f"Date parsing error for album {album.get('id')}: {e} (Date: '{rd}', Precision: '{precision}')")
                continue

            # Localize the datetime object (assuming release date is naive in UTC, then convert to target TZ)
            # This might need adjustment depending on Spotify's actual date handling
            pub_time = TIMEZONE.localize(d)
            delta = now_dt - pub_time

            # Check if released within the last 7 days
            if delta.days < 7 and delta.total_seconds() >= 0:
                try:
                    tracks_data = sp.album_tracks(album["id"])
                    if not tracks_data or "items" not in tracks_data:
                        logger.warning(f"No tracks found for album ID {album['id']}.")
                        continue

                    tracks = tracks_data["items"]
                    for tr in tracks:
                        tid = tr.get("id")
                        turl = tr.get("external_urls", {}).get("spotify")
                        tname = tr.get("name")
                        artists = tr.get("artists", [])

                        if not tid or not turl or not tname:
                             logger.warning(f"Skipping track with missing data in album {album['id']}: {tr}")
                             continue

                        if tid not in processed["spotify"]:
                            artist_names = ', '.join(a.get('name', 'Unknown Artist') for a in artists)
                            title = f"{artist_names} - {tname}"
                            new_tracks.append((tid, turl, title))
                            logger.info(f"Found new Spotify track: {title}")
                except spotipy.SpotifyException as e:
                     logger.error(f"Spotify API error fetching tracks for album {album['id']}: {e}")
                except Exception as e:
                     logger.error(f"Unexpected error processing tracks for album {album['id']}: {e}")

    except spotipy.SpotifyException as e:
        logger.error(f"Spotify API error fetching albums for artist {artist_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking Spotify artist {artist_id}: {e}")

    return new_tracks


def fetch_spotify_mp3(track_url):
    if not spdl:
        logger.error("Spotify downloader (spotdl) not initialized.")
        raise Exception("Spotify downloader not initialized")

    logger.info(f"Downloading Spotify MP3: {track_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # Search for the song using spotdl
            songs = spdl.search([track_url])
            if not songs:
                logger.error(f"spotdl could not find song for URL: {track_url}")
                raise Exception(f"spotdl: Song not found for {track_url}")

            # Define output format within the temporary directory
            # Using a fixed name might be simpler if only downloading one file at a time
            # output_format = os.path.join(tmpdir, "%(title)s - %(artist)s.%(ext)s")
            output_format = os.path.join(tmpdir, "spotify_audio.mp3") # Fixed temporary name


            # Download the song
            # spotdl v4 returns list of tuples (Song, Path | None)
            results = spdl.download_songs(songs, output=output_format)

            if not results or not results[0] or not results[0][1]:
                 logger.error(f"spotdl download failed for {track_url}. No path returned.")
                 raise Exception(f"spotdl: Download failed for {track_url}")

            downloaded_path = results[0][1] # Get the path of the downloaded file

            if not os.path.exists(downloaded_path):
                 logger.error(f"spotdl reported success, but file not found at: {downloaded_path}")
                 raise Exception(f"spotdl: Downloaded file missing at {downloaded_path}")


            logger.info(f"Spotify MP3 file ready: {downloaded_path}")
            # Read the downloaded file into BytesIO
            with open(downloaded_path, "rb") as f:
                return BytesIO(f.read())

        except Exception as e:
            logger.error(f"Error during Spotify download ({track_url}): {e}")
            raise # Re-raise the exception


# ==== TELEGRAM SENDER ====
bot = Bot(TOKEN)

async def send_audio(data: BytesIO, title: str):
    # Sanitize title for filename, keeping it reasonably short
    safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:60] + ".mp3"
    data.name = safe_title # Set the filename for Telegram upload
    data.seek(0) # Ensure stream position is at the beginning

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Attempt {attempt + 1}/{MAX_RETRIES}: Sending '{title}' to Telegram Group {GROUP_ID}")
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(data, filename=safe_title), # Pass filename explicitly
                caption=title,
                read_timeout=60, # Increase timeouts for potentially large files/slow network
                write_timeout=60,
                connect_timeout=30
            )
            logger.info(f"Successfully sent '{title}'")
            return True # Return True on success
        except RetryAfter as e:
            # Telegram rate limit hit
            wait_time = e.retry_after + 1 # Add a small buffer
            logger.warning(f"Rate limit hit sending '{title}'. Sleeping for {wait_time:.2f} seconds.")
            await asyncio.sleep(wait_time)
        except (NetworkError, TimedOut) as e:
            # Network issues, retry after delay
            logger.warning(f"Network error sending '{title}': {e}. Retrying in {RETRY_DELAY}s (Attempt {attempt + 1}/{MAX_RETRIES})")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            # Other unexpected errors
            logger.error(f"Failed to send audio '{title}' (Attempt {attempt + 1}/{MAX_RETRIES}): {type(e).__name__} - {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY) # Wait before final retry
            else:
                logger.error(f"Giving up sending '{title}' after {MAX_RETRIES} attempts.")
                return False # Return False after final attempt fails
        finally:
             data.seek(0) # Reset stream position for next attempt or if sending failed

    logger.error(f"Failed to send '{title}' after all retries.")
    return False # Return False if all retries failed


# ==== MAIN ORCHESTRATION ====
async def main():
    logger.info("=== Bot starting run ===")
    sent_count = 0
    processed_in_run = {"ytm": [], "spotify": []} # Track items processed in this specific run

    # --- YouTube Processing ---
    logger.info("--- Starting YouTube checks ---")
    for channel_url in YOUTUBE_CHANNELS:
        new_videos = []
        try:
            new_videos = list_new_youtube_videos(channel_url)
        except Exception as e:
             logger.error(f"Failed to list videos for {channel_url}: {e}")
             continue # Skip to next channel

        for video_id, video_url, video_title in new_videos:
            if video_id in processed["ytm"]:
                logger.debug(f"Skipping already processed YouTube video: {video_title} ({video_id})")
                continue
            if video_id in processed_in_run["ytm"]:
                 logger.debug(f"Skipping YouTube video already processed in this run: {video_title} ({video_id})")
                 continue

            logger.info(f"Processing new YouTube video: {video_title} ({video_id})")
            audio_data = None
            try:
                audio_data = fetch_youtube_mp3(video_url)
                if audio_data:
                    if await send_audio(audio_data, video_title):
                        processed["ytm"].append(video_id)
                        processed_in_run["ytm"].append(video_id)
                        sent_count += 1
                        save_history() # Save history immediately after successful send
                    else:
                         logger.error(f"Failed to send YouTube audio to Telegram: {video_title}")
                else:
                     logger.error(f"Fetching YouTube MP3 returned no data for: {video_title}")

            except Exception as e:
                # Log error from fetch_youtube_mp3 or send_audio if they raised exceptions
                logger.error(f"Error processing YouTube video {video_title} ({video_id}): {e}")
            finally:
                if audio_data:
                    audio_data.close() # Ensure BytesIO object is closed

            # Add a small delay between processing videos to avoid overwhelming APIs
            await asyncio.sleep(3) # Sleep for 3 seconds

    logger.info("--- Finished YouTube checks ---")


    # --- Spotify Processing ---
    logger.info("--- Starting Spotify checks ---")
    if sp and spdl: # Only proceed if Spotify clients are initialized
        for artist_url in SPOTIFY_ARTISTS:
            new_tracks = []
            try:
                 new_tracks = list_new_spotify_tracks(artist_url)
            except Exception as e:
                 logger.error(f"Failed to list tracks for {artist_url}: {e}")
                 continue # Skip to next artist

            for track_id, track_url, track_title in new_tracks:
                if track_id in processed["spotify"]:
                    logger.debug(f"Skipping already processed Spotify track: {track_title} ({track_id})")
                    continue
                if track_id in processed_in_run["spotify"]:
                     logger.debug(f"Skipping Spotify track already processed in this run: {track_title} ({track_id})")
                     continue


                logger.info(f"Processing new Spotify track: {track_title} ({track_id})")
                audio_data = None
                try:
                    audio_data = fetch_spotify_mp3(track_url)
                    if audio_data:
                        if await send_audio(audio_data, track_title):
                            processed["spotify"].append(track_id)
                            processed_in_run["spotify"].append(track_id)
                            sent_count += 1
                            save_history() # Save history immediately
                        else:
                             logger.error(f"Failed to send Spotify audio to Telegram: {track_title}")
                    else:
                         logger.error(f"Fetching Spotify MP3 returned no data for: {track_title}")

                except Exception as e:
                    logger.error(f"Error processing Spotify track {track_title} ({track_id}): {e}")
                finally:
                    if audio_data:
                        audio_data.close()

                # Add delay between Spotify tracks
                await asyncio.sleep(3) # Sleep for 3 seconds

    else:
        logger.warning("Skipping Spotify checks as clients are not initialized (check credentials).")
    logger.info("--- Finished Spotify checks ---")


    # Final save just in case (though saving after each success is preferred)
    if sent_count > 0:
        save_history()
    logger.info(f"=== Bot finished run: {sent_count} new tracks sent ===")

# ==== SCRIPT ENTRY POINT ====
if __name__ == "__main__":
    # Check for ffmpeg availability before starting
    try:
        # Use subprocess.run for better error handling and capturing output/errors
        result = subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True, text=True)
        logger.info("ffmpeg found successfully.")
        # Optionally log ffmpeg version: logger.debug(f"ffmpeg version info:\n{result.stdout[:100]}...") # Log first 100 chars
    except FileNotFoundError:
        logger.error("FATAL: ffmpeg command not found. Please install ffmpeg and ensure it's in your system's PATH.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
         logger.error(f"FATAL: ffmpeg command failed with error code {e.returncode}. Output: {e.stderr}")
         sys.exit(1)
    except Exception as e:
        logger.error(f"FATAL: An unexpected error occurred while checking for ffmpeg: {e}")
        sys.exit(1)

    # Run the main asynchronous function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
         logger.exception(f"An unhandled exception occurred in main: {e}") # Log full traceback
         sys.exit(1) # Exit with error status on unhandled exception