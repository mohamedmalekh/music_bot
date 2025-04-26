import os, json, subprocess, tempfile, datetime
from io import BytesIO
import pytz
from telegram import Bot, InputFile
from yt_dlp import YoutubeDL
from spotdl import Spotdl
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

YOUTUBE_CHANS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",
    "https://www.youtube.com/@Mootjeyek.",
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",
    "https://www.youtube.com/@M.M.Hofficial"
]

# Spotify Artists
SPOTIFY_ARTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",
    "https://open.spotify.com/intl-fr/artist/4BFLElxtBEdsdwGA1kHTsx"
]

# 1) CONFIG via ENV
TOKEN         = "7887523979:AAEoIhCgY7ksuKL1AHo-p81mj4byWlaoNXw"
GROUP_ID      = -1001628331527
# Spotify API creds
SPOTIFY_ID     = "325ea6f4f8114707811076e24f83b514"
SPOTIFY_SECRET = "17c9c497e32f494a906e4d030a4d8838"

KIRI_TZ = pytz.timezone("Pacific/Kiritimati")

# 2) HISTORIQUE
HIST_FILE = "processed.json"
processed = json.load(open(HIST_FILE)) if os.path.exists(HIST_FILE) else {"ytm": [], "spotify": []}
def save_hist(): json.dump(processed, open(HIST_FILE,"w"), indent=2)

# 3) YouTube Music → liste des nouvelles vidéos
def list_new_videos(channel_url):
    with YoutubeDL({"quiet":True,"skip_download":True}) as ydl:
        info = ydl.extract_info(f"{channel_url}/videos", download=False)
    now_kiri = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(KIRI_TZ)
    out=[]
    for e in info.get("entries",[]):
        vid=e.get("id"); ts=e.get("release_timestamp") or e.get("timestamp")
        if not ts or vid in processed["ytm"]: continue
        dt_kiri = datetime.datetime.fromtimestamp(ts,tz=pytz.utc).astimezone(KIRI_TZ)
        if dt_kiri<=now_kiri:
            url=e.get("webpage_url",f"https://www.youtube.com/watch?v={vid}")
            out.append((vid,url))
    return out

def fetch_ytm_mp3(url):
    p1 = subprocess.Popen(["yt-dlp","-f","bestaudio","-o","-","--quiet",url], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["ffmpeg","-i","pipe:0","-f","mp3","-ab","192k","pipe:1"],
                           stdin=p1.stdout, stdout=subprocess.PIPE)
    data,_=p2.communicate(); return BytesIO(data)

# 4) Spotify → liste des nouveaux tracks via API
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET))

def list_new_spotify(artist_url):
    artist_id = artist_url.rstrip("/").split("/")[-1]
    # récupère singles et albums triés par date
    items = sp.artist_albums(artist_id, include_groups="single,album", country="US", limit=20)["items"]
    # on élimine doublons d’albums+singles
    seen=set(); new=[]
    now_kiri = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(KIRI_TZ)
    for alb in items:
        for tr in sp.album_tracks(alb["id"])["items"]:
            tid = tr["id"]
            if tid in seen or tid in processed["spotify"]: continue
            seen.add(tid)
            # date de release de l’album
            rel = alb["release_date"]  # YYYY-MM-DD
            dt = datetime.datetime.fromisoformat(rel).replace(tzinfo=KIRI_TZ)
            if dt <= now_kiri:
                new.append((tid, tr["external_urls"]["spotify"]))
    return new

def fetch_spotify_mp3(url):
    tmpf = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
    spdl = Spotdl()
    spdl.download_track(url, output_format="mp3",
                       output_dir=os.path.dirname(tmpf.name),
                       output_filename=os.path.basename(tmpf.name))
    with open(tmpf.name,"rb") as f: data=f.read()
    os.unlink(tmpf.name)
    return BytesIO(data)

# 5) Envoi Telegram
bot = Bot(TOKEN)
def send(io, title):
    io.name=f"{title}.mp3"
    bot.send_audio(chat_id=GROUP_ID, audio=InputFile(io))

# 6) Orchestration
# YouTube Music
for ch in YOUTUBE_CHANS:
    for vid, url in list_new_videos(ch.strip()):
        try:
            io = fetch_ytm_mp3(url)
            send(io, f"YTM-{vid}")
            processed["ytm"].append(vid); save_hist()
        except Exception as e:
            print("YT err", vid, e)

# Spotify artistes
for art in SPOTIFY_ARTS:
    for tid, url in list_new_spotify(art.strip()):
        try:
            io = fetch_spotify_mp3(url)
            send(io, f"SP-{tid}")
            processed["spotify"].append(tid); save_hist()
        except Exception as e:
            print("SP err", tid, e)
