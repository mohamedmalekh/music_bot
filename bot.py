# main_bot.py
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
import sys # Import sys for exit

# ==== CONFIGURATION LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)

# ==== CONSTANTES (Lues depuis les variables d'environnement) ====

# --- Telegram ---
# !! RÉCUPÉRÉ DEPUIS LES SECRETS GITHUB !!
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
# !! RÉCUPÉRÉ DEPUIS LES SECRETS GITHUB (doit commencer par -100 pour les canaux) !!
GROUP_ID_STR = os.environ.get("TELEGRAM_GROUP_ID")

# --- Spotify ---
# !! RÉCUPÉRÉ DEPUIS LES SECRETS GITHUB !!
SPOTIFY_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

# --- Vérification des secrets ---
if not TOKEN:
    logger.error("FATAL: La variable d'environnement TELEGRAM_BOT_TOKEN n'est pas définie.")
    sys.exit(1)
if not GROUP_ID_STR:
    logger.error("FATAL: La variable d'environnement TELEGRAM_GROUP_ID n'est pas définie.")
    sys.exit(1)
try:
    GROUP_ID = int(GROUP_ID_STR)
except ValueError:
     logger.error(f"FATAL: TELEGRAM_GROUP_ID ('{GROUP_ID_STR}') n'est pas un entier valide.")
     sys.exit(1)

if not SPOTIFY_ID:
    logger.warning("ATTENTION: La variable d'environnement SPOTIFY_CLIENT_ID n'est pas définie. Spotdl/Spotipy pourrait utiliser des valeurs par défaut ou échouer.")
if not SPOTIFY_SECRET:
    logger.warning("ATTENTION: La variable d'environnement SPOTIFY_CLIENT_SECRET n'est pas définie. Spotdl/Spotipy pourrait utiliser des valeurs par défaut ou échouer.")


# --- YouTube Channels ---
# !! REMPLACEZ CES URL PAR LES VRAIES URL DES CHAINES YOUTUBE !!
# Exemple : "https://www.youtube.com/channel/UCxxxxxxxxxxxxxxx" ou "https://www.youtube.com/@NomChaine"
YOUTUBE_CHANS = [
    "https://www.youtube.com/channel/UCmksE9VcSitikCJcs74N22A",   # Mootjeyek - Topic
    "https://www.youtube.com/channel/UC2emR2ejJMlvHdghCs3qOmQ",  # A.L.A - Topic
    "https://www.youtube.com/channel/UCldUc3lPRbibHFOomDrypXA",  # A.L.A
    "https://www.youtube.com/@Mootjeyek",  # Moot jeyek
    "https://www.youtube.com/channel/UCTPID7oLcNr0H-VhAVIO8Jw",  # El Castro
    "https://www.youtube.com/channel/UC7UizrbfFRtxIiEVQmdpUMA",  # El Castro - Topic
    "https://www.youtube.com/channel/UCiqwANpD_MyogjjPJyrbB-A",  # ElGrandeToto - Topic
    "https://www.youtube.com/@M.M.Hofficial"   # M.M.H
]

SPOTIFY_ARTS = [
    "https://open.spotify.com/artist/4VxyE4jGlkGfceluWCWZvH",  # MOOTJEYEK
    "https://open.spotify.com/artist/3MKpGPhBp9KeXjGooKHNDX",  # A.L.A
    "https://open.spotify.com/artist/5aj6jIshzpUh4WQvQ5EzKO",  # El Castro
    "https://open.spotify.com/artist/4BFLElxtBEdsdwGA1kHTsx"   # ElGrandeToto
]

# --- Other Settings ---
KIRI_TZ = pytz.timezone("Pacific/Kiritimati") # Gardez si vous avez besoin de l'heure de Kiritimati
HIST_FILE = "processed.json" # Fichier d'historique dans le répertoire courant
MAX_RETRIES = 3 # Tentatives max pour l'envoi Telegram
RETRY_DELAY = 10 # Secondes d'attente entre les tentatives

# --- Dependencies Note ---
# Ce script nécessite 'ffmpeg' installé sur le runner GitHub Actions.
# Le workflow s'en chargera.

# ==== HISTORY HANDLING ====
processed = {"ytm": [], "spotify": []} # Initialisation par défaut
try:
    # Utilise le chemin HIST_FILE défini ci-dessus
    with open(HIST_FILE, "r", encoding='utf-8') as f:
        loaded_processed = json.load(f)
        # Vérifier la structure et fusionner pour éviter les erreurs si le fichier est mal formé
        if isinstance(loaded_processed, dict):
            processed["ytm"] = loaded_processed.get("ytm", [])
            processed["spotify"] = loaded_processed.get("spotify", [])
            if not isinstance(processed["ytm"], list): processed["ytm"] = []
            if not isinstance(processed["spotify"], list): processed["spotify"] = []
        else:
             logger.warning(f"{HIST_FILE} ne contient pas un objet JSON valide. Création d'un nouveau.")
             # Créer le fichier s'il était invalide
             with open(HIST_FILE, "w", encoding='utf-8') as f_write:
                json.dump(processed, f_write, indent=2)

except FileNotFoundError:
    logger.warning(f"{HIST_FILE} non trouvé. Création d'un nouveau.")
    # Crée le fichier s'il n'existe pas
    try:
        with open(HIST_FILE, "w", encoding='utf-8') as f:
            json.dump(processed, f, indent=2)
    except Exception as e_create:
         logger.error(f"Erreur lors de la création du fichier d'historique {HIST_FILE}: {e_create}")
except json.JSONDecodeError:
    logger.error(f"Erreur lors du décodage JSON de {HIST_FILE}. Le fichier est peut-être corrompu. Création d'un nouveau.")
    # Crée un nouveau fichier propre si l'ancien est corrompu
    try:
        with open(HIST_FILE, "w", encoding='utf-8') as f:
             json.dump(processed, f, indent=2)
    except Exception as e_create:
         logger.error(f"Erreur lors de la création du fichier d'historique {HIST_FILE}: {e_create}")

# ==== UTILITIES ====

def save_hist():
    """Sauvegarde l'état actuel des ID traités dans le fichier d'historique."""
    try:
        # Utilise le chemin HIST_FILE défini ci-dessus
        with open(HIST_FILE, "w", encoding='utf-8') as f:
            json.dump(processed, f, indent=2)
        logger.debug("Historique sauvegardé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de l'historique dans {HIST_FILE}: {e}")

def now_kiri():
    """Retourne la date et l'heure actuelles dans le fuseau horaire de Kiritimati."""
    return datetime.datetime.now(KIRI_TZ)

# ==== YOUTUBE MUSIC ====

def list_new_videos(channel_url):
    """Liste les vidéos d'une chaîne YouTube publiées au cours des 7 derniers jours."""
    logger.info(f"Vérification de la chaîne YouTube : {channel_url}")

    # Correction : 'extract_flat' à False récupère plus d'infos, mais peut être plus lent.
    # 'playlistend' limite le nombre d'éléments vérifiés pour l'efficacité.
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'ignoreerrors': True,
        'extract_flat': False, # Obtenir les métadonnées complètes
        'force_generic_extractor': False,
        'playlistend': 20, # Vérifier les 20 dernières vidéos/publications
        'dateafter': (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=8)).strftime('%Y%m%d') # Pré-filtrage par date si possible
    }

    all_entries = []
    try:
        with YoutubeDL(ydl_opts) as ydl:
            # yt-dlp gère les URL de chaînes (/channel/..., /@..., /user/...)
            info = ydl.extract_info(channel_url, download=False)

        if info and 'entries' in info and info['entries']: # Vérifie si entries n'est pas None ou vide
            all_entries = info['entries']
        else:
            # Essayer avec /videos si la première tentative échoue
            logger.info(f"Aucune entrée trouvée directement pour {channel_url}. Essai avec l'URL /videos...")
            try:
                 with YoutubeDL(ydl_opts) as ydl_videos:
                      info_videos = ydl_videos.extract_info(f"{channel_url.rstrip('/')}/videos", download=False)
                      if info_videos and 'entries' in info_videos and info_videos['entries']:
                           all_entries = info_videos['entries']
                      else:
                           logger.warning(f"Aucune entrée trouvée pour {channel_url}, même avec /videos.")
            except Exception as e_videos:
                logger.warning(f"Erreur lors de la tentative avec /videos pour {channel_url}: {e_videos}")


    except yt_dlp.utils.DownloadError as e:
        logger.warning(f"yt-dlp n'a pas pu extraire les infos pour {channel_url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Erreur inattendue lors de l'extraction de la liste de vidéos pour {channel_url}: {e}")
        return []

    if not all_entries:
        logger.info(f"Aucune vidéo/short listé pour {channel_url}")
        return []

    new_videos = []
    current_time_kiri = now_kiri() # Utilisation du fuseau horaire défini

    for entry in all_entries:
        # Certaines entrées peuvent être None si yt-dlp rencontre des erreurs
        if entry is None:
            continue

        vid = entry.get("id")
        if not vid or vid in processed["ytm"]:
            continue

        # Utiliser 'release_timestamp' ou 'timestamp' s'ils existent, sinon 'upload_date'
        ts = entry.get("release_timestamp") or entry.get("timestamp")
        upload_date_str = entry.get("upload_date") # Format YYYYMMDD

        release_dt_utc = None
        if ts:
            try:
                # Le timestamp est généralement déjà en UTC
                release_dt_utc = datetime.datetime.fromtimestamp(ts, tz=pytz.utc)
            except Exception as e_ts:
                 logger.warning(f"Impossible de traiter le timestamp {ts} pour la vidéo {vid}: {e_ts}")
        elif upload_date_str:
            try:
                release_dt_naive = datetime.datetime.strptime(upload_date_str, "%Y%m%d")
                # On suppose que la date de mise en ligne est à minuit UTC pour la comparaison
                release_dt_utc = pytz.utc.localize(release_dt_naive)
            except ValueError as e_date:
                logger.warning(f"Format de date de mise en ligne invalide '{upload_date_str}' pour la vidéo {vid}: {e_date}")

        if release_dt_utc:
            try:
                release_dt_kiri = release_dt_utc.astimezone(KIRI_TZ)
                time_diff = current_time_kiri - release_dt_kiri

                # Vérifie si dans les 7 derniers jours
                if 0 <= time_diff.total_seconds() < datetime.timedelta(days=7).total_seconds():
                    title = entry.get("title", f"Titre inconnu - {vid}")
                    # Construire l'URL si elle manque (moins fiable)
                    video_url = entry.get("webpage_url") or entry.get("original_url") or f"https://www.youtube.com/watch?v={vid}"
                    logger.info(f"Nouvelle vidéo YouTube trouvée : {title} ({vid}) publiée il y a {time_diff}")
                    new_videos.append((vid, video_url, title))
                # else:
                #      logger.debug(f"Vidéo ignorée (trop ancienne) : {entry.get('title')} ({vid}), publiée il y a {time_diff}")

            except Exception as e_conv:
                 logger.warning(f"Erreur lors de la conversion de fuseau horaire pour la vidéo {vid}: {e_conv}")
        else:
             logger.warning(f"Impossible de déterminer la date de publication pour la vidéo {vid} ({entry.get('title')}). Ignorée.")


    return new_videos


def fetch_ytm_mp3(url):
    """Télécharge l'audio d'une URL YouTube en MP3 dans BytesIO."""
    logger.info(f"Téléchargement de l'audio YouTube : {url}")
    mp3_data = None
    # Créer un dossier temporaire dédié pour ce téléchargement
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path_base = os.path.join(temp_dir, "audio_download") # Base du nom de fichier

        try:
            ydl_opts = {
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'outtmpl': f'{temp_path_base}.%(ext)s', # yt-dlp ajoutera .mp3
                'quiet': True,
                'no_warnings': True,
                'ffmpeg_location': 'ffmpeg', # S'assurer que ffmpeg est trouvé
                'noprogress': True,
                'retries': 3, # Ajouter des tentatives pour le téléchargement
                'fragment_retries': 3,
            }

            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            # Le fichier devrait être temp_path_base + ".mp3"
            actual_downloaded_path = f"{temp_path_base}.mp3"

            if not os.path.exists(actual_downloaded_path):
                # Vérifier si un autre fichier mp3 existe au cas où le nom serait différent
                found_mp3 = [f for f in os.listdir(temp_dir) if f.endswith(".mp3")]
                if found_mp3:
                    actual_downloaded_path = os.path.join(temp_dir, found_mp3[0])
                    logger.warning(f"Le fichier MP3 attendu n'a pas été trouvé, mais trouvé : {actual_downloaded_path}")
                else:
                    raise FileNotFoundError(f"Fichier MP3 attendu non trouvé après le téléchargement : {actual_downloaded_path}")


            # Lire le fichier téléchargé en mémoire
            with open(actual_downloaded_path, 'rb') as f:
                mp3_data = f.read()

            logger.info(f"Téléchargé et lu avec succès : {actual_downloaded_path} ({len(mp3_data)} bytes)")
            return BytesIO(mp3_data) # Retourne les données dans un objet BytesIO

        except Exception as e:
            logger.error(f"Erreur lors du téléchargement/conversion YouTube pour {url}: {e}")
            raise # Propage l'erreur pour que la boucle principale sache qu'elle a échoué
        # Le dossier temporaire et son contenu sont automatiquement supprimés à la sortie du bloc 'with'

# ==== SPOTIFY ====
sp = None
spdl = None
try:
    # Initialise le client Spotipy si les identifiants sont présents
    if SPOTIFY_ID and SPOTIFY_SECRET:
        sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET)
        )
        # Initialise le client Spotdl (utilisé pour le téléchargement)
        # Note : Spotdl peut nécessiter ffmpeg pour la conversion
        spdl = Spotdl(client_id=SPOTIFY_ID, client_secret=SPOTIFY_SECRET, ffmpeg='ffmpeg', headless=True) # headless=True est important pour les environnements non graphiques
        logger.info("Clients Spotipy et Spotdl initialisés.")
    else:
         logger.warning("Identifiants Spotify manquants, les fonctionnalités Spotify seront désactivées.")

except Exception as e:
    logger.error(f"Erreur lors de l'initialisation des clients Spotify : {e}")
    sp = None
    spdl = None # S'assurer que spdl est None si l'initialisation échoue


def list_new_spotify(artist_url):
    """Liste les morceaux d'un artiste Spotify sortis au cours des 7 derniers jours."""
    if sp is None:
        # logger.info("Client Spotipy non initialisé. Vérification Spotify ignorée.")
        return [] # Pas d'erreur, juste ignorer si non configuré

    logger.info(f"Vérification de l'artiste Spotify : {artist_url}")
    try:
        # Extrait l'ID de l'artiste depuis l'URL
        artist_id = artist_url.rstrip("/").split("/")[-1].split("?")[0]
    except IndexError:
        logger.error(f"Impossible d'extraire l'ID de l'artiste depuis l'URL : {artist_url}")
        return []

    new_tracks = []
    processed_track_ids_in_run = set() # Évite de traiter les doublons dans la même exécution/album
    current_time_kiri = now_kiri()

    try:
        # Obtenir les albums et singles de l'artiste
        # Augmenter légèrement la limite pour attraper les sorties récentes s'il y en a eu beaucoup
        results = sp.artist_albums(artist_id, album_type="album,single", country="FR", limit=30) # Utiliser un code pays pertinent ou None
        items = results["items"] if results and "items" in results else []
    except Exception as e:
        logger.warning(f"Erreur API Spotify lors de la récupération des albums pour {artist_url} (ID: {artist_id}): {e}")
        return []

    for album in items:
        album_id = album.get("id")
        release_date_str = album.get("release_date")
        release_precision = album.get("release_date_precision") # 'day', 'month', 'year'

        if not release_date_str or not album_id:
            logger.debug(f"Ignorer l'album avec ID ou date de sortie manquant : {album.get('name')}")
            continue

        # --- Vérification de la date ---
        try:
            release_dt = None
            if release_precision == "day":
                release_dt = datetime.datetime.strptime(release_date_str, "%Y-%m-%d")
            elif release_precision == "month":
                # Pour une précision au mois, on prend le premier jour du mois
                release_dt = datetime.datetime.strptime(release_date_str, "%Y-%m")
            elif release_precision == "year":
                 # Pour une précision à l'année, on prend le premier jour de l'année
                release_dt = datetime.datetime.strptime(release_date_str, "%Y")
            else: # Ne devrait pas arriver, mais fallback
                logger.warning(f"Précision de date inconnue '{release_precision}' pour l'album {album_id}. Tentative d'analyse ISO.")
                try:
                    # Essayer l'analyse ISO, mais cela pourrait échouer pour YYYY ou YYYY-MM
                    release_dt = datetime.datetime.fromisoformat(release_date_str)
                except ValueError:
                     logger.error(f"Impossible d'analyser la date '{release_date_str}' pour l'album {album_id}. Ignoré.")
                     continue # Passer à l'album suivant

            # Rendre la date consciente du fuseau horaire en utilisant le fuseau de Kiritimati
            # On compare par rapport à l'heure Kiri, donc rendre la date de sortie Kiri aussi pour une comparaison équitable.
            # Cela suppose que la date de sortie est vers minuit dans *un* fuseau horaire, on lui assigne Kiri.
            # Ajouter une gestion si release_dt est None (cas d'erreur d'analyse)
            if release_dt is None:
                logger.error(f"La date de sortie n'a pas pu être déterminée pour l'album {album_id}. Ignoré.")
                continue

            release_dt_kiri = KIRI_TZ.localize(release_dt) # Localiser l'heure naive

            time_diff = current_time_kiri - release_dt_kiri

            # Vérifier si la date de sortie de *l'album* est dans la fenêtre de 7 jours
            if 0 <= time_diff.total_seconds() < datetime.timedelta(days=7).total_seconds():
                logger.info(f"Album '{album.get('name')}' sorti récemment ({time_diff}). Vérification des pistes.")
                # --- Obtenir les pistes de cet album ---
                try:
                    tracks_result = sp.album_tracks(album_id, limit=50) # Obtenir toutes les pistes
                    album_tracks = tracks_result["items"] if tracks_result and "items" in tracks_result else []

                    for track in album_tracks:
                        track_id = track.get("id")
                        track_url = track.get("external_urls", {}).get("spotify")

                        if not track_id or not track_url:
                            logger.debug("Ignorer la piste avec ID ou URL manquant.")
                            continue

                        # Vérifier si traité globalement ou dans cette exécution
                        if track_id in processed["spotify"] or track_id in processed_track_ids_in_run:
                            continue

                        # Construire le titre
                        track_name = track.get("name", "Piste inconnue")
                        artist_names = ", ".join([a["name"] for a in track.get("artists", [])])
                        full_title = f"{artist_names} - {track_name}"

                        logger.info(f"Nouvelle piste Spotify trouvée : {full_title} ({track_id})")
                        new_tracks.append((track_id, track_url, full_title))
                        processed_track_ids_in_run.add(track_id) # Marquer comme traité pour cette exécution

                except Exception as e_track:
                    logger.warning(f"Erreur lors de la récupération des pistes pour l'album {album_id} ('{album.get('name')}'): {e_track}")
            # else:
            #      logger.debug(f"Ignorer l'album plus ancien : '{album.get('name')}' sorti il y a {time_diff}")

        except (ValueError, TypeError) as e_date:
            logger.warning(f"Impossible d'analyser la date de sortie '{release_date_str}' pour l'album {album_id}: {e_date}")
        except Exception as e_outer:
             logger.error(f"Erreur inattendue lors du traitement de l'album {album_id}: {e_outer}")

    return new_tracks


def fetch_spotify_mp3(url):
    """Télécharge une piste Spotify en utilisant Spotdl dans BytesIO."""
    if spdl is None:
         raise Exception("Client Spotdl non initialisé.")

    logger.info(f"Téléchargement de la piste Spotify via Spotdl : {url}")
    # Créer un répertoire temporaire pour la sortie de Spotdl
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.debug(f"Création du répertoire temporaire : {temp_dir}")
        download_path = None # Initialiser au cas où le téléchargement échoue

        try:
            # Spotdl v4+ utilise 'search' et 'download'
            # 'search' retourne une liste d'objets Song
            songs = spdl.search([url])
            if not songs:
                raise Exception(f"Spotdl n'a pas pu trouver la piste pour l'URL : {url}")

            # Télécharger la première chanson trouvée dans le dossier temporaire
            # Spotdl retourne (Song, Path | None)
            # Le format de sortie utilise les métadonnées de la chanson.
            # Spotdl gère la création du fichier dans temp_dir.
            song_object, downloaded_file_path = spdl.download(songs[0], output=f"{temp_dir}/{{title}} - {{artist}}.mp3")

            if downloaded_file_path and os.path.exists(downloaded_file_path):
                 download_path = downloaded_file_path # Chemin du fichier réussi
                 logger.info(f"Spotdl a téléchargé dans : {download_path}")
            else:
                 # Vérifier si un fichier .mp3 existe quand même (Spotdl peut parfois retourner None)
                 potential_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".mp3")]
                 if potential_files:
                      download_path = potential_files[0]
                      logger.warning(f"Spotdl n'a retourné aucun chemin, mais a trouvé un MP3 : {download_path}")
                 else:
                     # Rechercher des fichiers temporaires potentiels si le téléchargement a échoué à mi-chemin
                     temp_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".temp")]
                     if temp_files:
                         logger.error(f"Spotdl semble avoir échoué, fichier temporaire trouvé : {temp_files[0]}")
                     raise Exception(f"Échec du téléchargement Spotdl ou fichier non trouvé pour {url}. Vérifiez les logs ci-dessus.")

            # Lire le fichier téléchargé en mémoire
            with open(download_path, "rb") as f:
                data = f.read()
            logger.info(f"Fichier Spotify lu en mémoire ({len(data)} bytes).")
            return BytesIO(data)

        except Exception as e:
            logger.error(f"Erreur lors du téléchargement Spotdl pour {url}: {e}")
            raise # Propage l'erreur
        # Le répertoire temporaire et son contenu sont automatiquement supprimés

# ==== TELEGRAM ====

bot = Bot(TOKEN)

async def send_audio(io_data: BytesIO, title: str) -> bool:
    """Envoie le fichier audio (BytesIO) au groupe Telegram avec tentatives."""
    # Nettoyer le nom de fichier pour Telegram (limiter la longueur et les caractères)
    safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_', '.'))[:58] + ".mp3" # Limite ~64, laisser place à .mp3
    io_data.name = safe_filename # Nommer le flux
    logger.info(f"Tentative d'envoi vers Telegram : {io_data.name}")

    retries = 0
    while retries <= MAX_RETRIES:
        try:
            io_data.seek(0) # Réinitialiser la position du flux avant l'envoi/la tentative
            await bot.send_audio(
                chat_id=GROUP_ID,
                audio=InputFile(io_data, filename=io_data.name),
                caption=title, # Utiliser le titre original dans la légende
                read_timeout=120, # Augmenter le timeout pour les gros fichiers/réseaux lents
                write_timeout=120,
                connect_timeout=60,
                pool_timeout=180, # Timeout total pour l'opération
            )
            logger.info(f"Envoyé avec succès à Telegram : {io_data.name}")
            return True # Succès
        except RetryAfter as e:
            # Contrôle de flux Telegram
            wait_time = e.retry_after + 2 # Ajouter une marge
            logger.warning(f"Contrôle de flux Telegram : Attente de {wait_time} secondes avant la tentative ({retries+1}/{MAX_RETRIES})...")
            await asyncio.sleep(wait_time)
            retries += 1
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Erreur réseau/Timeout Telegram lors de l'envoi de '{io_data.name}': {e}. Nouvelle tentative dans {RETRY_DELAY}s ({retries+1}/{MAX_RETRIES})...")
            await asyncio.sleep(RETRY_DELAY)
            retries += 1
        except Exception as e:
            # Capturer d'autres erreurs potentielles (ex: token invalide, chat introuvable, fichier trop gros)
            logger.error(f"Erreur Telegram inattendue lors de l'envoi de '{io_data.name}': {e}")
            # Pas de nouvelle tentative pour les erreurs inattendues
            return False # Échec

    logger.error(f"Échec de l'envoi de '{io_data.name}' à Telegram après {MAX_RETRIES} tentatives.")
    return False # Échec après tentatives


# ==== MAIN ====

async def main():
    """Fonction principale pour vérifier les sources et envoyer les nouvelles pistes."""
    logger.info("=== Début de l'exécution du Bot ===")
    success_count = 0
    error_count = 0
    something_processed = False # Indicateur pour savoir si on doit sauvegarder l'historique

    # --- Vérification YouTube ---
    logger.info("--- Vérification des chaînes YouTube ---")
    for channel_url in YOUTUBE_CHANS:
        # Vérifier si l'URL est une URL YouTube valide (simpliste)
        if not channel_url or not ("youtube.com/channel/" in channel_url or "youtube.com/@" in channel_url):
             logger.warning(f"URL YouTube invalide ou placeholder ignorée : {channel_url}")
             continue
        try:
            new_youtube_videos = list_new_videos(channel_url.strip())
            for vid, url, title in new_youtube_videos:
                if vid in processed["ytm"]: # Double vérification
                    continue

                audio_data = None # Initialiser pour le bloc finally
                try:
                    logger.info(f"Traitement YouTube : {title} ({url})")
                    audio_data = fetch_ytm_mp3(url)
                    if await send_audio(audio_data, title):
                        processed["ytm"].append(vid)
                        something_processed = True # Marquer pour sauvegarder
                        success_count += 1
                        await asyncio.sleep(5) # Délai plus long entre les envois pour éviter le flood
                    else:
                        error_count += 1
                        logger.error(f"Échec de l'envoi de la piste YouTube : {title}")
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de la vidéo YouTube {vid} ('{title}'): {e}")
                    error_count += 1
                finally:
                    # Fermer le flux BytesIO s'il a été créé
                    if audio_data:
                        audio_data.close()

        except Exception as e:
            logger.error(f"Erreur lors du traitement de la chaîne YouTube {channel_url}: {e}")
            error_count += 1 # Compter aussi les erreurs de traitement de chaîne

    # --- Vérification Spotify ---
    if sp and spdl: # Vérifier si les clients Spotify sont initialisés
        logger.info("--- Vérification des artistes Spotify ---")
        for artist_url in SPOTIFY_ARTS:
            if not artist_url or not "open.spotify.com/artist/" in artist_url:
                logger.warning(f"URL Spotify invalide ou placeholder ignorée : {artist_url}")
                continue
            try:
                new_spotify_tracks = list_new_spotify(artist_url.strip())
                for tid, url, title in new_spotify_tracks:
                    if tid in processed["spotify"]: # Double vérification
                         continue

                    audio_data = None # Initialiser pour le bloc finally
                    try:
                        logger.info(f"Traitement Spotify : {title} ({url})")
                        audio_data = fetch_spotify_mp3(url)
                        if await send_audio(audio_data, title):
                            processed["spotify"].append(tid)
                            something_processed = True # Marquer pour sauvegarder
                            success_count += 1
                            await asyncio.sleep(5) # Délai plus long
                        else:
                            error_count += 1
                            logger.error(f"Échec de l'envoi de la piste Spotify : {title}")
                    except Exception as e:
                        logger.error(f"Erreur lors du traitement de la piste Spotify {tid} ('{title}'): {e}")
                        error_count += 1
                    finally:
                        # Fermer le flux BytesIO
                        if audio_data:
                            audio_data.close()

            except Exception as e:
                logger.error(f"Erreur lors du traitement de l'artiste Spotify {artist_url}: {e}")
                error_count += 1
    else:
        logger.info("--- Vérification Spotify ignorée (clients non initialisés) ---")


    # Sauvegarder l'historique uniquement si quelque chose a été traité avec succès
    if something_processed:
        logger.info("Sauvegarde du fichier d'historique mis à jour.")
        save_hist()
    else:
        logger.info("Aucun nouvel élément traité, l'historique n'a pas besoin d'être sauvegardé.")


    logger.info(f"=== Exécution du Bot terminée : {success_count} envoyé(s), {error_count} erreur(s) ===")
    # Indiquer s'il y a eu des erreurs pour potentiellement notifier dans le workflow
    if error_count > 0:
        # Vous pourriez vouloir quitter avec un code d'erreur pour que l'action échoue
        # sys.exit(1)
        pass # Pour l'instant, on termine normalement même avec des erreurs


if __name__ == "__main__":
    # La vérification FFmpeg est moins critique ici car le workflow l'installe,
    # mais on peut la laisser comme sécurité supplémentaire si le script est exécuté localement.
    try:
        # Utiliser 'subprocess.run' avec capture de sortie pour éviter d'afficher la version
        result = subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True, text=True)
        logger.info("ffmpeg trouvé.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FATAL: ffmpeg introuvable ou non exécutable. Vérifiez l'installation dans le workflow ou localement.")
        sys.exit(1) # Quitter si ffmpeg manque

    # Exécuter la boucle d'événements asyncio
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Erreur non gérée dans la fonction main asyncio : {e}")
        sys.exit(1) # Quitter avec une erreur en cas d'exception majeure