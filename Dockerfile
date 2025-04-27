FROM python:3.10-slim

WORKDIR /app

# Installer ffmpeg systémique
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l’application
COPY . .

# Point d’entrée : lance bot.py
CMD ["python", "bot.py"]
