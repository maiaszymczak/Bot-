# BSG bot (Python + JS)

Ce repo contient:

- `bot.py` (version Python)
- `bot_js.js` (version Node.js / discord.js)

## Prérequis

- Un service account Google + accès au Google Sheet
- Le fichier `credentials.json` (service account) dans ce dossier **ou** `GOOGLE_SERVICE_ACCOUNT_FILE` dans `.env`

Pour le rôle Discord (utilisé pour auto-enregistrer dans la sheet):

- `BSG_MEMBER_ROLE_ID=...` (recommandé) ou
- `BSG_MEMBER_ROLE_NAME=...` (par défaut: `bsg membre`)

Pour la sheet `JOUEURS`:

- `SHEET_HEADER_ROWS=3` (par défaut). Mets `0` si tu n'as pas de lignes d'entête.

## Node.js (recommandé si ton exemple JS marche déjà)

```zsh
cd bsg
npm install
npm run check:gsheets
npm start
```

### Déploiement Wispbyte (GitHub)

- `Git Repo Address`: l'URL du repo
- `Install Branch`: `main`
- `JS file`: `index.js`
- Variables d'env à définir dans le panel:
  - `DISCORD_TOKEN` (obligatoirel)
  - `GUILD_ID` (obligatoire)
  - `SHEET_ID` (obligatoire)
  - Auth Google: mets `credentials.json` dans `bsg/credentials.json` **ou** définis `GOOGLE_SERVICE_ACCOUNT_FILE` vers ton JSON
  - Optionnel: `BSG_MEMBER_ROLE_ID` (recommandé) ou `BSG_MEMBER_ROLE_NAME`
  - Optionnel: `SHEET_HEADER_ROWS`, `AUTO_DELETE_SECONDS`

Commandes (slash): `/register`, `/money`, `/top`, `/stats`, `/activity`, `/checkmembers`.

Messages non permanents:

- Par défaut, chaque réponse se supprime après 5 minutes avec un compteur.
- Optionnel: `AUTO_DELETE_SECONDS=300` dans `.env`.

## Python

```zsh
cd bsg
/usr/bin/python3 check_gsheets.py
/usr/bin/python3 bot.py
```
