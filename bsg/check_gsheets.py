import os
import sys
from pathlib import Path

from dotenv import load_dotenv

import sheets


def _validate_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    try:
        text = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return

    warnings: list[str] = []
    for idx, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip("\r\n")
        if not line or line.lstrip().startswith("#"):
            continue

        if "=" not in line:
            warnings.append(
                f"- Ligne {idx}: pas de '=' (probable retour à la ligne au milieu d'une valeur)"
            )
            continue

        key = line.split("=", 1)[0]
        if key != key.strip():
            warnings.append(f"- Ligne {idx}: espaces avant/après le nom de variable")
        if any(ch.isspace() for ch in key):
            warnings.append(
                f"- Ligne {idx}: nom de variable contient des espaces (ligne probablement cassée)"
            )

    if warnings:
        print("ATTENTION: ton `.env` semble mal formaté (lignes cassées).")
        print("Ça peut casser DISCORD_TOKEN et/ou GOOGLE_PRIVATE_KEY.")
        print("".join([w + "\n" for w in warnings]).rstrip())
        print("➡️  Ouvre `.env` et assure-toi que chaque variable est sur une seule ligne: KEY=VALUE")


def main() -> int:
    here = Path(__file__).resolve().parent
    env_path = here / ".env"
    _validate_env_file(env_path)
    load_dotenv(env_path)

    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        print("ERREUR: SHEET_ID manquant dans .env")
        return 2

    local_credentials = (here / "credentials.json")
    local_service_account = (here / "service_account.json")

    service_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    private_key_file = os.getenv("GOOGLE_PRIVATE_KEY_FILE")

    if not local_credentials.exists() and not local_service_account.exists() and not service_file and not private_key_file:
        print(
            "ERREUR: auth Google non configurée.\n\n"
            "Option 0 (comme ton bot JS):\n"
            "  - Mets le fichier `credentials.json` (service account) à côté de ce script: bsg/credentials.json\n\n"
            "Option A (recommandée): JSON service account\n"
            "  - Place le JSON sur disque et ajoute dans .env:\n"
            "    GOOGLE_SERVICE_ACCOUNT_FILE=/chemin/vers/service_account.json\n\n"
            "Option B: clé PEM (si tu n'as pas le JSON)\n"
            "  - Mets la clé dans un fichier .pem et ajoute dans .env:\n"
            "    GOOGLE_PRIVATE_KEY_FILE=/chemin/vers/private_key.pem\n"
            "    GOOGLE_SERVICE_ACCOUNT_EMAIL=ton-compte@projet.iam.gserviceaccount.com\n"
        )
        return 2

    # Test worksheets
    joueurs = sheets.get_sheet("JOUEURS")
    activites = sheets.get_sheet("ACTIVITES")

    if not joueurs or not activites:
        print("ERREUR: impossible d'ouvrir JOUEURS/ACTIVITES. Regarde le log au-dessus.")
        return 1

    # Lightweight read checks
    try:
        j_title = joueurs.title
        a_title = activites.title
        j_rows = joueurs.row_count
        a_rows = activites.row_count
        print("OK: connexion Google Sheets")
        print(f"- JOUEURS: '{j_title}' (rows={j_rows})")
        print(f"- ACTIVITES: '{a_title}' (rows={a_rows})")
        return 0
    except Exception as e:
        print(f"ERREUR: lecture worksheets échouée: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
