import base64
import json
import os
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials


def _get_scopes():
    return [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]


def _credentials_from_env():
    scopes = _get_scopes()

    # JS-style: if a local credentials file exists in the project, use it.
    project_dir = Path(__file__).resolve().parent
    for candidate in (project_dir / "credentials.json", project_dir / "service_account.json"):
        if candidate.exists():
            return Credentials.from_service_account_file(str(candidate), scopes=scopes)

    service_account_file = (
        os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if service_account_file:
        if not os.path.exists(service_account_file):
            print(
                "Error: fichier JSON service account introuvable: "
                f"{service_account_file}\n"
                "➡️  Vérifie le chemin ou exporte un nouveau JSON depuis Google Cloud."
            )
            return None
        return Credentials.from_service_account_file(service_account_file, scopes=scopes)

    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if service_account_json:
        info = json.loads(service_account_json)
        return Credentials.from_service_account_info(info, scopes=scopes)

    # Alternative: store the private key in a PEM file (more reliable than .env for long secrets)
    private_key_file = os.getenv("GOOGLE_PRIVATE_KEY_FILE")
    if private_key_file:
        client_email = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
        if not client_email:
            print(
                "Error: GOOGLE_SERVICE_ACCOUNT_EMAIL manquant. "
                "Il est requis avec GOOGLE_PRIVATE_KEY_FILE."
            )
            return None
        if not os.path.exists(private_key_file):
            print(f"Error: fichier PEM introuvable: {private_key_file}")
            return None
        try:
            private_key = open(private_key_file, "r", encoding="utf-8").read()
        except Exception as e:
            print(f"Error: impossible de lire le fichier PEM: {e}")
            return None

        key_lines = [ln.strip() for ln in private_key.splitlines() if ln.strip()]
        body = "".join([ln for ln in key_lines if not ln.startswith("-----")])
        try:
            base64.b64decode(body, validate=True)
        except Exception as e:
            print("Error: clé PEM invalide (Base64).")
            print(f"- Longueur body: {len(body)} (mod 4 = {len(body) % 4})")
            print(f"- Détail decode Base64: {e}")
            print(
                "➡️  Ça arrive quand la clé est tronquée ou copiée avec des caractères manquants. "
                "Le moyen le plus fiable est de télécharger le JSON du service account (Option A) "
                "ou de ré-exporter une nouvelle clé depuis Google Cloud."
            )
            return None

        info = {
            "type": "service_account",
            "project_id": "valiant-carrier-344623",
            "private_key": private_key,
            "client_email": client_email,
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        return Credentials.from_service_account_info(info, scopes=scopes)

    client_email = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
    private_key_raw = os.getenv("GOOGLE_PRIVATE_KEY")
    if not client_email or not private_key_raw:
        return None

    private_key = private_key_raw.strip().strip('"').replace("\\n", "\n")

    # Validate Base64 body early to provide a clear error.
    key_lines = [ln.strip() for ln in private_key.splitlines() if ln.strip()]
    body = "".join([ln for ln in key_lines if not ln.startswith("-----")])
    try:
        base64.b64decode(body, validate=True)
    except Exception as e:
        print(
            "Error: GOOGLE_PRIVATE_KEY est illisible côté Python (Base64 invalide). "
            "Solution recommandée: utilisez un fichier JSON de service account et settez "
            "GOOGLE_SERVICE_ACCOUNT_FILE=/chemin/vers/credentials.json (ou GOOGLE_APPLICATION_CREDENTIALS)."
        )
        print(f"- Longueur body: {len(body)} (mod 4 = {len(body) % 4})")
        print(f"- Détail decode Base64: {e}")
        print(
            "Hint: si tu as déjà un bot JS qui marche avec `keyFile: 'credentials.json'`, "
            "copie ce `credentials.json` dans ce projet (`/Users/maia/Desktop/bsg/credentials.json`)."
        )
        return None

    info = {
        "type": "service_account",
        "project_id": "valiant-carrier-344623",
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    return Credentials.from_service_account_info(info, scopes=scopes)


def get_sheet(sheet_name=None):
    """Connecte à Google Sheets et retourne un onglet (worksheet)."""
    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        print("Error: SHEET_ID manquant dans l'environnement.")
        return None

    try:
        creds = _credentials_from_env()
        if not creds:
            print(
                "Error: identifiants Google manquants. "
                "Fournissez GOOGLE_SERVICE_ACCOUNT_FILE (recommandé) ou GOOGLE_SERVICE_ACCOUNT_EMAIL + GOOGLE_PRIVATE_KEY."
            )
            return None

        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet(sheet_name) if sheet_name else spreadsheet.sheet1
        print(f"Successfully connected to Google Sheet: '{sheet.title}'.")
        return sheet
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        msg = str(e)
        if "Unable to load PEM file" in msg or "InvalidByte" in msg:
            print(
                "Hint: ce message arrive souvent quand la clé privée est mal lue depuis .env. "
                "La solution la plus fiable est d'utiliser le JSON du service account via "
                "GOOGLE_SERVICE_ACCOUNT_FILE=/chemin/vers/service_account.json."
            )
        return None

# Column mapping based on the screenshot
COL_NOM = 'C'
COL_SOLDE = 'D'
COL_CUMUL = 'E'
COL_ID_DISCORD = 'F'
COL_PARTICIPATIONS = 'G'
COL_REGEAR = 'H'

# ... (keep get_sheet function as is) ...

def find_user_row(sheet, user_id):
    """Finds a user's row by their Discord ID in the sheet."""
    try:
        # gspread's find method is simple. For column-specific search, it's better to get all values.
        id_list = sheet.col_values(gspread.utils.a1_to_rowcol(f'{COL_ID_DISCORD}1')[1])
        for i, id_val in enumerate(id_list):
            if str(id_val) == str(user_id):
                return i + 1 # Return 1-based row index
        return None
    except Exception as e:
        print(f"An error occurred while searching for user {user_id}: {e}")
        return None

def add_user(sheet, user_id, user_name):
    """Adds a new user to the sheet with default values."""
    try:
        if find_user_row(sheet, user_id) is None:
            # Create a new row with default values in the correct columns
            # This is a bit tricky as we need to create a list with many empty spots
            # Assuming the sheet has columns up to H.
            new_row = [''] * 8 # Create a list for A-H
            new_row[gspread.utils.a1_to_rowcol(f'{COL_NOM}1')[1] - 1] = user_name
            new_row[gspread.utils.a1_to_rowcol(f'{COL_SOLDE}1')[1] - 1] = '0 €'
            new_row[gspread.utils.a1_to_rowcol(f'{COL_CUMUL}1')[1] - 1] = '0 €'
            new_row[gspread.utils.a1_to_rowcol(f'{COL_ID_DISCORD}1')[1] - 1] = str(user_id)
            new_row[gspread.utils.a1_to_rowcol(f'{COL_PARTICIPATIONS}1')[1] - 1] = 0
            new_row[gspread.utils.a1_to_rowcol(f'{COL_REGEAR}1')[1] - 1] = 0
            sheet.append_row(new_row)
            print(f"Added user {user_name} ({user_id}) to the sheet.")
            return True
        else:
            print(f"User {user_name} ({user_id}) already in the sheet.")
            return False
    except Exception as e:
        print(f"Error adding user {user_name} ({user_id}) to the sheet: {e}")
        return False

def update_user_name(sheet, user_id, new_name):
    """Updates the name of a user in the sheet."""
    try:
        row = find_user_row(sheet, user_id)
        if row:
            sheet.update(f'{COL_NOM}{row}', new_name)
            print(f"Updated username for {user_id} to {new_name}.")
            return True
        else:
            print(f"Could not find user with ID {user_id} to update name.")
            return False
    except Exception as e:
        print(f"Error updating username for {user_id}: {e}")
        return False

def get_balance(sheet, user_id):
    """Gets the balance and cumulative earnings for a user."""
    try:
        row = find_user_row(sheet, user_id)
        if row:
            values = sheet.row_values(row)
            name = values[gspread.utils.a1_to_rowcol(f'{COL_NOM}1')[1] - 1]
            balance = values[gspread.utils.a1_to_rowcol(f'{COL_SOLDE}1')[1] - 1]
            cumulative = values[gspread.utils.a1_to_rowcol(f'{COL_CUMUL}1')[1] - 1]
            return {"name": name, "balance": balance, "cumulative": cumulative}
        else:
            return None
    except Exception as e:
        print(f"Error getting balance for user {user_id}: {e}")
        return None

def get_top_players(sheet, sort_col, top_n=10):
    """Gets the top N players based on a specified column."""
    try:
        # Get all data, skipping header rows (assuming 3 headers)
        all_data = sheet.get_all_values()[3:]
        
        name_col_index = gspread.utils.a1_to_rowcol(f'{COL_NOM}1')[1] - 1
        sort_col_index = gspread.utils.a1_to_rowcol(f'{sort_col}1')[1] - 1

        # Filter out rows with invalid data
        valid_data = []
        for row in all_data:
            try:
                # Ensure row has enough columns and the value is a number
                if len(row) > sort_col_index and len(row) > name_col_index:
                    # Clean the value: remove '€', spaces, and convert to number
                    value_str = row[sort_col_index].replace('€', '').replace(' ', '').replace(',', '.')
                    if value_str:
                        valid_data.append((row[name_col_index], float(value_str)))
            except (ValueError, IndexError):
                continue # Skip rows that can't be converted

        # Sort the data
        sorted_data = sorted(valid_data, key=lambda x: x[1], reverse=True)
        
        return sorted_data[:top_n]
    except Exception as e:
        print(f"Error getting top players for column {sort_col}: {e}")
        return []

def get_column_sum(sheet, column):
    """Calculates the sum of a numeric column."""
    try:
        # Get all values from the column, skipping header rows
        col_values = sheet.col_values(gspread.utils.a1_to_rowcol(f'{column}1')[1])[3:]
        
        total = 0
        for value in col_values:
            try:
                # Clean and convert to number
                num_value = float(value.replace('€', '').replace(' ', '').replace(',', '.'))
                total += num_value
            except (ValueError, AttributeError):
                continue # Ignore non-numeric values
        return total
    except Exception as e:
        print(f"Error calculating sum for column {column}: {e}")
        return 0

def get_activities(sheet, count=10):
    """Gets the last 'count' activities from the 'ACTIVITES' sheet."""
    try:
        # Assuming headers are on row 3, data starts on row 4
        all_activities = sheet.get_all_records(head=3)
        # Reverse to get the latest first, and filter out empty rows
        latest_activities = [act for act in reversed(all_activities) if act.get('ID')]
        return latest_activities[:count]
    except Exception as e:
        print(f"Error getting activities: {e}")
        return []

def get_activity_details(sheet, activity_id):
    """Gets all details for a specific activity ID."""
    try:
        all_activities = sheet.get_all_records(head=3)
        for activity in all_activities:
            if str(activity.get('ID')) == str(activity_id):
                return activity
        return None
    except Exception as e:
        print(f"Error getting activity details for ID {activity_id}: {e}")
        return None

def count_registered_users(sheet):
    """Counts the number of registered users in the sheet."""
    try:
        # Get all values from the ID column, skipping headers
        id_list = sheet.col_values(gspread.utils.a1_to_rowcol(f'{COL_ID_DISCORD}1')[1])[3:]
        # Filter out empty cells
        non_empty_ids = [id_val for id_val in id_list if id_val]
        return len(non_empty_ids)
    except Exception as e:
        print(f"Error counting registered users: {e}")
        return 0
