import "dotenv/config";

import { getSheetByName } from "./sheets_js.js";

try {
  const joueurs = await getSheetByName("JOUEURS");
  const activites = await getSheetByName("ACTIVITES");

  console.log("OK: connexion Google Sheets");
  console.log(`- JOUEURS: '${joueurs.title}' (rows=${joueurs.rowCount}, cols=${joueurs.columnCount})`);
  console.log(`- ACTIVITES: '${activites.title}' (rows=${activites.rowCount}, cols=${activites.columnCount})`);
} catch (e) {
  console.error("ERREUR:", e?.message ?? e);
  console.error(
    "\nFix rapide (comme ton bot JS):\n" +
      "- Copie ton fichier service account JSON ici: /Users/maia/Desktop/bsg/credentials.json\n" +
      "  (ou définis GOOGLE_SERVICE_ACCOUNT_FILE=/chemin/vers/credentials.json dans .env)\n"
  );
  process.exitCode = 1;
}
