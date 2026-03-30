# Commandes du bot (Albion PVP/PVE)

Toutes les commandes sont des **slash commands** (tape `/` puis le nom).

## Commandes membres

- `/register [membre]` — Enregistre un membre dans la sheet (si `membre` est vide, ça t’enregistre toi).
  - Exemple: `/register`
  - Exemple: `/register membre:@Pseudo`

- `/money [membre]` — Affiche le coffre/solde (toi ou un membre).
  - Exemple: `/money`
  - Exemple: `/money membre:@Pseudo`

- `/top type:<bal|money|participation|regear>` — Affiche un classement.
  - `bal` / `money` : argent (selon la colonne configurée dans la sheet)
  - `participation` : participations
  - `regear` : regear
  - Exemple: `/top type:money`

- `/stats` — Stats globales (participations).
  - Exemple: `/stats`

- `/activity list [n]` — Liste les dernières activités (max 20).
  - Exemple: `/activity list`
  - Exemple: `/activity list n:15`

- `/activity detail id:<ID>` — Détail d’une activité à partir de son ID.
  - Exemple: `/activity detail id:12345`

## Commandes staff

- `/checknames` — Compare **noms Discord** vs **noms dans la sheet** (par ID Discord) et propose des corrections.
  - Le bot affiche une proposition puis tu valides avec **Appliquer** ou **Annuler**.
  - Accès: admins / manage serveur, ou rôles listés dans `BSG_STAFF_ROLE_IDS`.

## Commande diagnostic

- `/checkmembers` — Compare le nombre de membres (rôle Discord) vs le nombre d’IDs enregistrés dans la sheet.
  - Note: si tu vois `(cache)`, le chiffre Discord peut être incomplet.
  - Pour un chiffre exact, active `BSG_CHECKMEMBERS_FETCH_ALL_MEMBERS=true` (puis redémarre le bot).
