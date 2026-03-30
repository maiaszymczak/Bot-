import "dotenv/config";
import http from "http";
import {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ApplicationCommandOptionType,
  MessageFlags,
  PermissionsBitField,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  Routes,
} from "discord.js";

import {
  COL_CUMUL,
  COL_PARTICIPATIONS,
  COL_REGEAR,
  COL_SOLDE,
  addUser,
  countRegisteredUsers,
  getBalance,
  getColumnSum,
  getSheetByName,
  getTopPlayers,
  getActivityById,
  invalidateSheetCache,
  listRegisteredDiscordUsers,
  listActivities,
  updateUserName,
} from "./sheets_js.js";

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const GUILD_ID = process.env.GUILD_ID;

if (!DISCORD_TOKEN) throw new Error("DISCORD_TOKEN manquant (variable d'environnement)");
if (!GUILD_ID) throw new Error("GUILD_ID manquant (variable d'environnement)");

const ROLE_ID = process.env.BSG_MEMBER_ROLE_ID || null;
const ROLE_NAME = process.env.BSG_MEMBER_ROLE_NAME || "Membre";

function parseBoolEnv(value, defaultValue = false) {
  if (value == null) return defaultValue;
  const s = String(value).trim().toLowerCase();
  if (!s) return defaultValue;
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return defaultValue;
}

// IMPORTANT: `guild.members.fetch()` sans argument déclenche une requête Gateway opcode 8
// (Request Guild Members) qui est fortement rate-limitée. On évite donc par défaut.
const BSG_STARTUP_SYNC_ENABLED = parseBoolEnv(process.env.BSG_STARTUP_SYNC_ENABLED, true);
const BSG_STARTUP_SYNC_MODE = String(process.env.BSG_STARTUP_SYNC_MODE ?? "rest").trim().toLowerCase();
const BSG_CHECKMEMBERS_FETCH_ALL_MEMBERS = parseBoolEnv(
  process.env.BSG_CHECKMEMBERS_FETCH_ALL_MEMBERS,
  false
);

async function listGuildMembersViaRest(guildId) {
  const out = [];
  let after = null;

  // Pagination: 1000 max par page.
  // NOTE: évite l'opcode 8 (gateway) et donc son rate limit.
  // IMPORTANT: sur très gros serveurs ça peut prendre un peu de temps.
  for (;;) {
    const query = new URLSearchParams();
    query.set("limit", "1000");
    if (after) query.set("after", after);

    // discord.js REST accepte { query } (URLSearchParams)
    const page = await client.rest.get(Routes.guildMembers(guildId), { query });
    if (!Array.isArray(page) || page.length === 0) break;

    out.push(...page);

    const last = page[page.length - 1];
    const lastId = last?.user?.id;
    if (!lastId || page.length < 1000) break;
    after = String(lastId);
  }

  return out;
}

function getApiMemberDisplayName(m) {
  const nick = String(m?.nick ?? "").trim();
  if (nick) return nick;
  const globalName = String(m?.user?.global_name ?? "").trim();
  if (globalName) return globalName;
  return String(m?.user?.username ?? "").trim();
}

const STAFF_ROLE_IDS = new Set(
  String(process.env.BSG_STAFF_ROLE_IDS ?? "")
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter(Boolean)
);

async function replyEphemeral(interaction, payload) {
  try {
    if (interaction.replied) {
      await interaction.followUp({ flags: MessageFlags.Ephemeral, ...payload });
    } else {
      await interaction.editReply({ ...payload });
    }
  } catch {
    try {
      await interaction.followUp({ flags: MessageFlags.Ephemeral, ...payload });
    } catch {
      // impossible de répondre
    }
  }
}

async function followUpEphemeral(interaction, payload) {
  await interaction.followUp({ flags: MessageFlags.Ephemeral, ...payload });
}

function norm(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function memberHasRoleByName(member, roleName) {
  const target = norm(roleName);
  return member.roles.cache.some((r) => norm(r.name) === target);
}

function memberHasRole(member) {
  if (ROLE_ID) return member.roles.cache.has(ROLE_ID);
  return memberHasRoleByName(member, ROLE_NAME);
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

client.on("error", (err) => {
  console.error("Discord client error:", err);
});
client.on("shardError", (err) => {
  console.error("Discord shard error:", err);
});

let joueursSheet;
let activitesSheet;
let sheetsLoadedAt = 0;
const SHEETS_META_TTL_MS = 5 * 60 * 1000;

const NAME_FIX_TTL_MS = 10 * 60 * 1000;
const pendingNameFixes = new Map();

function cleanupPendingFixes() {
  const now = Date.now();
  for (const [token, obj] of pendingNameFixes.entries()) {
    if (!obj?.createdAt || now - obj.createdAt > NAME_FIX_TTL_MS) pendingNameFixes.delete(token);
  }
}

function makeToken() {
  return Math.random().toString(36).slice(2, 10);
}

function normNameSimple(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function namesRoughlyMatch(a, b) {
  const x = normNameSimple(a);
  const y = normNameSimple(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}

function isStaffInteraction(interaction) {
  const perms = interaction.memberPermissions;
  if (
    perms &&
    (perms.has(PermissionsBitField.Flags.Administrator) ||
      perms.has(PermissionsBitField.Flags.ManageGuild))
  ) {
    return true;
  }

  if (!STAFF_ROLE_IDS.size) return false;

  const roles = interaction.member?.roles;
  // GuildMember
  if (roles?.cache?.has) {
    for (const id of STAFF_ROLE_IDS) {
      if (roles.cache.has(id)) return true;
    }
    return false;
  }
  // APIInteractionGuildMember
  if (Array.isArray(roles)) {
    return roles.some((id) => STAFF_ROLE_IDS.has(String(id)));
  }
  return false;
}

function formatTopLines(rows) {
  return rows
    .map((r, i) => {
      const rank = String(i + 1).padStart(2, "0");
      return { rank, name: r.name, value: r.value };
    });
}

function clamp(s, max) {
  const str = String(s ?? "");
  if (str.length <= max) return str;
  return str.slice(0, Math.max(0, max - 1)) + "…";
}

function formatNumberFr(n) {
  try {
    return new Intl.NumberFormat("fr-FR").format(n);
  } catch {
    return String(n);
  }
}

function makeTextTable({ headers, rows, widths }) {
  const pad = (v, w) => String(v ?? "").padEnd(w, " ");
  const line = (cells) => cells.map((c, idx) => pad(c, widths[idx])).join("  ").trimEnd();

  const out = [];
  out.push(line(headers));
  out.push(line(headers.map((h, i) => "-".repeat(Math.min(widths[i], String(h).length || 3)))));
  for (const r of rows) out.push(line(r));
  return "```text\n" + out.join("\n") + "\n```";
}

function renderTopTable(items, { isMoney }) {
  const widths = [4, 22, 14];
  const headers = ["#", "Nom", isMoney ? "€" : "Valeur"];
  const rows = items.map((it) => {
    const value = isMoney ? `${formatNumberFr(it.value)} €` : formatNumberFr(it.value);
    return [
      clamp(it.rank, widths[0]),
      clamp(it.name, widths[1]),
      clamp(value, widths[2]),
    ];
  });
  return makeTextTable({ headers, rows, widths });
}

function renderActivitiesTable(items) {
  const widths = [10, 12, 32];
  const headers = ["ID", "Date", "Titre"];
  const rows = items.map((it) => [
    clamp(it.id, widths[0]),
    clamp(it.date || "", widths[1]),
    clamp(it.title || "", widths[2]),
  ]);
  return makeTextTable({ headers, rows, widths });
}

async function ensureSheetsLoaded() {
  if (joueursSheet && activitesSheet && Date.now() - sheetsLoadedAt < SHEETS_META_TTL_MS) return;
  joueursSheet = await getSheetByName("JOUEURS");
  activitesSheet = await getSheetByName("ACTIVITES");
  sheetsLoadedAt = Date.now();
}

async function getGuildAndRole() {
  const guild = await client.guilds.fetch(GUILD_ID);
  const roles = await guild.roles.fetch();

  let role = null;
  if (ROLE_ID) {
    try {
      role = await guild.roles.fetch(ROLE_ID);
    } catch {
      role = null;
    }
    if (!role) {
      console.warn(`Rôle introuvable via BSG_MEMBER_ROLE_ID=${ROLE_ID}`);
    }
  }

  if (!role) {
    const target = norm(ROLE_NAME);
    role = roles.find((r) => norm(r?.name) === target) ?? null;
    if (!role) role = roles.find((r) => norm(r?.name).includes(target)) ?? null;
  }

  if (!role) {
    const names = roles.map((r) => r.name);
    console.warn(
      `Rôle introuvable. Configure BSG_MEMBER_ROLE_NAME ou BSG_MEMBER_ROLE_ID. Exemples de rôles: ${names
        .slice(0, 15)
        .join(" | ")}`
    );
  }

  return { guild, role };
}

async function registerGuildCommands() {
  const { guild } = await getGuildAndRole();
  await guild.commands.set([
    {
      name: "register",
      description: "Enregistre un membre dans la sheet (toi ou quelqu'un d'autre)",
      options: [
        {
          name: "membre",
          description: "Membre à enregistrer (laisser vide pour toi-même)",
          type: ApplicationCommandOptionType.User,
          required: false,
        },
      ],
    },
    {
      name: "money",
      description: "Affiche ton coffre de guilde (ou celui d'un membre)",
      options: [
        {
          name: "membre",
          description: "Membre à consulter",
          type: ApplicationCommandOptionType.User,
          required: false,
        },
      ],
    },
    {
      name: "top",
      description: "Classements (solde, cumul, participations, regear)",
      options: [
        {
          name: "type",
          description: "Type de classement",
          type: ApplicationCommandOptionType.String,
          required: true,
          choices: [
            { name: "bal", value: "bal" },
            { name: "money", value: "money" },
            { name: "participation", value: "participation" },
            { name: "regear", value: "regear" },
          ],
        },
      ],
    },
    {
      name: "stats",
      description: "Stats globales (participations)",
    },
    {
      name: "activity",
      description: "Lire les activités",
      options: [
        {
          name: "list",
          description: "Liste les dernières activités",
          type: ApplicationCommandOptionType.Subcommand,
          options: [
            {
              name: "n",
              description: "Nombre d'items (max 20)",
              type: ApplicationCommandOptionType.Integer,
              required: false,
            },
          ],
        },
        {
          name: "detail",
          description: "Détail d'une activité",
          type: ApplicationCommandOptionType.Subcommand,
          options: [
            {
              name: "id",
              description: "ID de l'activité",
              type: ApplicationCommandOptionType.String,
              required: true,
            },
          ],
        },
      ],
    },
    {
      name: "checkmembers",
      description: "Compare Discord vs Sheet (membres enregistrés)",
    },
    {
      name: "checknames",
      description: "Vérifie les noms (sheet vs Discord) et propose une correction",
    },
  ]);
}

async function startupSyncRoleMembers() {
  if (!BSG_STARTUP_SYNC_ENABLED) {
    console.log("Startup sync: désactivé (BSG_STARTUP_SYNC_ENABLED=false)");
    return;
  }

  const { guild, role } = await getGuildAndRole();
  if (!role) {
    console.warn(`Rôle introuvable pour sync: ${ROLE_NAME}`);
    return;
  }

  const mode = BSG_STARTUP_SYNC_MODE;
  if (mode === "off" || mode === "disabled" || mode === "false" || mode === "0") {
    console.log("Startup sync: mode=off");
    return;
  }

  if (mode === "gateway") {
    // ⚠️ Peut déclencher le rate limit opcode 8.
    await guild.members.fetch();
    const roleMembers = role.members;
    console.log(`Sync (gateway): ${roleMembers.size} membres avec rôle '${role.name}'`);

    invalidateSheetCache(joueursSheet);
    let added = 0;
    for (const member of roleMembers.values()) {
      const display = member.displayName;
      if (await addUser(joueursSheet, member.id, display)) added++;
    }
    console.log(`Sync terminé: ajoutés=${added}`);
    return;
  }

  if (mode === "rest") {
    // Évite l'opcode 8 en listant via REST.
    let apiMembers;
    try {
      apiMembers = await listGuildMembersViaRest(guild.id);
    } catch (e) {
      console.warn(`Startup sync (rest) a échoué (${e?.message ?? e}). Fallback mode=cache.`);
      apiMembers = null;
    }

    if (apiMembers) {
      const roleId = role.id;
      const roleMembers = apiMembers.filter((m) => Array.isArray(m?.roles) && m.roles.includes(roleId));
      console.log(`Sync (rest): ${roleMembers.length} membres avec rôle '${role.name}'`);

      invalidateSheetCache(joueursSheet);
      let added = 0;
      for (const m of roleMembers) {
        const id = m?.user?.id;
        if (!id) continue;
        const display = getApiMemberDisplayName(m);
        if (await addUser(joueursSheet, String(id), display)) added++;
      }
      console.log(`Sync terminé: ajoutés=${added}`);
      return;
    }
  }

  // mode=cache (ou fallback)
  {
    const roleMembers = role.members;
    console.log(`Sync (cache): ${roleMembers.size} membres avec rôle '${role.name}'`);
    console.log(
      "Sync (cache): sans fetch global des membres. Si le count est trop bas, mets BSG_STARTUP_SYNC_MODE=rest (recommandé) ou gateway."
    );

    invalidateSheetCache(joueursSheet);
    let added = 0;
    for (const member of roleMembers.values()) {
      const display = member.displayName;
      if (await addUser(joueursSheet, member.id, display)) added++;
    }
    console.log(`Sync terminé: ajoutés=${added}`);
  }
}

client.once("ready", async () => {
  console.log(`Connecté en tant que ${client.user?.tag}`);
  await ensureSheetsLoaded();
  console.log("Sheets: OK");

  try {
    await registerGuildCommands();
    console.log("Slash commands: OK (guild)");
  } catch (e) {
    console.error("Slash commands: FAIL", e);
  }

  try {
    await startupSyncRoleMembers();
  } catch (e) {
    console.error("Startup sync: FAIL", e);
  }
});

client.on("guildMemberUpdate", async (oldMember, newMember) => {
  try {
    await ensureSheetsLoaded();

    const oldHas = memberHasRole(oldMember);
    const newHas = memberHasRole(newMember);
    if (!oldHas && newHas) {
      const display = newMember.displayName;
      const added = await addUser(joueursSheet, newMember.id, display);
      if (added) console.log(`Ajout sheet: ${display} (${newMember.id})`);
    }

    const oldName = oldMember.displayName;
    const newName = newMember.displayName;
    if (oldName !== newName) {
      const updated = await updateUserName(joueursSheet, newMember.id, newName);
      if (updated) console.log(`Nom maj sheet: ${newMember.id} -> ${newName}`);
    }
  } catch (e) {
    console.error("guildMemberUpdate error:", e);
  }
});

client.on("interactionCreate", async (interaction) => {
  cleanupPendingFixes();
  if (interaction.guildId !== GUILD_ID) return;

  if (interaction.isButton()) {
    const id = String(interaction.customId ?? "");
    if (!id.startsWith("checknames:")) return;

    if (!isStaffInteraction(interaction)) {
      // Best-effort ACK, then refuse.
      try {
        if (!interaction.deferred && !interaction.replied) {
          await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        }
      } catch {
        // ignore
      }
      await replyEphemeral(interaction, { content: "❌ Réservé aux admins/staff." });
      return;
    }

    if (!interaction.deferred && !interaction.replied) {
      try {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      } catch {
        return;
      }
    }

    const [, action, token] = id.split(":");
    const pending = token ? pendingNameFixes.get(token) : null;
    if (!pending) {
      await replyEphemeral(interaction, { content: "⏳ Proposition expirée. Relance `/checknames`." });
      return;
    }
    if (pending.userId !== interaction.user.id) {
      await replyEphemeral(interaction, { content: "❌ Seule la personne qui a lancé `/checknames` peut valider." });
      return;
    }

    if (action === "cancel") {
      pendingNameFixes.delete(token);
      await replyEphemeral(interaction, { content: "✅ Correction annulée." });
      return;
    }

    if (action !== "apply") {
      await replyEphemeral(interaction, { content: "Action inconnue." });
      return;
    }

    pendingNameFixes.delete(token);
    try {
      await ensureSheetsLoaded();
      invalidateSheetCache(joueursSheet);

      let updated = 0;
      for (const ch of pending.changes) {
        if (!ch?.discordId || !ch?.to) continue;
        const ok = await updateUserName(joueursSheet, ch.discordId, ch.to);
        if (ok) updated++;
      }
      await replyEphemeral(interaction, { content: `✅ Noms corrigés: ${updated}/${pending.changes.length}` });
    } catch (e) {
      const msg = e?.message ?? String(e);
      await replyEphemeral(interaction, { content: `Erreur: ${msg}` });
    }
    return;
  }

  if (!interaction.isChatInputCommand()) return;

  if (!interaction.deferred && !interaction.replied) {
    try {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral });
    } catch (e) {
      // Une autre instance a déjà pris cet événement → on abandonne
      console.warn(`deferReply échoué pour ${interaction.commandName} (${interaction.id}): ${e?.message}`);
      return;
    }
  }

  try {
    await ensureSheetsLoaded();

    if (interaction.commandName === "register") {
      const targetUser = interaction.options.getUser("membre") ?? interaction.user;
      const { guild } = await getGuildAndRole();
      const member = await guild.members.fetch(targetUser.id);
      const display = member.displayName;
      const added = await addUser(joueursSheet, targetUser.id, display);
      await replyEphemeral(interaction, {
        content: added
          ? `✅ Enregistré dans la sheet: ${display}`
          : `ℹ️ ${display} est déjà enregistré(e) dans la sheet.`,
      });
      return;
    }

    if (interaction.commandName === "money") {
      const user = interaction.options.getUser("membre") ?? interaction.user;
      const memberDisplayName = await (async () => {
        try {
          const guild = interaction.guild;
          if (!guild) return null;
          const member = await guild.members.fetch(user.id);
          return member?.displayName ? String(member.displayName) : null;
        } catch {
          return null;
        }
      })();
      let bal = await getBalance(joueursSheet, user.id);
      if (!bal) {
        const hint =
          user.id === interaction.user.id
            ? "\n➡️  Fais `/register` pour t'ajouter automatiquement."
            : "";
        await replyEphemeral(interaction, {
          content: `❌ Pas trouvé dans la sheet: ${user.username}${hint}`,
        });
        return;
      }

      const guildName = interaction.guild?.name ?? "Guilde";
      const aventurier = (memberDisplayName || bal.name || user.username || "-").trim();
      const stripEuro = (s) => (s || "0").trim().replace(/\s*€\s*$/, "").trim();
      const soldeRaw = stripEuro(bal.balance);
      const cumulRaw = stripEuro(bal.cumulative);
      const embed = new EmbedBuilder()
        .setTitle(`🏦 Bank - ${guildName} -`)
        .setDescription("Voici le contenu de ton coffre de guilde")
        .addFields(
          { name: "👤 Aventurier", value: aventurier || "-", inline: false },
          { name: "💰 Portefeuille", value: `${soldeRaw} silvers`, inline: false },
          { name: "📈 Cumul", value: `${cumulRaw} silvers`, inline: false }
        )
        .setFooter({
          text: "Veuillez contacter un membre du staff pour récupérer vos gains",
        });

      await replyEphemeral(interaction, { embeds: [embed] });
      return;
    }

    if (interaction.commandName === "top") {
      const type = interaction.options.getString("type", true);
      let col;
      let title;
      let isMoney = false;
      if (type === "bal") {
        col = COL_SOLDE;
        title = "Top SOLDE";
        isMoney = true;
      } else if (type === "money") {
        col = COL_CUMUL;
        title = "Top CUMUL";
        isMoney = true;
      } else if (type === "participation") {
        col = COL_PARTICIPATIONS;
        title = "Top PARTICIPATIONS";
      } else if (type === "regear") {
        col = COL_REGEAR;
        title = "Top REGEAR";
      }
      const rows = await getTopPlayers(joueursSheet, col, 10);
      const items = formatTopLines(rows);
      const embed = new EmbedBuilder()
        .setTitle(title)
        .setDescription(items.length ? renderTopTable(items, { isMoney }) : "Aucune donnée")
        .setFooter({ text: "Source: Google Sheets" });
      await replyEphemeral(interaction, { embeds: [embed] });
      return;
    }

    if (interaction.commandName === "stats") {
      const sum = await getColumnSum(joueursSheet, COL_PARTICIPATIONS);
      await replyEphemeral(interaction, { content: `Participations totales (guilde): ${sum}` });
      return;
    }

    if (interaction.commandName === "activity") {
      const sub = interaction.options.getSubcommand(true);
      if (sub === "list") {
        const n = interaction.options.getInteger("n") ?? 10;
        const limit = Number.isFinite(n) && n > 0 ? Math.min(20, Math.trunc(n)) : 10;
        const items = await listActivities(activitesSheet, limit);
        if (!items.length) {
          await replyEphemeral(interaction, { content: "Aucune activité trouvée." });
          return;
        }
        const embed = new EmbedBuilder()
          .setTitle("Activités (récentes)")
          .setDescription(renderActivitiesTable(items));
        await replyEphemeral(interaction, { embeds: [embed] });
        return;
      }
      if (sub === "detail") {
        const id = interaction.options.getString("id", true);
        const obj = await getActivityById(activitesSheet, id);
        if (!obj) {
          await replyEphemeral(interaction, { content: `Activité introuvable: ${id}` });
          return;
        }
        const keys = Object.keys(obj);
        const shown = keys.slice(0, 12);
        const embed = new EmbedBuilder().setTitle(`Activité ${id}`);
        for (const k of shown) {
          const v = obj[k];
          const s = String(v ?? "").trim();
          if (!s) continue;
          embed.addFields({ name: k, value: s.slice(0, 1024) });
        }
        await replyEphemeral(interaction, { embeds: [embed] });
        return;
      }
    }

    if (interaction.commandName === "checkmembers") {
      const { guild, role } = await getGuildAndRole();
      if (!role) {
        await replyEphemeral(interaction, { content: `Rôle introuvable: ${ROLE_NAME}` });
        return;
      }

      if (BSG_CHECKMEMBERS_FETCH_ALL_MEMBERS) {
        try {
          const apiMembers = await listGuildMembersViaRest(guild.id);
          const roleId = role.id;
          const exact = apiMembers.filter((m) => Array.isArray(m?.roles) && m.roles.includes(roleId)).length;
          const sheetCount = await countRegisteredUsers(joueursSheet);
          await replyEphemeral(interaction, {
            content: `Discord (rôle '${role.name}'): ${exact} (exact) | Sheet (IDs enregistrés): ${sheetCount}`,
          });
          return;
        } catch (e) {
          console.warn(`checkmembers: fetch all members a échoué (${e?.message ?? e}).`);
        }
      }

      const discordCount = role.members.size;
      const sheetCount = await countRegisteredUsers(joueursSheet);
      await replyEphemeral(interaction, {
        content: `Discord (rôle '${role.name}'): ${discordCount} (cache) | Sheet (IDs enregistrés): ${sheetCount}`,
      });
      return;
    }

    if (interaction.commandName === "checknames") {
      if (!isStaffInteraction(interaction)) {
        await replyEphemeral(interaction, { content: "❌ Réservé aux admins/staff." });
        return;
      }
      const { guild } = await getGuildAndRole();
      // On ne fetch pas tous les membres: uniquement ceux présents dans la sheet.
      const regs = await listRegisteredDiscordUsers(joueursSheet);
      if (!regs.length) {
        await replyEphemeral(interaction, { content: "Aucun ID Discord trouvé dans la sheet." });
        return;
      }

      const changes = [];
      let missing = 0;

      for (const r of regs) {
        const discordId = String(r.discordId);
        let member;
        try {
          member = await guild.members.fetch(discordId);
        } catch {
          member = null;
        }
        if (!member) {
          missing++;
          continue;
        }

        const discordName = String(member.displayName ?? member.user?.username ?? "").trim();
        const sheetName = String(r.sheetName ?? "").trim();
        if (!sheetName) {
          changes.push({ discordId, from: sheetName, to: discordName });
          continue;
        }
        if (namesRoughlyMatch(sheetName, discordName)) continue;
        changes.push({ discordId, from: sheetName, to: discordName });
      }

      if (!changes.length) {
        const extra = missing ? ` (IDs introuvables sur Discord: ${missing})` : "";
        await replyEphemeral(interaction, { content: `✅ Aucun nom à corriger${extra}.` });
        return;
      }

      const token = makeToken();
      pendingNameFixes.set(token, {
        createdAt: Date.now(),
        userId: interaction.user.id,
        changes,
      });

      const shown = changes.slice(0, 12);
      const lines = shown
        .map((c) => `- <@${c.discordId}>: sheet="${String(c.from).slice(0, 40)}" → discord="${String(c.to).slice(0, 40)}"`)
        .join("\n");
      const more = changes.length > shown.length ? `\n… +${changes.length - shown.length} autres` : "";
      const info = `🔎 Propositions de correction: ${changes.length}${missing ? ` | IDs Discord introuvables: ${missing}` : ""}`;

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`checknames:apply:${token}`)
          .setLabel("Appliquer")
          .setStyle(ButtonStyle.Success),
        new ButtonBuilder()
          .setCustomId(`checknames:cancel:${token}`)
          .setLabel("Annuler")
          .setStyle(ButtonStyle.Secondary)
      );

      await replyEphemeral(interaction, {
        content: `${info}\n\n${lines}${more}\n\nValider ?` ,
        components: [row],
      });
      return;
    }
  } catch (e) {
    console.error("interaction error:", e);
    const msg = e?.message ?? String(e);
    try {
      if (interaction.deferred || interaction.replied) {
        await followUpEphemeral(interaction, { content: `Erreur: ${msg}` });
      } else {
        await replyEphemeral(interaction, { content: `Erreur: ${msg}` });
      }
    } catch (replyErr) {
      console.error("Failed to send error response:", replyErr);
    }
  }
});

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("OK");
});
server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.warn(`Port ${PORT} déjà utilisé, health check ignoré.`);
  } else {
    console.error("Health check server error:", err);
  }
});
server.listen(PORT, () => {
  console.log(`Health check server listening on port ${PORT}`);
});

await client.login(DISCORD_TOKEN);
