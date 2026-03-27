import "dotenv/config";
import {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ApplicationCommandOptionType,
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
  listActivities,
  updateUserName,
} from "./sheets_js.js";

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const GUILD_ID = process.env.GUILD_ID;

if (!DISCORD_TOKEN) throw new Error("DISCORD_TOKEN manquant (variable d'environnement)");
if (!GUILD_ID) throw new Error("GUILD_ID manquant (variable d'environnement)");

const ROLE_ID = process.env.BSG_MEMBER_ROLE_ID || null;
const ROLE_NAME = process.env.BSG_MEMBER_ROLE_NAME || "bsg membre";

const AUTO_DELETE_SECONDS_RAW = process.env.AUTO_DELETE_SECONDS;
const AUTO_DELETE_SECONDS = (() => {
  const n = AUTO_DELETE_SECONDS_RAW == null ? 300 : Number(AUTO_DELETE_SECONDS_RAW);
  if (!Number.isFinite(n)) return 300;
  return Math.min(3600, Math.max(30, Math.trunc(n)));
})();
const AUTO_DELETE_MS = AUTO_DELETE_SECONDS * 1000;

function formatRemaining(ms) {
  const total = Math.max(0, Math.ceil(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function withCountdownLine(baseContent, remainingStr) {
  const countdown = `⏳ Suppression dans ${remainingStr}`;
  const base = String(baseContent ?? "").trim();
  if (!base) return countdown;
  const combined = `${base}\n\n${countdown}`;
  return combined.length <= 2000 ? combined : (base.slice(0, 2000 - countdown.length - 2) + `\n\n${countdown}`);
}

function decoratePayloadWithCountdown(payload, remainingStr) {
  const countdown = `⏳ Suppression dans ${remainingStr}`;
  const hasEmbeds = Array.isArray(payload?.embeds) && payload.embeds.length > 0;
  const baseContent = payload?.content ?? "";

  // Prefer putting countdown in the embed footer when there is no content.
  if (hasEmbeds && !String(baseContent ?? "").trim()) {
    const embeds = payload.embeds.map((e) => {
      const eb = EmbedBuilder.from(e);
      const current = eb.data?.footer?.text ? String(eb.data.footer.text) : "";
      const next = current ? `${current} • ${countdown}` : countdown;
      eb.setFooter({ text: next.slice(0, 2048) });
      return eb;
    });
    return { ...payload, embeds };
  }

  // Otherwise, show countdown in message content.
  return { ...payload, content: withCountdownLine(baseContent, remainingStr) };
}

async function replyAutoDelete(interaction, payload) {
  // Ensure the interaction is acknowledged quickly, then post a PUBLIC message in the channel.
  // This avoids any accidental ephemeral behavior and guarantees the message is visible to everyone.
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ ephemeral: true });
  }

  const channel = interaction.channel;
  if (!channel || !channel.isTextBased()) {
    throw new Error("Impossible d'envoyer un message: channel indisponible");
  }

  const initialRemaining = formatRemaining(AUTO_DELETE_MS);
  const message = await channel.send(decoratePayloadWithCountdown(payload, initialRemaining));

  // Replace the ephemeral ACK with something minimal (optional) then delete it.
  try {
    await interaction.deleteReply();
  } catch {
    // ignore
  }

  const start = Date.now();
  const tick = async () => {
    const elapsed = Date.now() - start;
    const remaining = AUTO_DELETE_MS - elapsed;
    if (remaining <= 0) {
      clearInterval(interval);
      try {
        await message.delete();
      } catch {
        // ignore
      }
      return;
    }
    try {
      await message.edit(decoratePayloadWithCountdown(payload, formatRemaining(remaining)));
    } catch {
      // If we can't edit anymore (permissions/deleted), stop.
      clearInterval(interval);
    }
  };

  const interval = setInterval(tick, 10_000);
  return message;
}

async function followUpAutoDelete(interaction, payload) {
  // When we already replied/deferred, just post another PUBLIC message.
  const channel = interaction.channel;
  if (!channel || !channel.isTextBased()) {
    throw new Error("Impossible d'envoyer un message: channel indisponible");
  }

  const initialRemaining = formatRemaining(AUTO_DELETE_MS);
  const msg = await channel.send(decoratePayloadWithCountdown(payload, initialRemaining));

  const start = Date.now();
  const tick = async () => {
    const elapsed = Date.now() - start;
    const remaining = AUTO_DELETE_MS - elapsed;
    if (remaining <= 0) {
      clearInterval(interval);
      try {
        await msg.delete();
      } catch {
        // ignore
      }
      return;
    }
    try {
      await msg.edit(decoratePayloadWithCountdown(payload, formatRemaining(remaining)));
    } catch {
      clearInterval(interval);
    }
  };
  const interval = setInterval(tick, 10_000);
  return msg;
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

let joueursSheet;
let activitesSheet;

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
  if (joueursSheet && activitesSheet) return;
  joueursSheet = await getSheetByName("JOUEURS");
  activitesSheet = await getSheetByName("ACTIVITES");
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
      description: "Enregistre ton compte (ID Discord) dans la sheet",
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
  ]);
}

async function startupSyncRoleMembers() {
  const { guild, role } = await getGuildAndRole();
  if (!role) {
    console.warn(`Rôle introuvable pour sync: ${ROLE_NAME}`);
    return;
  }

  const members = await guild.members.fetch();
  const roleMembers = role.members;
  console.log(`Sync: ${roleMembers.size} membres avec rôle '${role.name}'`);

  let added = 0;
  let updated = 0;
  for (const member of roleMembers.values()) {
    const display = member.displayName;
    // addUser is idempotent; update name too
    if (await addUser(joueursSheet, member.id, display)) added++;
    if (await updateUserName(joueursSheet, member.id, display)) updated++;
  }

  // Ensure cache isn't the only source; keep members fetched to avoid partial role.members
  void members;
  console.log(`Sync terminé: ajoutés=${added}, noms_maj=${updated}`);
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

    // Role gained
    const oldHas = memberHasRole(oldMember);
    const newHas = memberHasRole(newMember);
    if (!oldHas && newHas) {
      const display = newMember.displayName;
      const added = await addUser(joueursSheet, newMember.id, display);
      if (added) console.log(`Ajout sheet: ${display} (${newMember.id})`);
    }

    // Nickname/display name changed
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
  if (!interaction.isChatInputCommand()) return;
  if (interaction.guildId !== GUILD_ID) return;

  try {
    await ensureSheetsLoaded();

    if (interaction.commandName === "register") {
      const { guild } = await getGuildAndRole();
      const member = await guild.members.fetch(interaction.user.id);
      const display = member.displayName;
      const added = await addUser(joueursSheet, interaction.user.id, display);
      await updateUserName(joueursSheet, interaction.user.id, display);
      await replyAutoDelete(interaction, {
        content: added
          ? `✅ Enregistré dans la sheet: ${display}`
          : `✅ Déjà enregistré. Nom mis à jour: ${display}`,
      });
      return;
    }

    if (interaction.commandName === "money") {
      const user = interaction.options.getUser("membre") ?? interaction.user;
      let bal = await getBalance(joueursSheet, user.id);
      if (!bal) {
        if (!bal) {
          const hint =
            user.id === interaction.user.id
              ? "\n➡️  Fais `/register` pour t'ajouter automatiquement."
              : "";
          await replyAutoDelete(interaction, {
            content: `❌ Pas trouvé dans la sheet: ${user.username}${hint}`,
          });
          return;
        }
      }

      const guildName = interaction.guild?.name ?? "Guilde";
      const aventurier = (bal.name || user.username || "-").trim();
      const soldeRaw = (bal.balance || "0 €").trim();
      const cumulRaw = (bal.cumulative || "0 €").trim();
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

      await replyAutoDelete(interaction, { embeds: [embed] });
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
      await replyAutoDelete(interaction, { embeds: [embed] });
      return;
    }

    if (interaction.commandName === "stats") {
      const sum = await getColumnSum(joueursSheet, COL_PARTICIPATIONS);
      await replyAutoDelete(interaction, { content: `Participations totales (guilde): ${sum}` });
      return;
    }

    if (interaction.commandName === "activity") {
      const sub = interaction.options.getSubcommand(true);
      if (sub === "list") {
        const n = interaction.options.getInteger("n") ?? 10;
        const limit = Number.isFinite(n) && n > 0 ? Math.min(20, Math.trunc(n)) : 10;
        const items = await listActivities(activitesSheet, limit);
        if (!items.length) {
          await replyAutoDelete(interaction, { content: "Aucune activité trouvée." });
          return;
        }
        const embed = new EmbedBuilder()
          .setTitle("Activités (récentes)")
          .setDescription(renderActivitiesTable(items));
        await replyAutoDelete(interaction, { embeds: [embed] });
        return;
      }
      if (sub === "detail") {
        const id = interaction.options.getString("id", true);
        const obj = await getActivityById(activitesSheet, id);
        if (!obj) {
          await replyAutoDelete(interaction, { content: `Activité introuvable: ${id}` });
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
        await replyAutoDelete(interaction, { embeds: [embed] });
        return;
      }
    }

    if (interaction.commandName === "checkmembers") {
      const { guild, role } = await getGuildAndRole();
      if (!role) {
        await replyAutoDelete(interaction, { content: `Rôle introuvable: ${ROLE_NAME}` });
        return;
      }
      await guild.members.fetch();
      const discordCount = role.members.size;
      const sheetCount = await countRegisteredUsers(joueursSheet);
      await replyAutoDelete(interaction, {
        content: `Discord (rôle '${role.name}'): ${discordCount} | Sheet (IDs enregistrés): ${sheetCount}`,
      });
      return;
    }
  } catch (e) {
    console.error("interaction error:", e);
    const msg = e?.message ?? String(e);
    if (interaction.deferred || interaction.replied) {
      await followUpAutoDelete(interaction, { content: `Erreur: ${msg}` });
    } else {
      await replyAutoDelete(interaction, { content: `Erreur: ${msg}` });
    }
  }
});

await client.login(DISCORD_TOKEN);
