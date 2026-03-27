import os

import discord
from discord.ext import commands
from dotenv import load_dotenv

# Load environment variables from .env file FIRST
_here = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_here, ".env"))

# Now import other modules that depend on environment variables
import sheets

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")

# Bot setup
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)

# Google Sheet instances
joueurs_sheet = sheets.get_sheet("JOUEURS")
activites_sheet = sheets.get_sheet("ACTIVITES")
BSG_MEMBER_ROLE_NAME = "bsg membre" # Or whatever the exact role name is

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    print(f'Guild ID: {GUILD_ID}')
    guild = bot.get_guild(int(GUILD_ID))
    if guild:
        print(f'Bot is in guild: {guild.name}')
    else:
        print(f'Bot is not in guild with ID: {GUILD_ID}')

@bot.event
async def on_member_update(before, after):
    # Check for role addition
    new_role = next((role for role in after.roles if role not in before.roles), None)
    if new_role and new_role.name.lower() == BSG_MEMBER_ROLE_NAME.lower():
        print(f"User {after.name} received the '{BSG_MEMBER_ROLE_NAME}' role.")
        if joueurs_sheet:
            sheets.add_user(joueurs_sheet, after.id, after.name)

    # Check for nickname change
    if before.display_name != after.display_name:
        print(f"User {before.name}'s nickname changed to {after.display_name}")
        if joueurs_sheet:
            sheets.update_user_name(joueurs_sheet, after.id, after.display_name)

# New commands for balance and top lists
@bot.command(name='bal')
async def balance(ctx, member: discord.Member = None):
    """Affiche le solde et le cumul d'un membre."""
    if member is None:
        member = ctx.author

    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return

    user_data = sheets.get_balance(joueurs_sheet, member.id)

    if user_data:
        embed = discord.Embed(
            title=f"Statistiques de {user_data['name']}",
            color=discord.Color.green()
        )
        embed.add_field(name="Solde Actuel", value=user_data['balance'], inline=True)
        embed.add_field(name="Argent Total Gagné", value=user_data['cumulative'], inline=True)
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"Désolé, je n'ai pas trouvé de données pour {member.display_name}.")

@bot.group(name='top', invoke_without_command=True)
async def top(ctx):
    """Affiche les classements de la guilde."""
    # This message will be sent if the user just types /top
    embed = discord.Embed(
        title="Commandes de classement",
        description="Utilisez les sous-commandes pour voir les classements.",
        color=discord.Color.blue()
    )
    embed.add_field(name="/top bal", value="Classement des joueurs les plus riches (solde actuel).", inline=False)
    embed.add_field(name="/top money", value="Classement des joueurs ayant gagné le plus d'argent au total.", inline=False)
    embed.add_field(name="/top participation", value="Classement des joueurs les plus participatifs.", inline=False)
    embed.add_field(name="/top regear", value="Classement des joueurs avec le plus de regears (Wall of Shame).", inline=False)
    await ctx.send(embed=embed)

@top.command(name='bal')
async def top_balance(ctx):
    """Classement des joueurs par solde actuel."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return
    
    top_players = sheets.get_top_players(joueurs_sheet, sheets.COL_SOLDE)
    
    embed = discord.Embed(
        title="🏆 Top 10 des plus riches (Solde)",
        color=discord.Color.gold()
    )

    if not top_players:
        embed.description = "Aucune donnée à afficher."
    else:
        description = ""
        for i, (name, balance) in enumerate(top_players):
            description += f"**{i+1}. {name}** - {balance:,.2f} €\n"
        embed.description = description

    await ctx.send(embed=embed)

@top.command(name='participation')
async def top_participation(ctx):
    """Classement des joueurs par participation."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return
    
    top_players = sheets.get_top_players(joueurs_sheet, sheets.COL_PARTICIPATIONS)
    
    embed = discord.Embed(
        title="📈 Top 10 des participations",
        color=discord.Color.orange()
    )

    if not top_players:
        embed.description = "Aucune donnée à afficher."
    else:
        description = ""
        for i, (name, score) in enumerate(top_players):
            description += f"**{i+1}. {name}** - {int(score)} participations\n"
        embed.description = description

    await ctx.send(embed=embed)

@top.command(name='regear')
async def top_regear(ctx):
    """Classement des joueurs par nombre de regears."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return
    
    top_players = sheets.get_top_players(joueurs_sheet, sheets.COL_REGEAR)
    
    embed = discord.Embed(
        title="💀 Wall of Shame (Top 10 Regears)",
        color=discord.Color.red()
    )

    if not top_players:
        embed.description = "Aucune donnée à afficher."
    else:
        description = ""
        for i, (name, score) in enumerate(top_players):
            description += f"**{i+1}. {name}** - {int(score)} regears\n"
        embed.description = description

    await ctx.send(embed=embed)

@top.command(name='money')
async def top_money(ctx):
    """Classement des joueurs par argent total gagné."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return
    
    top_players = sheets.get_top_players(joueurs_sheet, sheets.COL_CUMUL)
    
    embed = discord.Embed(
        title="💰 Top 10 des plus gros gains (Cumul)",
        color=discord.Color.purple()
    )

    if not top_players:
        embed.description = "Aucune donnée à afficher."
    else:
        description = ""
        for i, (name, cumul) in enumerate(top_players):
            description += f"**{i+1}. {name}** - {cumul:,.2f} €\n"
        embed.description = description

    await ctx.send(embed=embed)

@bot.command(name='stats')
async def stats(ctx):
    """Affiche les statistiques globales de la guilde."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return

    total_participations = sheets.get_column_sum(joueurs_sheet, sheets.COL_PARTICIPATIONS)
    
    embed = discord.Embed(
        title="Statistiques Globales de la Guilde",
        color=discord.Color.dark_blue()
    )
    embed.add_field(name="Nombre total de sorties enregistrées", value=f"**{int(total_participations)}** sorties", inline=False)

    await ctx.send(embed=embed)

@bot.command(name='checkmembers')
async def check_members(ctx):
    """Compare le nombre de membres sur Discord et dans la feuille de calcul."""
    if not joueurs_sheet:
        await ctx.send("La connexion à la feuille des joueurs a échoué.")
        return

    # Count members with the role in Discord
    guild = ctx.guild
    role = discord.utils.get(guild.roles, name=BSG_MEMBER_ROLE_NAME)
    if not role:
        await ctx.send(f"Le rôle '{BSG_MEMBER_ROLE_NAME}' n'a pas été trouvé sur ce serveur.")
        return
    
    discord_member_count = len(role.members)

    # Count members in the sheet
    sheet_member_count = sheets.count_registered_users(joueurs_sheet)

    embed = discord.Embed(
        title="Vérification des Membres",
        color=discord.Color.dark_purple()
    )
    embed.add_field(name="Membres avec le rôle Discord", value=f"**{discord_member_count}**", inline=False)
    embed.add_field(name="Membres inscrits dans le Google Sheet", value=f"**{sheet_member_count}**", inline=False)

    diff = discord_member_count - sheet_member_count
    if diff == 0:
        embed.description = "Le nombre de membres correspond parfaitement ! ✅"
    elif diff > 0:
        embed.description = f"Il y a **{diff}** membre(s) de plus sur Discord qui ne sont peut-être pas dans la feuille."
    else:
        embed.description = f"Il y a **{-diff}** membre(s) de plus dans la feuille qui n'ont peut-être plus le rôle sur Discord."

    await ctx.send(embed=embed)

@bot.group(name='activity', invoke_without_command=True)
async def activity(ctx):
    """Affiche les commandes liées aux activités."""
    embed = discord.Embed(
        title="Commandes d'activité",
        description="Utilisez les sous-commandes pour voir les détails des activités.",
        color=discord.Color.dark_orange()
    )
    embed.add_field(name="/activity list", value="Affiche les 10 dernières activités.", inline=False)
    embed.add_field(name="/activity detail <ID>", value="Affiche les détails d'une activité spécifique.", inline=False)
    await ctx.send(embed=embed)

@activity.command(name='list')
async def activity_list(ctx):
    """Affiche les 10 dernières activités."""
    if not activites_sheet:
        await ctx.send("La connexion à la feuille des activités a échoué.")
        return

    activities = sheets.get_activities(activites_sheet, 10)

    embed = discord.Embed(
        title="📜 Dernières activités",
        color=discord.Color.dark_orange()
    )

    if not activities:
        embed.description = "Aucune activité trouvée."
    else:
        description = ""
        for act in activities:
            description += f"**ID {act['ID']}**: `{act['TAGS']}` - {act['DATE']} par *{act['INITIATEUR']}*\n"
        embed.description = description
    
    await ctx.send(embed=embed)

@activity.command(name='detail')
async def activity_detail(ctx, activity_id: int):
    """Affiche les détails d'une activité spécifique."""
    if not activites_sheet:
        await ctx.send("La connexion à la feuille des activités a échoué.")
        return

    details = sheets.get_activity_details(activites_sheet, activity_id)

    if not details:
        await ctx.send(f"Aucune activité trouvée avec l'ID `{activity_id}`.")
        return

    embed = discord.Embed(
        title=f"Détails de l'activité #{details['ID']}: {details['TAGS']}",
        description=f"Dirigée par **{details['INITIATEUR']}** le **{details['DATE']}**.",
        color=discord.Color.blue()
    )
    
    # General Info
    embed.add_field(name="Participants", value=details.get('PARTICIPANTS', 'N/A'), inline=True)
    embed.add_field(name="Joueurs", value=details.get('NB JOUEURS', 'N/A'), inline=True)
    embed.add_field(name="Joueurs Ext.", value=details.get('NB JOUEURS EXT', 'N/A'), inline=True)
    
    # Gains
    embed.add_field(name="Loot", value=f"{details.get('LOOT', 0):,} €", inline=True)
    embed.add_field(name="Silver", value=f"{details.get('SILVER', 0):,} €", inline=True)
    
    # Frais
    embed.add_field(name="Réparations", value=f"{details.get('REPARATIONS', 0):,} €", inline=True)
    embed.add_field(name="Regear", value=f"{details.get('REGEAR (S)', 0):,} €", inline=True)
    embed.add_field(name="Budget", value=f"{details.get('BUDGET', 0):,} €", inline=True)
    
    # Bilan
    embed.add_field(name="💰 Bilan (Payout)", value=f"**{details.get('PAYOUT', 0):,} €**", inline=False)

    await ctx.send(embed=embed)

# Placeholder for your commands and events
@bot.command()
async def hello(ctx):
    await ctx.send("Hello!")

if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN)
    else:
        print("DISCORD_TOKEN not found in .env file")
