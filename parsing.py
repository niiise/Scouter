from demoparser2 import DemoParser
import json
import pandas as pd
import numpy as np
import os
import glob
import itertools
import shutil
import time


# Initialize variables
regseason_folder = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Regular Season"
playoffs_folder = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Playoffs"
example_folder = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/example"

# displays more of each dataframe
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)


# Defining useful functions
def get_round_stats(parser, rounds_played, stats):
    """Retrieves statistics for each player at a specified rounds_played
    IMPORTANT NOTE: rounds_played is how many rounds have been played at the current point, which is one less than the current round of play.
    this counter updates at the round_end event"""
    stats_needed = ["total_rounds_played"]
    stats_needed = stats_needed + stats
    # using round starts because round ends do not have updated damage info from the current round
    round_starts = parser.parse_event("round_start")
    round_starts = round_starts.sort_values("tick").drop_duplicates("round", keep="last")
    last_rounds_played = round_starts["round"].iloc[-1] - 1 # get last rounds_played of demo
    # get things for parser: whatever properties are desired/needed for intermediate processes, and which ticks to process
    wanted_ticks = round_starts["tick"].to_list()
    # makes sure that all cases are handled
    if rounds_played > last_rounds_played: # functionality for if invalid rounds are inputted
        print(f"Round #{rounds_played+1} is outside of range. This demo contains only {last_round + 1} rounds.")
        return None
    if rounds_played == last_rounds_played: # special case since round_start of the last round + 1 does not exist
        game_end = parser.parse_event("round_end")["tick"].max()
        wanted_ticks.append(game_end) # adds last ticks of game to list of desired ticks
    df_starts = parser.parse_ticks(stats_needed, ticks=wanted_ticks)
    df_starts_filtered = df_starts.drop(["name", "tick", "total_rounds_played"], axis=1)  # dropping some columns
    # groups dataframe by round
    grouped = df_starts_filtered.groupby(df_starts.total_rounds_played)
    # get the two dfs needed for subtraction (the round start of the round in question and the round start of the round after)
    round_stats = grouped.get_group(rounds_played+1).set_index("steamid")
    prev_stats = grouped.get_group(rounds_played).set_index("steamid")
    # arithmetic
    stats = round_stats.subtract(prev_stats, fill_value=0).reset_index()
    return stats

def get_stats(parser):
    """Retrieves aggregate end of game information for all 10 players that participated in a match, given the demo path"""
    # gets the tick for the very end of the game
    game_end = parser.parse_event("round_end")["tick"].max()
    # desired scoreboard fields
    stats_needed = [
        "kills_total", "deaths_total", "assists_total", "utility_damage_total",
        "damage_total", "headshot_kills_total", "ace_rounds_total", "4k_rounds_total",
        "3k_rounds_total", "enemies_flashed_total", "total_rounds_played"
    ]
    # queries parser for the desired aggregate statistics at the desired tick
    stats = parser.parse_ticks(stats_needed, ticks=[game_end])
    # manipulate dataframe to get what is desired
    del stats["tick"]
    stats["damage_per_round"] = stats["damage_total"] / stats["total_rounds_played"]
    stats["kills_per_round"] = stats["kills_total"] / stats["total_rounds_played"]
    stats["deaths_per_round"] = stats["deaths_total"] / stats["total_rounds_played"]
    stats["assists_per_round"] = stats["assists_total"] / stats["total_rounds_played"]
    stats.columns = stats.columns.str.replace("_total", "", regex=False)
    return stats


def get_pistol_stats(parser):
    """Retrieves aggregate pistol round statistics for each player"""
    # using round starts because round ends actually do not have updated damage info from the most recent round
    stats_needed = [
        "kills_total", "deaths_total",
        "damage_total", "headshot_kills_total", "ace_rounds_total",
        "4k_rounds_total", "3k_rounds_total", "total_rounds_played"
    ]
    # get both pistol rounds from replay using get_round_stats
    first_pistol = get_round_stats(parser, 0, stats_needed).set_index("steamid")
    second_pistol = get_round_stats(parser, 12, stats_needed).set_index("steamid")
    # add them together
    pistol_stats = first_pistol.add(second_pistol, fill_value=0).reset_index()
    # rename columns for differentiation (except shared columns)
    pistol_stats = pistol_stats.set_index(["steamid"]).add_prefix("pstl_").reset_index()
    pistol_stats.columns = pistol_stats.columns.str.replace("_total", "", regex=False)
    # sorting
    pistol_stats = pistol_stats.sort_values(by="steamid").reset_index(drop=True)
    return pistol_stats


def get_multi_stuff(parser):
    """Checks whether a player got a 2k in a round, and checks if a multikill (2 3 or 4) led to the round win for that player"""
    round_ends = parser.parse_event("round_end")
    end_ticks = round_ends["tick"].tolist()
    # the round counter for the round end event start at 2 (round 1 is listed as 2), so round is round-1 and rounds_played is round - 2
    last_rounds_played = round_ends["round"].iloc[-1] - 2 # get last rounds_played of demo
    # the round counter goes up at round_end, i.e. rounds_played at the end of round 1 is 1, whereas it is 0 for any other point in the round
    # team_names dataframe therefore shows round wanted, not the round, basically, index value is equal to rounds_played
    team_names = parser.parse_ticks(["damage_total", "team_name", "total_rounds_played"], ticks=end_ticks) # get each players team for each round
    # initializing dictionaries to store relevant info in
    player_2ks = dict.fromkeys(set(team_names["steamid"]), 0)
    multi_conversions = dict.fromkeys(set(team_names["steamid"]), 0)
    # needed for get_round_stats
    stats_needed = ["kills_total"]
    for round in range(0, last_rounds_played):
        # getting stats and teams for each player for each round
        temp_stats = get_round_stats(parser, round, stats_needed)
        temp_teams = team_names.groupby("total_rounds_played").get_group(round+1) # using round+1 due to difference described above
        # temporary tracking of 2ks and multi conversions
        temp_2ks = temp_stats[temp_stats["kills_total"] == 2]["steamid"] # get dataframe of players who got 2 kills in the round
        temp_multis = temp_stats[temp_stats["kills_total"] >= 2]["steamid"] # get dataframe of players who got any multikill in the round
        round_winner = round_ends.iloc[round]["winner"] # uses index (rounds_played) to find round winner from round_ends
        for steamid in temp_2ks:
            player_2ks[steamid] += 1 # track every 2k each player gets
        for steamid in temp_multis: # iterate through each player
            this_player_team = temp_teams.loc[temp_teams["steamid"] == steamid, "team_name"].iloc[0]
            if round_winner == this_player_team:
                multi_conversions[steamid] += 1 # track when players who got multi won the round
    return player_2ks, multi_conversions


def check_KAST(deaths, round_idx, player_ids):
    """Checks whether a player got a kill, assist, trade, or survived in a specified round.
    also checks whether an opening death was traded"""
    # gets first death of round
    first_death = deaths[deaths["total_rounds_played"] == round_idx].iloc[0]["user_steamid"]
    # setting up temp dictionaries to hold desired values
    temp_players_KAST = dict.fromkeys(player_ids, 0) # temp dictionary of KAST. a value of 1 means the player got a kill, assisted, survived, or was traded
    dead_players = dict.fromkeys(player_ids, 0) # value of 0 means alive, value of 1 means dead, tracks all deaths
    temp_traded_deaths = dict.fromkeys(player_ids, 0) # initializes dictionary of total traded deaths for each player
    temp_trade_kills = dict.fromkeys(player_ids, 0) # initializes dictionary of total trade kills for each player
    temp_opd_tr = dict.fromkeys(player_ids, 0) # store if a player's opening death was traded in the round
    who_killed_me = {} # dead person steamid:killer steamid, tracks player's killers
    when_did_i_die = {} # dead person steamid:tick of death, tracks players time of death
    # iterates through all deaths
    for _, death in deaths.iterrows():
        if death["total_rounds_played"] == round_idx: # only checks in the current round
            dead_players[death["user_steamid"]] = 1 # gets all players who died in a round
            who_killed_me[death["user_steamid"]] = death["attacker_steamid"] # gets all killers for those deaths
            when_did_i_die[death["user_steamid"]] = death["tick"] # gets all times for those deaths
            if death["assister_steamid"] in temp_players_KAST.keys(): # KAST +1 for an assist
                temp_players_KAST[death["assister_steamid"]] = 1
            elif death["attacker_steamid"] in temp_players_KAST.keys(): # KAST +1 for a kill
                temp_players_KAST[death["attacker_steamid"]] = 1
    for steam_id in who_killed_me: # iterates through every killer to get trade(s)
        attacker_steam_id = who_killed_me[steam_id]
        if dead_players[attacker_steam_id] == 1: # checks to see if attacker is also dead
            trader_steam_id = who_killed_me[attacker_steam_id]
            if when_did_i_die[attacker_steam_id] - when_did_i_die[steam_id] < 320: # checks timing to see if it matches (320 ticks or 5 seconds according to HLTV)]
                if steam_id == first_death:
                    temp_opd_tr[steam_id] += 1
                temp_players_KAST[steam_id] = 1
                temp_traded_deaths[steam_id] += 1
                temp_trade_kills[trader_steam_id] += 1
    # tracks player survival for KAST
    for steam_id in dead_players:
        if dead_players[steam_id] == 0:
            temp_players_KAST[steam_id] = 1
    return temp_players_KAST, temp_traded_deaths, temp_trade_kills, temp_opd_tr

def get_KAST(parser):
    """Retrieves the total KAST%, number of deaths traded (including any opening deaths), and number of trade kills of each player"""
    # get all deaths and relevant information at the times of those deaths in the game
    deaths = parser.parse_event(
        "player_death", player=["team_name"],
        other=["total_rounds_played", "game_time", "round_start_time", "is_warmup_period"]
    )
    # filter out various unwanted deaths
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]] # team kills
    deaths = deaths[deaths["is_warmup_period"] == False] # warmup
    deaths = deaths.dropna(subset=["attacker_name"])  # falling off map, dying to bomb, etc.
    # get length of game
    max_round = deaths["total_rounds_played"].max() + 1
    # setting up dictionaries to hold desired values
    player_ids = set(deaths["user_steamid"])
    players_KAST = dict.fromkeys(player_ids,0)  # dictionary to store final KAST values
    trade_kills = dict.fromkeys(player_ids,0) # how many trade kills a player got
    traded_deaths = dict.fromkeys(player_ids, 0) # how many of a player's deaths were traded
    opd_tr = dict.fromkeys(player_ids,0) # how many of a player's opening deaths were traded
    # looping thru rounds
    for round in range(0, max_round):
        trade_stuff = check_KAST(deaths, round, player_ids)
        round_KAST = trade_stuff[0]
        round_trade_kills = trade_stuff[1]
        round_traded_deaths = trade_stuff[2]
        round_opd_tr = trade_stuff[3]
        for steam_id in player_ids:
            trade_kills[steam_id] = trade_kills[steam_id] + round_trade_kills[steam_id]
            traded_deaths[steam_id] = traded_deaths[steam_id] + round_traded_deaths[steam_id]
            opd_tr[steam_id] = opd_tr[steam_id] + round_opd_tr[steam_id]
            if round_KAST[steam_id] == 1:
                players_KAST[steam_id] += 1
    for steam_id in players_KAST:
        players_KAST[steam_id] /= int(max_round)
    final_trade_kills = {int(key): value for key, value in trade_kills.items()}
    final_traded_deaths = {int(key): value for key, value in traded_deaths.items()}
    KAST = {int(key): value for key, value in players_KAST.items()} # turn dict keys from str to int so they can be used later
    opd_tr = {int(key): value for key, value in opd_tr.items()}
    return KAST, final_trade_kills, final_traded_deaths, opd_tr


def get_clutches(parser):
    """Retrieves the total number of clutch wins of each player given a demo's path"""
    # event info from death, TKs and warmup period filtered out as well
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    # filter out warmup and teamkills
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]
    deaths = deaths[deaths["is_warmup_period"] == False]
    # end of round info, needed to see who wins
    round_ends = parser.parse_event("round_end")
    # tick info at each death, needed to see who is alive at each death
    df = parser.parse_ticks(["is_alive", "team_name", "total_rounds_played"], ticks=deaths["tick"].to_list())
    max_round = deaths["total_rounds_played"].max() + 1 # length of game
    def check_1vX(deaths, round_idx, round_ends, df, X=2):
        """Checks whether there was a clutch situation in a round"""
        for _, death in deaths.iterrows(): # iterates thru deaths
            if death["total_rounds_played"] == round_idx: # if the current death is in the specified round
                subdf = df[df["tick"] == death["tick"]] # set scope to current round
                ct_alive = subdf[(subdf["team_name"] == "CT") & (subdf["is_alive"] == True)] # get all living CTs
                t_alive = subdf[(subdf["team_name"] == "TERRORIST") & (subdf["is_alive"] == True)] # get all living Ts
                if len(ct_alive) == 1 and len(t_alive) >= X and round_ends.iloc[round_idx]["winner"] == "CT":
                    return ct_alive["steamid"].iloc[0] # returns the steamid of the CT who won alone against at least 2 Ts
                if len(t_alive) == 1 and len(ct_alive) >= X and round_ends.iloc[round_idx]["winner"] == "T":
                    return t_alive["steamid"].iloc[0] # returns the steamid of the T who won alone against at least 2 CTs
    clutch_wins = dict.fromkeys(deaths["user_steamid"], 0) # dict of steamid:clutch wins
    for round_idx in range(0, max_round): # iterates thru rounds
        clutcher_steamid = check_1vX(deaths, round_idx, round_ends, df) # gets the steamid of the clutcher
        if clutcher_steamid != None:
            clutcher_steamid = str(clutcher_steamid)
            clutch_wins[clutcher_steamid] += 1 # tally up clutch wins when found
    clutches = {int(key): value for key, value in clutch_wins.items()} # dict keys from str to int for later use
    return clutches


def get_round_openings(parser, rounds_played):
    """Get the opening kills for a particular round"""
    # getting all player deaths to iterate through
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    # filtering undesired deaths
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]  # filters out team kills
    deaths = deaths[deaths["is_warmup_period"] == False]  # filters out warmup
    deaths = deaths.dropna(subset=["attacker_name"]) # take out deaths with no killer (falling off map, dying to bomb)
    # FIXING AN EDGE CASE:
    # players that die after the last round ends WILL count for the current round, so we need to discard those deaths by
    # discarding any deaths that happen before freeze time ends
    freeze_ends_parse = parser.parse_event("round_freeze_end", other=["total_rounds_played"])
    freeze_ends = dict(zip(freeze_ends_parse["total_rounds_played"], freeze_ends_parse["tick"]))
    deaths = deaths[deaths["tick"] >= deaths["total_rounds_played"].map(freeze_ends)]
    # group death info by round
    grouped = deaths.groupby("total_rounds_played")
    # initialize dictionaries to store player:firstkill/death values
    FK = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    FD = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    # gets the first death of the round in question
    this_opening = grouped.get_group(rounds_played).iloc[0]
    # gets user info of killer/victim from first kill of the round
    attacker_steamid = int(this_opening["attacker_steamid"])
    victim_steamid = int(this_opening["user_steamid"])
    FK[attacker_steamid] += 1
    FD[victim_steamid] += 1
    return FK, FD

def get_opening_stats(parser):
    """Retrieves the total number of opening duels won/lost for each player given a demo's path"""
    # getting player deaths to iterate through
    deaths = parser.parse_event("player_death",
                                player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    # filtering unnecessary
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]  # filters out team kills
    deaths = deaths[deaths["is_warmup_period"] == False]  # filters out warmup
    deaths = deaths.dropna(subset=["attacker_name"]) # take out deaths with no killer (falling off map, dying to bomb)
    # FIXING AN EDGE CASE:
    # players that die after the last round ends WILL count for the current round, so we need to discard those deaths by
    # discarding any deaths that happen before freeze time ends
    freeze_ends_parse = parser.parse_event("round_freeze_end", other=["total_rounds_played"])
    freeze_ends = dict(zip(freeze_ends_parse["total_rounds_played"], freeze_ends_parse["tick"]))
    deaths = deaths[deaths["tick"] >= deaths["total_rounds_played"].map(freeze_ends)]
    # group death info by round
    grouped = deaths.groupby("total_rounds_played")
    max_round = deaths["total_rounds_played"].max() + 1
    round_ends = parser.parse_event("round_end") # get who wins each round
    # initialize dictionaries to store player:firstkill/death values
    FK = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    opening_conversions = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    FD = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    opd_traded = dict.fromkeys(deaths["user_steamid"].astype(int), 0)
    # iterate through each round
    for round in range(0, max_round):
        this_opening = grouped.get_group(round).iloc[0] # gets death event info around the first kill of the round
        # gets user info of killer/victim from first kill of the round
        attacker_steamid = int(this_opening["attacker_steamid"])
        victim_steamid = int(this_opening["user_steamid"])
        # tracks number of first kills/deaths
        FK[attacker_steamid] += 1
        FD[victim_steamid] += 1
        # tracks whether an opening kill resulted in a round win
        if round_ends.iloc[round]["winner"] == this_opening["attacker_team_name"]:
             opening_conversions[attacker_steamid] += 1
    return FK, FD, opening_conversions


def get_antis(parser):
    """Retrieves anti-eco and eco/partial buy rounds for each player"""
    # FIRST DEATH EQUIP VALUE:
    # this is more accurate than checking the end of freezetime since some teams may continue to make economic decisions after freeze time ends
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    # filtering out unnecessary deaths
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]
    deaths = deaths[deaths["is_warmup_period"] == False]
    deaths = deaths.dropna(subset=["attacker_name"])
    anti_rp = dict.fromkeys(deaths["user_steamid"].astype(int), 0) # getting steamids for counting number of antis
    # FIXING AN EDGE CASE:
    # players that die after the last round ends WILL count for the current round, so we need to discard those deaths by
    # discarding any deaths that happen before freeze time ends
    freeze_ends_parse = parser.parse_event("round_freeze_end", other=["total_rounds_played"])
    freeze_ends = dict(zip(freeze_ends_parse["total_rounds_played"], freeze_ends_parse["tick"]))
    deaths = deaths[deaths["tick"] >= deaths["total_rounds_played"].map(freeze_ends)]
    deaths = deaths.sort_values("tick").drop_duplicates("total_rounds_played", keep="first") # gets tick of first death after filtering
    wanted = deaths["tick"].tolist()
    # parses equipment value at the end of each of the desired ticks (at the first death of the round of play)
    # equipment value is more accurate than purchases, since some players may survive between rounds and retain equipment and
    # saved equipment should count towards buys
    df = parser.parse_ticks(["current_equip_value", "total_rounds_played", "team_name"], ticks=wanted)
    max_round = df["total_rounds_played"].max() + 1
    df_filtered = df[~df["total_rounds_played"].isin([0, 12])] # filter out pistol rounds
    # 3300 is the breakpoint standard used by HLTV analysts for an eco buy
    breakpoint = 3300
    team_breakpoint = breakpoint*5 # evaluating on teamwide basis, so * 5 is necessary
    # split data based on "current_equip_value" of the entire team
    team_spending = df_filtered.groupby(["total_rounds_played", "team_name"])["current_equip_value"].sum().reset_index()
    eco_rounds = team_spending[team_spending["current_equip_value"] < team_breakpoint]
    anti_df = pd.DataFrame() # initialization of dataframe
    # iteration through each eco round
    for round in set(eco_rounds["total_rounds_played"]):
        subdf = df_filtered[df_filtered["total_rounds_played"] == round]
        eco_sub = eco_rounds[eco_rounds["total_rounds_played"] == round]
        eco_team = subdf.loc[subdf["team_name"] == eco_sub["team_name"].item()] # returns which team is saving
        anti_team = subdf.loc[subdf["team_name"] != eco_sub["team_name"].item()] # returns which team is playing against a save round
        anti_df = pd.concat([anti_df, anti_team]) # creating larger dataframe
    del anti_df["tick"]
    del anti_df["current_equip_value"]
    for steamid in anti_df["steamid"]: # counting anti ecos for each player
        anti_rp[steamid] += 1
    return anti_df, anti_rp

def get_anti_stats(parser):
    """Get desired statistics from anti eco rounds"""
    antis = get_antis(parser)[0]
    anti_rp = get_antis(parser)[1]
    stats = None # set to none for now
    # loop through all anti eco rounds
    for round in set(antis["total_rounds_played"]):
        # need opening kills and general statistics from other functions for anti eco rounds
        stats_needed = [
            "kills_total", "deaths_total", "assists_total",
            "damage_total", "total_rounds_played"
        ]
        temp_stats = get_round_stats(parser, round, stats_needed)
        temp_openings = get_round_openings(parser, round)
        temp_stats["first_kills"] = temp_stats["steamid"].map(temp_openings[0])
        temp_stats["first_deaths"] = temp_stats["steamid"].map(temp_openings[1])
        temp_stats.set_index(["steamid"], inplace=True) # exclude steamid from addition
        # add results
        if stats is None:
            stats = temp_stats
        else:
            stats = stats.add(temp_stats, fill_value=0)
    stats.columns = stats.columns.str.replace("_total", "", regex=False)
    stats = stats.add_prefix("anti_").reset_index()
    stats["anti_rp"] = stats["steamid"].map(anti_rp)
    return stats


def get_all_stats(replay_path):
    """Combine all statistics into one dataframe for one replay"""
    parser = DemoParser(replay_path)
    basic_stats = get_stats(parser)  # DATAFRAME
    pistol_stats = get_pistol_stats(parser)  # DATAFRAME
    anti_stats = get_anti_stats(parser)  # DATAFRAME and a DICT of number of antis taken for each player
    clutch_stats = get_clutches(parser)  # DICT
    opening_stats = get_opening_stats(parser)  # TWO DICTS, first kills and first deaths
    trade_stuff = get_KAST(parser) # THREE DICTS, KAST, trade kills, and traded deaths
    multi_stuff = get_multi_stuff(parser) # TWO DICTS, total 2ks, and conversions off of multifrags
    # merge dataframes
    all_stats = pd.merge(basic_stats, pistol_stats, on=["steamid"])
    all_stats = pd.merge(all_stats, anti_stats, on=["steamid"])
    # add dicts as columns
    all_stats["doubles"] = all_stats["steamid"].map(multi_stuff[0])
    all_stats["mk_win"] = all_stats["steamid"].map(multi_stuff[1])
    all_stats["clutch_ws"] = all_stats["steamid"].map(clutch_stats)
    all_stats["OpK"] = all_stats["steamid"].map(opening_stats[0])
    all_stats["OpD"] = all_stats["steamid"].map(opening_stats[1])
    all_stats["tr_OpD"] = all_stats["steamid"].map(trade_stuff[3])
    all_stats["OpK_w"] = all_stats["steamid"].map(opening_stats[2])
    all_stats["KAST"] = all_stats["steamid"].map(trade_stuff[0])
    all_stats["trK"] = all_stats["steamid"].map(trade_stuff[1])
    all_stats["trD"] = all_stats["steamid"].map(trade_stuff[2])
    # renaming some columns for brevity
    all_stats.rename(
        columns={"total_rounds_played": "RP", "3k_rounds": "triples", "4k_rounds": "quads", "ace_rounds": "aces",
                 "utility_damage": "UD", "damage_per_round": "adr", "assists_per_round": "apr", "kills_per_round": "kpr",
                 "deaths_per_round": "dpr", "pstl_headshot_kills": "pstl_hsk", "headshot_kills": "hsk"}, inplace=True)
    return all_stats


def get_everything(demo_folder):
    """Collects relevant info from each demo in the specified folder, combines it, then outputs into one csv. If the csv already exists, it will add onto its dataframe"""
    # create or load directory for failed replays
    failed_folder = os.path.join(demo_folder, "Failed Files")
    if not os.path.exists(failed_folder):
        os.makedirs(failed_folder)

    # create or load list of already parsed files
    log_file = os.path.join(demo_folder, "parsed_files.json")
    if os.path.exists(log_file):
        with open(log_file, "r") as log:
            parsed_files = set(json.load(log))
    else:
        parsed_files = set()

    # create or load current data
    output_csv = os.path.join(demo_folder, "player_stats.csv")
    if os.path.exists(output_csv):
        everything = pd.read_csv(output_csv)
    else:
        everything = None

    # get list of only the dem files in our directory
    dem_files = glob.glob(f"{demo_folder}/*.dem")
    # track progress
    print(f"{len(dem_files)- len(parsed_files)} files to check!")

    # checks every downloaded .dem file
    for count, filepath in enumerate(dem_files, start=1):
        filename = os.path.relpath(filepath, demo_folder)
        if filename in parsed_files:
            print(f"Skipping file #{count}, it has already been processed!")
            continue
        try:
            # gets dataframe from replay file
            print(f"Starting to process file #{count}...")
            temp = get_all_stats(filepath)
        except Exception as e:
            shutil.move(filepath, failed_folder)
            print(f"Something may have been wrong with file #{count}. File has been moved to the failed files folder.")
            continue
        # combines existing dataframe with new dataframe, needs further grouping/math
        combined_df = pd.concat([everything, temp], ignore_index=True)
        # sorting columns by what operation should be done
        # i.e. total_kills should just be summed with the new data avg_damage should have a new weighted mean calculated
        avg_cols = ["kpr", "apr", "dpr", "adr", "KAST"]
        agg_cols = combined_df.select_dtypes(include="number").columns.tolist()
        agg_cols = [col for col in agg_cols if col not in avg_cols]
        agg_cols.remove("steamid")
        # cleaning data in case of errors/missing values
        combined_df.dropna(inplace=True)
        # custom function created to create new accurate average with weights based on rounds played
        weighted_avg = lambda x: np.average(x, weights=combined_df.loc[x.index, "RP"])
        # compiling new statistics for each player (by steamid) and calls each new function on relevant columns
        everything = combined_df.groupby("steamid", as_index=False).agg(
            {
                "name": "last",
                **{col: "sum" for col in agg_cols},
                **{col: weighted_avg for col in avg_cols}
            }
        )
        # reordering columns
        everything.insert(0, "steamid", everything.pop("steamid"))
        everything.insert(1, "name", everything.pop("name"))
        everything.insert(2, "RP", everything.pop("RP"))
        everything.insert(everything.columns.get_loc("anti_kills") - 1, "anti_rp", everything.pop("anti_rp"))
        everything.insert(everything.columns.get_loc("triples") + 1, "doubles", everything.pop("doubles"))
        everything.insert(everything.columns.get_loc("doubles") + 1, "mk_win", everything.pop("mk_win"))
        everything.insert(everything.columns.get_loc("kills") + 1, "kpr", everything.pop("kpr"))
        everything.insert(everything.columns.get_loc("deaths") + 1, "dpr", everything.pop("dpr"))
        everything.insert(everything.columns.get_loc("assists") + 1, "apr", everything.pop("apr"))
        everything.insert(everything.columns.get_loc("damage") + 1, "adr", everything.pop("adr"))
        # adds filename to log to make sure replays aren't parsed twice
        print(f"Successfuly parsed file #{count}. Only {len(dem_files) - count} replay(s) left!")
        parsed_files.add(filename)
        # dumps compiled log file into outside json for persistence
        with open(log_file, 'w') as log:
            json.dump(list(parsed_files), log)
        # creates the csv
        everything.to_csv(output_csv, index=False)
    return everything

#                                            EXECUTION
start = time.time()
get_everything(regseason_folder)
get_everything(playoffs_folder)
end = time.time()
print(end-start)





