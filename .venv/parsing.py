from demoparser2 import DemoParser
import json
import pandas as pd
import os
import glob

# Initialize variables
replays_folder = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays/example/"
this_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays/example/g2-vs-spirit-m1-mirage.dem"
second_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays/example/g2-vs-spirit-m2-anubis.dem"
third_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays/example/g2-vs-spirit-m3-dust2.dem"
fourth_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays/example/g2-vs-vitality-m1-dust2.dem"
stats_csv = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/example/player_stats.csv"
check_file = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/example/check_file.json"

# Initialize file to store what demos have been parsed
if not os.path.exists(check_file):
    os.makedirs(os.path.dirname(check_file))
    with open(check_file, "w") as f:
        json.dump({"parsed_matches": []}, f)

with open(check_file, "r") as f:
    database = json.load(f)

# displays more of each dataframe
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)

# Defining useful functions
def get_names(replay_path):
    """Get a list of player steamids and their corresponding names"""
    parser = DemoParser(replay_path)
    game_end = parser.parse_event("round_end")["tick"].max()
    stats = parser.parse_ticks(["total_rounds_played"], ticks=[game_end])
    del stats["total_rounds_played"]
    del stats["tick"]
    stats["steamid"] = stats["steamid"].astype(str)
    player_names = stats.set_index("steamid")["name"].to_dict()
    names = {int(key): value for key, value in player_names.items()}# turn dict keys from str to int so they can be used later
    # print(names)
    return(names)

def get_round_stats(replay_path, rounds_played):
    """Retrieves statistics for each player at a specified rounds_played"""
    parser = DemoParser(replay_path)
    # using round starts because round ends actually do not have updated damage info from the most recent round
    round_starts = parser.parse_event("round_start")
    round_starts = round_starts.sort_values("tick").drop_duplicates("round", keep="last")
    round_starts = round_starts["tick"].to_list()
    stats_needed = [
        "kills_total", "deaths_total", "assists_total", "utility_damage_total",
        "damage_total", "headshot_kills_total", "ace_rounds_total",
        "4k_rounds_total", "3k_rounds_total", "total_rounds_played"
    ]
    df_starts = parser.parse_ticks(stats_needed, ticks=round_starts)
    df_starts_filtered = df_starts.drop(["name", "tick", "total_rounds_played"], axis=1)  # dropping some columns
    # groups dataframe by round
    grouped = df_starts_filtered.groupby(df_starts.total_rounds_played)
    # get pistol groups
    round_stats = grouped.get_group(rounds_played+1).reset_index(drop=True)
    prev_stats = grouped.get_group(rounds_played).reset_index(drop=True)
    # arithmetic
    stats = round_stats.subtract(prev_stats, fill_value=0)
    # re-add identifier columns
    stats["steamid"] = df_starts["steamid"]
    stats["name"] = df_starts["name"]
    # make sure every function is sorted the same
    stats = stats.sort_values(by="steamid").reset_index(drop=True)
    # print(stats)
    return stats

def get_stats(replay_path):
    """Retrieves aggregate end of game information for all 10 players that participated in a match, given the demo path"""
    parser = DemoParser(replay_path)
    # gets the tick for the very end of the game
    game_end = parser.parse_event("round_end")["tick"].max()
    # desired scoreboard fields
    stats_needed = [
        "kills_total", "deaths_total", "assists_total", "utility_damage_total",
        "damage_total", "headshot_kills_total", "ace_rounds_total", "4k_rounds_total",
        "3k_rounds_total", "total_rounds_played"
    ]
    # queries parser for the desired aggregate statistics at the desired tick
    stats = parser.parse_ticks(stats_needed, ticks=[game_end])
    # manipulate dataframe to get what is desired
    del stats["tick"]
    stats["damage_total"] = stats["damage_total"]/stats["total_rounds_played"]
    stats.rename(columns={"damage_total": "avg_damage_per_round"}, inplace=True)
    stats["avg_frags_per_round"] = stats["kills_total"]/stats["total_rounds_played"]
    # make sure every function is sorted the same
    stats = stats.sort_values(by="steamid").reset_index(drop=True)
    # print(stats)
    return stats

def get_pistol_stats(replay_path):
    """Retrieves aggregate pistol round statistics for each player"""
    parser = DemoParser(replay_path)
    # using round starts because round ends actually do not have updated damage info from the most recent round
    round_starts = parser.parse_event("round_start")
    round_starts = round_starts.sort_values("tick").drop_duplicates("round",keep="last")
    round_starts = round_starts["tick"].to_list()
    stats_needed = [
        "kills_total", "deaths_total", "assists_total", "utility_damage_total",
        "damage_total", "headshot_kills_total", "ace_rounds_total",
        "4k_rounds_total", "3k_rounds_total", "total_rounds_played"
    ]
    df_starts = parser.parse_ticks(stats_needed, ticks=round_starts)
    df_starts_filtered = df_starts.drop(["name", "tick"], axis=1) # dropping some columns
    # groups dataframe by round
    grouped = df_starts_filtered.groupby(df_starts.total_rounds_played)
    # get pistol groups
    first_pistol = grouped.get_group(1)
    second_pistol_sub = grouped.get_group(13)
    pre_second_pistol = grouped.get_group(12) # need the round before to make a subtraction to de-aggregate 2nd pistol stats
    # drop index for arithmetic
    first_pistol.reset_index(drop=True, inplace=True)
    second_pistol_sub.reset_index(drop=True, inplace=True)
    pre_second_pistol.reset_index(drop=True, inplace=True)
    # arithmetic
    second_pistol = second_pistol_sub.subtract(pre_second_pistol, fill_value=0)
    pistol_stats = first_pistol.add(second_pistol, fill_value=0)
    # re-add identifier columns
    pistol_stats["steamid"] = df_starts["steamid"]
    pistol_stats["name"] = df_starts["name"]
    del pistol_stats["total_rounds_played"]
    pistol_stats = pistol_stats.sort_values(by="steamid").reset_index(drop=True) # make sure every function is sorted the same
    pistol_stats = pistol_stats.set_index(["steamid", "name"]).add_prefix("pistol_").reset_index() # rename columns for differentiation (except shared columns)
    pistol_stats.columns = pistol_stats.columns.str.replace("_total", "", regex=False)
    # print(pistol_stats)
    return pistol_stats

def get_KAST(replay_path):
    """Retrieves the total KAST% of each player"""
    parser = DemoParser(replay_path)
    deaths = parser.parse_event(
        "player_death", player=["last_place_name", "team_name"],
        other=["total_rounds_played", "game_time", "round_start_time", "is_warmup_period"]
    )
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]] # filters out team kills
    deaths = deaths[deaths["is_warmup_period"] == False] # filters out warmup
    deaths = deaths.dropna(subset=["attacker_name"])  # take out people dying to bomb/fall damage
    # get length of game
    max_round = deaths["total_rounds_played"].max() + 1
    players_KAST = dict.fromkeys(deaths["user_steamid"],0)  # KAST dictionary initialization
    def check_KAST(deaths, round_idx, players_KAST):
        """Checks whether a player got a kill, assist, trade, or simply survived in a specified round"""
        temp_players_KAST = dict.fromkeys(players_KAST, 0) # value of 1 means the player got a kill, assisted, survived, or was traded
        dead_players = dict.fromkeys(players_KAST, 0) # value of 0 means alive, value of 0 means dead
        who_killed_me = {} # dead person steamid:killer steamid
        when_did_i_die = {} # dead person steamid:tick of death
        for _, death in deaths.iterrows():
            if death["total_rounds_played"] == round_idx:
                dead_players[death["user_steamid"]] = 1 # tracks dead players
                who_killed_me[death["user_steamid"]] = death["attacker_steamid"] # tracks who killed who
                when_did_i_die[death["user_steamid"]] = death["tick"] # tracks when players died
                if death["assister_steamid"] in players_KAST.keys(): # assist
                    temp_players_KAST[death["assister_steamid"]] = 1
                elif death["attacker_steamid"] in players_KAST.keys(): # kill
                    temp_players_KAST[death["attacker_steamid"]] = 1
        # every player in the game should have a corresponding key in "dead_players"
        # every player who got a kill or assist in round "round_idx" should have a 1 in "temp_players_KAST"
        # every player who died in round "round_idx" should be 1 in "dead_players"
        # every dead player's killer and the tick at which they died should be stored
        for steam_id in who_killed_me:
            attacker_steam_id = who_killed_me[steam_id]
            if dead_players[attacker_steam_id] == 1: # if the attacker also died
                if when_did_i_die[attacker_steam_id] - when_did_i_die[steam_id] < 320: # as defined by HLTV a trade is within 320 ticks
                    temp_players_KAST[steam_id] = 1
        # every player whose killer died within 320 ticks of their death should have a 1 in "temp_players_KAST"
        for steam_id in dead_players:
            if dead_players[steam_id] == 0:
                temp_players_KAST[steam_id] = 1
        # every player who didn't die should have a 1 in "temp_players_KAST"
        return temp_players_KAST
    for round in range(0, max_round):
        round_KAST = check_KAST(deaths, round, players_KAST) # this gives temp_players_KAST at each round
        for steam_id in players_KAST:
            if round_KAST[steam_id] == 1:
                players_KAST[steam_id] += 1
    for steam_id in players_KAST:
        players_KAST[steam_id] /= int(max_round)
    # names = get_names(replay_path) # using to debug
    # players_KAST = {key: [players_KAST[key], names[key]] for key in players_KAST}
    KAST = {int(key): value for key, value in players_KAST.items()} # turn dict keys from str to int so they can be used later
    # print(KAST) # outputs as a dictionary currently
    return KAST

def get_clutches(replay_path):
    """Retrieves the total number of clutch wins of each player given a demo's path"""
    parser = DemoParser(replay_path)
    # event info from death, TKs and warmup period filtered out as well
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
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
                subdf = df[df["tick"] == death["tick"]] # set dataframe equal to deaths in the round
                ct_alive = subdf[(subdf["team_name"] == "CT") & (subdf["is_alive"] == True)] # get all living CTs
                t_alive = subdf[(subdf["team_name"] == "TERRORIST") & (subdf["is_alive"] == True)] # get all living Ts
                if len(ct_alive) == 1 and len(t_alive) >= X and round_ends.iloc[round_idx]["winner"] == "CT":
                    return ct_alive["steamid"].iloc[0] # returns the steamid of the CT who won alone against at least 2 Ts
                if len(t_alive) == 1 and len(ct_alive) >= X and round_ends.iloc[round_idx]["winner"] == "T":
                    return t_alive["steamid"].iloc[0] # returns the steamid of the T who won alone against at least 2 CTs
        return None
    clutch_wins = dict.fromkeys(deaths["user_steamid"], 0) # dict of steamid:clutch wins
    # clutch_attempts = dict.fromkeys(deaths["user_steamid"], 0) # dict of steamid: clutch attempts total
    for round_idx in range(0, max_round): # iterates thru rounds
        clutcher_steamid = check_1vX(deaths, round_idx, round_ends, df) # gets the steamid of the clutcher
        if clutcher_steamid != None:
            clutcher_steamid = str(clutcher_steamid)
            clutch_wins[clutcher_steamid] += 1 # tally up clutch wins when found
    clutches = {int(key): value for key, value in clutch_wins.items()} # dict keys from str to int for later use
    # print(clutches) # outputs a dictionary for now
    return clutches

def get_round_openings(replay_path, rounds_played):
    """Get the opening kills for a particular round"""
    parser = DemoParser(replay_path)
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]  # filters out team kills
    deaths = deaths[deaths["is_warmup_period"] == False]  # filters out warmup
    # group death info by round
    deaths = deaths.dropna(subset=["attacker_name"])  # take out people dying to bomb after time
    grouped = deaths.groupby("total_rounds_played")
    max_round = deaths["total_rounds_played"].max() + 1
    # initialize dictionaries to store player:firstkill/death values
    FK = dict.fromkeys(deaths["user_steamid"], 0)
    FD = dict.fromkeys(deaths["user_steamid"], 0)
    # iterate through each round
    this_opening = grouped.get_group(rounds_played).iloc[0]  # gets death event info around the first kill of the round
    # gets user info of killer/victim from first kill of the round
    attacker_steamid = this_opening["attacker_steamid"]
    # print(attacker_steamid)
    victim_steamid = this_opening["user_steamid"]
    FK[attacker_steamid] += 1
    FD[victim_steamid] += 1
    openings = {key: [FK[key], FD[key]] for key in FK}
    opening_stats = {int(key): value for key, value in openings.items()} # turn dict values from str to int so they can be used later
    # print(opening_stats)
    return opening_stats

def get_opening_stats(replay_path):
    """Retrieves the total number of opening duels won/lost for each player given a demo's path"""
    parser = DemoParser(replay_path)
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]  # filters out team kills
    deaths = deaths[deaths["is_warmup_period"] == False]  # filters out warmup
    # group death info by round
    deaths = deaths.dropna(subset=["attacker_name"]) # take out people dying to bomb after time
    grouped = deaths.groupby("total_rounds_played")
    max_round = deaths["total_rounds_played"].max() + 1
    # initialize dictionaries to store player:firstkill/death values
    FK = dict.fromkeys(deaths["user_steamid"], 0)
    FD = dict.fromkeys(deaths["user_steamid"], 0)
    # iterate through each round
    for round in range(0, max_round):
        this_opening = grouped.get_group(round).iloc[0] # gets death event info around the first kill of the round
        # gets user info of killer/victim from first kill of the round
        attacker_steamid = this_opening["attacker_steamid"]
        # print(attacker_steamid)
        victim_steamid = this_opening["user_steamid"]
        FK[attacker_steamid] += 1
        FD[victim_steamid] += 1
    openings = {key:[FK[key], FD[key]] for key in FK}
    opening_stats = {int(key): value for key, value in openings.items()} # turn dict values from str to int so they can be used later
    # print(opening_stats)
    return opening_stats

def get_antis(replay_path):
    """Retrieves anti-eco and eco/partial buy rounds for each player"""
    parser = DemoParser(replay_path)
    # END OF FREEZE TIME EQUIP VALUE
    # this is less accurate than the next alternative because it is common to see pro teams linger in spawn after freeze time on an iffy buy
    # and may decide to force/save after freeze time ends
    # wanted = parser.parse_event("round_freeze_end")["tick"].tolist()

    # FIRST DEATH EQUIP VALUE
    freeze_ends_parse = parser.parse_event("round_freeze_end", other=["total_rounds_played"])
    freeze_ends = dict(zip(freeze_ends_parse["total_rounds_played"], freeze_ends_parse["tick"]))
    deaths = parser.parse_event("player_death", player=["team_name"], other=["total_rounds_played", "is_warmup_period"])
    deaths = deaths[deaths["attacker_team_name"] != deaths["user_team_name"]]
    deaths = deaths[deaths["is_warmup_period"] == False]
    deaths = deaths.dropna(subset=["attacker_name"])  # take out people dying to bomb after time
    # discards deaths that happen in the current round before the end of current round freeze time
    # this fixes the edge case where someone dies in the previous round but after time
    deaths = deaths[deaths["tick"] >= deaths["total_rounds_played"].map(freeze_ends)]
    deaths = deaths.sort_values("tick").drop_duplicates("total_rounds_played", keep="first")
    wanted = deaths["tick"].tolist()
    # parses purchases at each of the desired ticks (at the first death of the round of play)
    df = parser.parse_ticks(["current_equip_value", "total_rounds_played", "team_name"], ticks=wanted)
    max_round = df["total_rounds_played"].max() + 1
    df_filtered = df[~df["total_rounds_played"].isin([0, 12])] # filter out pistol rounds
    # 3300 is the breakpoint standard used by HLTV analysts for an eco buy
    breakpoint = 3300
    team_breakpoint = breakpoint*5 # evaluating on a whole team basis, so *5 is necessary
    # split data based on "current_equip_value" of the entire team
    team_spending = df_filtered.groupby(["total_rounds_played", "team_name"])["current_equip_value"].sum().reset_index()
    eco_rounds = team_spending[team_spending["current_equip_value"] < team_breakpoint]
    anti_df = pd.DataFrame() # initialization of dataframe
    for round in set(eco_rounds["total_rounds_played"]):
        subdf = df_filtered[df_filtered["total_rounds_played"] == round]
        eco_sub = eco_rounds[eco_rounds["total_rounds_played"] == round]
        eco_team = subdf.loc[subdf["team_name"] == eco_sub["team_name"].item()] # returns which team is saving
        anti_team = subdf.loc[subdf["team_name"] != eco_sub["team_name"].item()] # returns which team is playing against a save round
        anti_df = pd.concat([anti_df, anti_team])
    del anti_df["tick"]
    del anti_df["current_equip_value"]
    # print(anti_df)
    return anti_df

def get_anti_stats(replay_path):
    """Get desired statistics from anti eco rounds"""
    antis = get_antis(replay_path)
    stats = None # set to none for now
    # loop through all anti eco rounds
    for round in set(antis["total_rounds_played"]):
        # need opening kills and general statistics from other functions for anti eco rounds
        temp_stats = get_round_stats(replay_path, round)
        # temp_stats["steamid"] = temp_stats["steamid"].astype("object")
        temp_openings = get_round_openings(replay_path, round)
        temp_stats[["first_kills", "first_deaths"]] = temp_stats["steamid"].map(temp_openings).apply(pd.Series)
        del temp_stats["name"] # deleting name category since it can't be converted/added
        temp_stats.drop(["steamid"], axis=1, inplace=True) # exclude steamid from addition
        # add results
        if stats is None:
            stats = temp_stats
        else:
            stats = stats.add(temp_stats, fill_value=0)
    stats.columns = stats.columns.str.replace("_total", "", regex=False)
    stats = stats.add_prefix("antieco_").reset_index(drop=True)
    # print(stats)
    return(stats)

def get_all_stats(replay_path):
    """Combine all statistics into one dataframe for one replay"""
    basic_stats = get_stats(replay_path)  # DATAFRAME
    pistol_stats = get_pistol_stats(replay_path)  # DATAFRAME
    anti_stats = get_anti_stats(replay_path)  # DATAFRAME
    clutch_stats = get_clutches(replay_path)  # DICT
    opening_stats = get_opening_stats(replay_path)  # DICT
    KAST = get_KAST(replay_path)  # DICT
    # merge dataframes
    all_stats = pd.merge(basic_stats, pistol_stats, on=["steamid", "name"])
    all_stats = all_stats.join(anti_stats)
    all_stats.insert(0, "steamid", all_stats.pop("steamid"))
    all_stats.insert(0, "name", all_stats.pop("name"))
    # add dicts as columns
    all_stats["clutches_won"] = all_stats["steamid"].map(clutch_stats)
    all_stats[["first_kills", "first_deaths"]] = all_stats["steamid"].map(opening_stats).apply(pd.Series)
    all_stats["KAST"] = all_stats["steamid"].map(KAST)
    # print(all_stats)
    return all_stats

def get_everything(demo_folder, output_csv):
    """Collects relevant info from each demo in the specified folder, combines it, then outputs into one csv. If the csv already exists, it will add onto its dataframe"""
    if os.path.exists(output_csv):
        everything = pd.read_csv(output_csv)
    else:
        everything = None
    for filename in os.listdir(demo_folder):
        if filename in database["parsed_matches"]:
            continue
        if filename.endswith(".dem"):  # Check for demo files
            demo_path = os.path.join(demo_folder, filename)
            temp = get_all_stats(demo_path)
            # combine dataframes
            combined_df = pd.concat([everything, temp])
            # if players have multiple names, this will return a list of their unique nicknames
            name_mapping = combined_df.groupby("steamid")["name"].apply(lambda x: list(set(x)))
            # sum columns based on steamid
            everything = combined_df.groupby("steamid", as_index=False).sum()
            # add the names back to the dataframe
            everything["name"] = everything["steamid"].map(name_mapping)
            # if no errors?
            database["parsed_matches"].append(filename)
    everything.to_csv(output_csv, index=False)

# Execution
# get_names(this_path)
# get_round_stats(this_path, 5)
# get_stats(this_path)
# get_pistol_stats(this_path)
# get_KAST(this_path)
# get_clutches(this_path)
# get_clutches(second_path)
# get_clutches(third_path)
# get_round_openings(this_path, 5)
# get_opening_stats(this_path)
# get_antis(this_path)
# get_anti_stats(this_path)
# get_everything()
