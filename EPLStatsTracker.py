# CREATE WEEKLY REPEAT VIA CRON OR WINDOWS SCHEDULER

import json
import requests
import sqlite3 as sql
import pandas as pd
import docx # pip install python-docx
import asyncio
import nest_asyncio
import json
import aiohttp
from understat import Understat
from fuzzywuzzy import fuzz

pd.set_option('precision', 3)
pd.options.display.precision = 1


### Scrape FPL API ### 

# Connects to the API and converts to json
url = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
data = url.json()

# Clean version of the data that is readable if needed
data_clean = json.dumps(data, indent = 2)
parsed_data = json.loads(data_clean)

# Checks which game week the API is updated to
gameweeks = data['events']
for gameweek in gameweeks:
    if gameweek['is_current'] == True:
        week = gameweek['name']
        break
    else:
        continue 

# Creates the database name to connect to depending on what Gameweek it is
week = week.replace(' ', '')
season = '2020'

# Saves json clean format to word doc for reading
jsonclean = 'FPL-{}-{}.docx'.format(season, week)
doc = docx.Document()
doc.add_paragraph(data_clean)
doc.save(jsonclean)

### Scrape Understat ###
nest_asyncio.apply()

ustat = []

# Connects to understat and retrieves all data from get_league_players
async def main():
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        player = await understat.get_league_players(
            "epl", 2020,
        )
        #print(json.dumps(players))
        ustat.append(json.dumps(player))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

# Save understat json to doc
ustat_jsonclean = 'Understat-{}-{}.docx'.format(season, week)
doc = docx.Document()
doc.add_paragraph(ustat)
doc.save(ustat_jsonclean)

### Convert to Pandas DF, save as CSV ###

## FPL

players = data['elements']

df = pd.DataFrame(players)

cols = ['first_name', 'second_name', 'web_name', 'id', 'element_type', 'team', 'team_code', 'total_points', 'goals_scored', 'assists', 'clean_sheets', 'bonus', 'saves', 'yellow_cards', 'red_cards', 'form', 
        'points_per_game', 'penalties_saved', 'penalties_missed', 'influence', 'creativity', 'threat', 'ict_index', 'transfers_in_event', 'transfers_out_event' ]
df = df[cols]

## Understat
ustat = json.loads(ustat[0])
dfu = pd.DataFrame(ustat)

colsu = ['key_passes', 'npg', 'npxG', 'player_name', 'shots', 'xA', 'xG', 'xGBuildup', 'xGChain']
dfu = dfu[colsu]

# Split player names
dfu.loc[:, 'first_name'] = dfu['player_name'].str.split(' ').str[0]
dfu.loc[:, 'second_name'] = dfu['player_name'].str.split(' ').str[-1]

df['player_name'] = df['first_name'] + ' ' + df['second_name']


# Finds missing names in both datasets
names_u = list(set(list(dfu.player_name)) - set(list(df.player_name)))
names =  list(set(list(df.player_name)) - set(list(dfu.player_name)))

# Splits understat name and finds surname 
matches = []
for name in names_u:
    sub = name.split(' ')[-1]
    match = [(name, s) for s in names if sub in s]
    matches.append(match)
    
# Finds name matches in both datasets by fuzzy string matching
name_change = {}
for match in matches: 
    # If only one match, save as key-value pair
    if len(match) == 1:
        name_change[match[0][1]] = match[0][0]
        matches.remove(match)
    # If multiple matches, take the key-value pair with highest fuzzy match ratio
    elif len(match) > 1:
        prefu = 0
        for m in match:
            fu = fuzz.ratio(m[0], m[1])
            if fu > prefu:
                save = m
                prefu = fu
        name_change[save[1]] = save[0]
        matches.remove(match)

# Change instances of player name to Ustat name formatting
df.loc[:, 'player_name'].replace(name_change, inplace=True)

# Join DataFrames
df = pd.concat([df.set_index('player_name'), dfu.set_index('player_name')], axis=1, join='inner').reset_index()
# Drop unnecessary columns
df.drop(['first_name', 'team_code', 'second_name', 'first_name', 'second_name'], axis=1, inplace=True)
# Change Element type to position
position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
df.loc[:, 'element_type'] = df['element_type'].map(position_map);

df.rename(columns={'element_type':'position'}, inplace=True)

# Save gameweek data to csv
df.to_csv('FPL-{}-{}.csv'.format(season, week))

### Create SQLITE Database ###

# db name
db = 'EPL-Data-{}.sqlite'.format(season)

conn = sql.connect(db)
cur = conn.cursor()

# Create table that specifies gameweek and season
sql_create_player_table = '''
CREATE TABLE IF NOT EXISTS Player (
id    integer    PRIMARY KEY,
name  text       NOT NULL,
web_name    text    NOT NULL,
player_id    integer    NOT NULL,
position   text   NOT NULL,
team   text   NOT NULL,
total_points    integer    NOT NULL,
goals_scored    integer    NOT NULL,
assists    integer    NOT NULL,
clean_sheets    integer    NOT NULL,
bonus    integer    NOT NULL,
saves    integer     NOT NULL,
yellow_cards    integer    NOT NULL,
red_cards    integer    NOT NULL,
form     real    NOT NULL,
points_per_game    real    NOT NULL,
penalties_saved    integer    NOT NULL,
penalties_missed    integer    NOT NULL,
influence     real    NOT NULL,
creativity    real    NOT NULL,
threat    real    NOT NULL,
ict_index   real    NOT NULL,
transfers_in_event    integer    NOT NULL,
transfers_out_event    integer    NOT NULL,
key_passes    integer    NOT NULL,
npg    real    NOT NULL,
npxG    real    NOT NULL,
shots    integer    NOT NULL,
xA    real    NOT NULL,
xG    real    NOT NULL, 
xGBuildup    real    NOT NULL,
xGChain    real    NOT NULL,
gameweek    integer    NOT NULL
);'''


# Create SQL table in database
cur.execute(sql_create_player_table)
conn.commit()


### Insert gameweek data to sqlite table ### 

# Iterates over each row and inserts into db. All in the same table.
for i, player in df.iterrows():
    name = player['player_name']
    web_name = player['web_name']
    player_id = player['id']
    position = player['position']
    team = player['team']
    total_points = player['total_points']
    goals_scored = player['goals_scored']
    assists = player['assists']
    clean_sheets = player['clean_sheets']
    bonus = player['bonus']
    saves = player['saves']
    yellow_cards = player['yellow_cards']
    red_cards = player['red_cards']
    form = player['form']
    points_per_game = player['points_per_game']
    penalties_saved = player['penalties_saved']
    penalties_missed = player['penalties_missed']
    influence = player['influence']
    creativity = player['creativity']
    threat = player['threat']
    ict_index = player['ict_index']
    transfers_in_event = player['transfers_in_event']
    transfers_out_event = player['transfers_out_event']
    key_passes = player['key_passes']
    npg = player['npg']
    npxG = player['npxG']
    shots = player['shots']
    xA = player['xA']
    xG = player['xG']
    xGBuildup = player['xGBuildup']
    xGChain = player['xGChain']
    gameweek = week
    
    # Parameterised queries rather that ''.format() 
    cur.execute('''INSERT OR REPLACE INTO PLAYER
    (name, web_name, player_id, position, team, total_points, goals_scored, assists, clean_sheets,
    bonus, saves, yellow_cards, red_cards, form, points_per_game, penalties_saved, penalties_missed,
    influence, creativity, threat, ict_index, transfers_in_event, transfers_out_event, key_passes,
    npg, npxG, shots, xA, xG, xGBuildup, xGChain, gameweek) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (name, web_name, player_id, position, team, total_points, goals_scored, assists, clean_sheets,
    bonus, saves, yellow_cards, red_cards, form, points_per_game, penalties_saved, penalties_missed,
    influence, creativity, threat, ict_index, transfers_in_event, transfers_out_event, key_passes,
    npg, npxG, shots, xA, xG, xGBuildup, xGChain, gameweek) )

conn.commit()