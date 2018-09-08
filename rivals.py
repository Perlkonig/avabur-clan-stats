#!/usr/local/bin/python3

import requests
import json
import re
import sqlite3
from lomond.websocket import WebSocket
from lomond.persist import persist
import lomond.events as events
from time import sleep
import lzstring
lz = lzstring.LZString()

s = requests.Session()

#Load settings
with open('/home/protected/avabur/settings.json') as j:
    settings = json.load(j)

#Load/Initialize database
try:
    conn = sqlite3.connect(settings['dbfile'])
except sqlite3.DatabaseError as e:
    raise sqlite3.DatabaseError(repr(e))
c = conn.cursor()

try:
    c.execute('''
        CREATE TABLE IF NOT EXISTS rivalclans (
            clanid INTEGER,
            datestamp STRING,
            xp INTEGER,
            level INTEGER,
            PRIMARY KEY (clanid, datestamp)
        );
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS rivals (
            username STRING,
            datestamp STRING,
            clanid INTEGER,
            level INTEGER,
            fishing INTEGER,
            woodcutting INTEGER,
            mining INTEGER,
            stonecutting INTEGER,
            crafting INTEGER,
            carving INTEGER,
            stats INTEGER,
            kills INTEGER,
            deaths INTEGER,
            harvests INTEGER,
            resources INTEGER,
            craftingacts INTEGER,
            carvingacts INTEGER,
            quests INTEGER,
            lastactive INTEGER,
            totalacts INTEGER,
            PRIMARY KEY (username, datestamp)
        );
    ''')
except Exception as e:
    conn.rollback()
    c.close()
    conn.close()
    raise RuntimeError(repr(e))
conn.commit()
c.close()

#Authenticate
r = s.post('https://avabur.com/login', data={"acctname": settings['username'], "password": settings['password']});
try:
    login = r.json()
    assert "s" in login
except:
    raise RuntimeError("Could not understand server response to login attempt. Aborting.")
if login['s'] == 0:
    raise RuntimeError("Authentication failed: {}".format(login['m']))
print("Login successful")

#Extract data from websockets for later processing
cdict = requests.utils.dict_from_cookiejar(s.cookies)
cookies = list()
for k in cdict:
    cookies.append('='.join([k,cdict[k]]))
cstr = ';'.join(cookies)

ws = WebSocket('wss://avabur.com/websocket')
ws.add_header('cookie'.encode('utf-8'), cstr.encode('utf-8'))

msgs = dict()
msgs['clan_profile'] = json.dumps({'name': '%%', 'type': 'page', 'page': 'clan_view'})
msgs['clan_members'] = json.dumps({'clan': '%%', 'type': 'page', 'page': 'clan_members'})
msgs['profile'] = json.dumps({'type': 'page', 'page': 'profile', 'username': '%%'})

clans = list()
profiles = dict()
listsRequested = 0
entriesExpected = 0

battles = 0
for event in ws:
    #print(event)
    try:
        if isinstance(event, events.Ready):
            for rival in settings['rivals']:
                print("Requesting clan profile for clan " + rival['name'])
                ws.send_text(msgs['clan_profile'].replace('%%', str(rival['id'])))
                sleep(1)

        if isinstance(event, events.Text):
            j = json.loads(event.text)[0]
            if 'type' in j:
                if (j['type'] == 'page') and (j['page'] == 'clan_view'):
                    print("Processing clan profile")
                    node = dict()
                    node['id'] = j['result']['id']
                    node['xp'] = j['result']['experience']
                    node['level'] = j['result']['level']
                    clans.append(node)
                    print("Requesting membership list for clan " + j['result']['name'])
                    ws.send_text(msgs['clan_members'].replace('%%', str(node['id'])))
                    listsRequested += 1
                    sleep(1)

        if isinstance(event, events.Text):
            j = json.loads(event.text)[0]
            if 'type' in j:
                if (j['type'] == 'page') and (j['page'] == 'clan_members'):
                    print("Processing clan membership list")
                    members = json.loads(json.dumps(j))
                    #get count first to make sure we don't terminate prematurely
                    for m in j['members']:
                        if int(m['rankid']) >= 0:
                            entriesExpected += 1

                    for m in j['members']:
                        if int(m['rankid']) < 0:
                            continue
                        sleep(1)
                        print("Requesting profile for {}".format(m['username'].upper()))
                        ws.send_text(msgs['profile'].replace('%%', m['username']))

        if isinstance(event, events.Text):
            j = json.loads(event.text)[0]
            if 'type' in j:
                if (j['type'] == 'page') and (j['page'] == 'profile'):
                    rec = j['result']
                    print('Receiving profile for {}'.format(rec['username'].upper()))
                    node = dict()
                    node['username'] = rec['username']
                    for rival in settings['rivals']:
                        if rec['clan']['name'] == rival['name']:
                            node['clanid'] = rival['id']
                    node['level'] = rec['levels']['character']['level']
                    node['rank_level'] = rec['levels']['character']['rank']
                    node['fishing'] = rec['levels']['fishing']['level']
                    node['rank_fishing'] = rec['levels']['fishing']['rank']
                    node['woodcutting'] = rec['levels']['woodcutting']['level']
                    node['rank_woodcutting'] = rec['levels']['woodcutting']['rank']
                    node['mining'] = rec['levels']['mining']['level']
                    node['rank_mining'] = rec['levels']['mining']['rank']
                    node['stonecutting'] = rec['levels']['stonecutting']['level']
                    node['rank_stonecutting'] = rec['levels']['stonecutting']['rank']
                    node['crafting'] = rec['levels']['crafting']['level']
                    node['rank_crafting'] = rec['levels']['crafting']['rank']
                    node['carving'] = rec['levels']['carving']['level']
                    node['rank_carving'] = rec['levels']['carving']['rank']
                    node['house'] = rec['levels']['house']['level']
                    node['rank_house'] = rec['levels']['house']['rank']
                    node['stats'] = rec['stats']['base']['value']
                    node['rank_stats'] = rec['stats']['base']['rank']
                    node['kills'] = rec['battle']['kills']['value']
                    node['rank_kills'] = rec['battle']['kills']['rank']
                    node['deaths'] = rec['battle']['deaths']['value']
                    node['rank_deaths'] = rec['battle']['deaths']['rank']
                    node['harvests'] = rec['harvests']['harvests']['value']
                    node['rank_harvests'] = rec['harvests']['harvests']['rank']
                    node['resources'] = rec['harvests']['resources']['value']
                    node['rank_resources'] = rec['harvests']['resources']['rank']
                    node['crafting_acts'] = rec['profession']['crafts']['value']
                    node['rank_crafting_acts'] = rec['profession']['crafts']['rank']
                    node['carving_acts'] = rec['profession']['carves']['value']
                    node['rank_carving_acts'] = rec['profession']['carves']['rank']
                    node['quests'] = rec['quest']['total']['value']
                    node['rank_quests'] = rec['quest']['total']['rank']

                    profiles[rec['username']] = node

                    #check for termination
                    if ( (listsRequested == len(settings['rivals'])) and (len(profiles) == entriesExpected) ):
                        print("Looks like we have everything we need. Shutting down.")
                        ws.close()

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        ws.close()

print("Data fetched")

# Store it
c = conn.cursor()
try:
    #clan data
    for clan in clans:
        c.execute("REPLACE INTO rivalclans (datestamp, clanid, xp, level) VALUES (date('now'), ?, ?, ?)", (clan['id'], clan['xp'], clan['level']))

    #individual data
    for member in profiles.values():
        totalacts = member['kills'] + member['deaths'] + member['harvests'] + member['crafting_acts'] + member['carving_acts']
        c.execute("REPLACE INTO rivals (username, clanid, datestamp, level, fishing, woodcutting, mining, stonecutting, crafting, carving, stats, kills, deaths, harvests, resources, craftingacts, carvingacts, quests, totalacts) VALUES (?, ?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (member['username'], int(member['clanid']), member['level'], member['fishing'], member['woodcutting'], member['mining'], member['stonecutting'], member['crafting'], member['carving'], member['stats'], member['kills'], member['deaths'], member['harvests'], member['resources'], member['crafting_acts'], member['carving_acts'], member['quests'], totalacts))

except Exception as e:
    conn.rollback()
    c.close()
    conn.close()
    raise RuntimeError("An error occured while storing fetched data: {}".format(repr(e)))
conn.commit()
c.close()
print("Data stored")
conn.close()

