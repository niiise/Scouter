Scouter seeks to assemble a database of individual player statistics for the scouting and analytics purposes of the amateur North American Counter-Strike scene. 

Statistics collected include:
Traditional scoreboard statistics (kills, deaths, damage, etc.)
Opening statistics (opening kills, opening deaths, opening kill round win conversions, traded opening deaths)
Multikill statistics (multikills, multikill round win conversions)
Pistol statistics (pistol kills, deaths, hs%)
Clutch statistics (clutch wins)
Trade statistics (KAST, trade kills, traded deaths)


Files were obtained through FACEIT's API, and are replays from season 51 of their advanced division in the region of North America.
This project makes use of LaihoE's demoparser tool (found at https://github.com/LaihoE/demoparser) to parse and compile Counter-Strike 2 player statistics from CS2 .dem files. 


This project was done in fulfillment of capstone project requirements for my MS in Business Analytics at Northwood University (MGT 683).


Future plans:
More detailed clutch statistics (attempts, save rate, etc.)
More detailed utility statistics (number of nades thrown total, util to kill conversion rate)
Accuracy statistics (hs per bullet shot, smoke spam kills)
Positional statistics (determining player roles and therefore target statistics for each role)
Stratification of ratings per map
