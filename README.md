Scouter seeks to assemble a database of individual player statistics for the scouting and analytics purposes of the amateur North American Counter-Strike scene. <br/>

Statistics collected include: <br/>
Traditional scoreboard statistics (kills, deaths, damage, etc.) <br/>
Opening statistics (opening kills, opening deaths, opening kill round win conversions, traded opening deaths) <br/>
Multikill statistics (multikills, multikill round win conversions) <br/>
Pistol statistics (pistol kills, deaths, hs%) <br/>
Clutch statistics (clutch wins) <br/>
Trade statistics (KAST, trade kills, traded deaths) <br/>


Files were obtained through FACEIT's API, and are replays from season 51 of their advanced division in the region of North America. 
Scouter makes use of LaihoE's demoparser tool (found at https://github.com/LaihoE/demoparser) to parse and compile Counter-Strike 2 player statistics from CS2 .dem files. 
This project was done in fulfillment of capstone project requirements for an MS degree in Business Analytics at Northwood University (MGT 683). <br/>

The parser itself currently has errors processing replays where players have had connectivity issues, which causes some errors with rounds played. Therefore, the stats below are a WIP. <br/>
For a detailed write-up, read [here](https://drive.google.com/file/d/1ToyujwOU4a1vT85PSfO8bE3BSYIg8DAo/view?usp=sharing). <br/>
For tentative visualizations, see [here](https://public.tableau.com/app/profile/benjamin.mauldin/viz/PlayoffStats_17342277879400/Aggression). <br/>
For links to the stats themselves, see [here](https://docs.google.com/spreadsheets/d/14VX9sPJ73NpPNvmdsaxyJrjUthXKz7p-TGmh6vEIOxs/edit?gid=908267265#gid=908267265) for regular season and [here](https://docs.google.com/spreadsheets/d/1AIXK8a1fjrWflYPCML3fzShTOimCZ4MbEYmTgOKh5CA/edit?gid=444461729#gid=444461729) for playoffs.


Future plans: <br/>
More detailed clutch statistics (attempts, save rate, etc.) <br/>
More detailed utility statistics (number of nades thrown total, util to kill conversion rate) <br/>
Accuracy statistics (hs per bullet shot, smoke spam kills) <br/>
Positional statistics (determining player roles and therefore target statistics for each role) <br/>
Stratification of ratings per map <br/>
