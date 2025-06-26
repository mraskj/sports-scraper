from scoresway import Scoresway

leagues = ["DEN-Superliga", "NOR-Eliteserien", "SWE-Allsvenskan"]
sw = Scoresway(leagues=leagues, header=HEADERS, proxy=TOR_PROXIES)

sw_leagues = sw.read_leagues()
sw_seasons = sw.read_seasons()
sw_games = sw.read_matches()
sw_events = sw.read_events(dataframe=sw_games)

