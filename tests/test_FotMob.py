from fotmob import FotMob

leagues = ["DEN-Superliga", "NOR-Eliteserien", "SWE-Allsvenskan"]
fm = FotMob(leagues=leagues, header=HEADERS, proxy=TOR_PROXIES)

fm_leagues = fm.read_leagues()
fm_seasons = fm.read_seasons()
fm_games = fm.read_schedule()
fm_matches = fm.read_games()

