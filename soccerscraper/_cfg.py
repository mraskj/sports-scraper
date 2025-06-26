import os
import sys
import json
from pathlib import Path

import logging.config
from rich.logging import RichHandler

# Configuration
NOCACHE = os.environ.get("SOCCERSCRAPER_NOCACHE", "False").lower() in ("true", "1", "t")
NOSTORE = os.environ.get("SOCCERSCRAPER_NOSTORE", "False").lower() in ("true", "1", "t")

# Directories
BASE_DIR = Path(os.environ.get("SOCCERSCRAPER_DIR", Path(Path.home(), 'Dropbox/sport/soccer/soccerdata')))
LOGS_DIR = Path(BASE_DIR, "logs")
DATA_DIR = Path(BASE_DIR, "data")
CONFIG_DIR = Path(BASE_DIR, "config")

MAXAGE = None
if os.environ.get("SOCCERSCRAPER_MAXAGE") is not None:
    MAXAGE = int(os.environ.get("SOCCERSCRAPER_MAXAGE", 0))

# Create dirs
LOGS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


LOGLEVEL = os.environ.get("SOCCERSCRAPER_LOGLEVEL", "INFO").upper()

TOR_PROXIES = {'http': 'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 "
                  "Safari/537.36",
    "Referer": None,
    "Origin": None,
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Dest": "document",
    "Connection": "keep-alive",
    "Accept-Language": "en-US,en;q=0.5",
}

# Logger
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "minimal": {"format": "%(message)s"},
        "detailed": {
            "format": "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d]\n%(message)s\n"  # noqa: E501
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "minimal",
            "level": logging.DEBUG,
        },
        "info": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "info.log"),
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "error.log"),
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.ERROR,
        },
    },
    "loggers": {
        "root": {
            "handlers": ["console", "info", "error"],
            "level": LOGLEVEL,
            "propagate": True,
        },
    },
}
logging.config.dictConfig(logging_config)
logging.captureWarnings(True)
logger = logging.getLogger("root")
logger.handlers[0] = RichHandler(markup=True)


# League dict
LEAGUE_DICT = {
    "ENG-Premier League": {
        "FBref": "Premier League",
        "FotMob": "ENG-Premier League",
        "Scoresway": "Premier League",
        "countryName": "England",
        "countryCode": "ENG"
    },
    "ENG-Championship": {
        "FBref": "Championship",
        "FotMob": "ENG-Championship",
        "Scoresway": "Championship",
        "countryName": "England",
        "countryCode": "ENG"
    },
    "ENG-League One": {
            "FBref": "League One",
            "FotMob": "ENG-League One",
            "Scoresway": "League One",
            "countryName": "England",
            "countryCode": "ENG"
    },
    "ENG-FA Cup": {
        "FBref": "FA Cup",
        "FotMob": "ENG-FA Cup",
        "Scoresway": "FA Cup",
        "countryName": "England",
        "countryCode": "ENG",
        "optaEvents": "2013-2014"
    },
    "ENG-EFL Cup": {
        "FBref": "EFL Cup",
        "FotMob": "ENG-EFL Cup",
        "Scoresway": "League Cup",
        "countryName": "England",
        "countryCode": "ENG"
    },
    "ESP-La Liga": {
        "FBref": "La Liga",
        "FotMob": "ESP-LaLiga",
        "Scoresway": "Primera División",
        "countryName": "Spain",
        "countryCode": "ESP"
    },
    "ESP-La Liga 2": {
        "FBref": "La Liga",
        "FotMob": "ESP-LaLiga2",
        "Scoresway": "Segunda División",
        "countryName": "Spain",
        "countryCode": "ESP"
    },
    "ITA-Serie A": {
        "FBref": "Serie A",
        "FotMob": "ITA-Serie A",
        "Scoresway": "Serie A",
        "countryName": "Italy",
        "countryCode": "ITA"
    },
    "ITA-Serie B": {
        "FBref": "Serie B",
        "FotMob": "ITA-Serie B",
        "Scoresway": "Serie B",
        "countryName": "Italy",
        "countryCode": "ITA"
    },
    "GER-Bundesliga": {
        "FBref": "Fußball-Bundesliga",
        "FotMob": "GER-Bundesliga",
        "Scoresway": "Bundesliga",
        "countryName": "Germany",
        "countryCode": "GER"
    },
    "GER-2 Bundesliga": {
        "FBref": "2. Fußball-Bundesliga",
        "FotMob": "GER-2. Bundesliga",
        "Scoresway": "2. Bundesliga",
        "countryName": "Germany",
        "countryCode": "GER"
    },
    "FRA-Ligue 1": {
        "FBref": "Ligue 1",
        "FotMob": "FRA-Ligue 1",
        "Scoresway": "Ligue 1",
        "countryName": "France",
        "countryCode": "FRA"
    },
    "FRA-Ligue 2": {
        "FotMob": "FRA-Ligue 2",
        "Scoresway": "Ligue 2",
        "countryName": "France",
        "countryCode": "FRA"
    },
    "DEN-Superliga": {
        "FBref": "Danish Superliga",
        "FotMob": "DEN-Superligaen",
        "Scoresway": "Superliga",
        "countryName": "Denmark",
        "countryCode": "DEN",
        "optaEvents": "2018-2019"
    },
    "DEN-1 Division": {
        "FotMob": "DEN-1. Division",
        "Scoresway": "1. Division",
        "countryName": "Denmark",
        "countryCode": "DEN",
        "optaEvents": "2021-2022"
    },
    "DEN-2 Division": {
        "FotMob": "DEN-2. Division",
        "Scoresway": "2. Division",
        "countryName": "Denmark",
        "countryCode": "DEN",
        "optaEvents": None,
    },
    "DEN-DBU Pokalen": {
        "FotMob": "DEN-DBU Pokalen",
        "Scoresway": "DBU Pokalen",
        "countryName": "Denmark",
        "countryCode": "DEN",
        "optaEvents": "2021-2022"
    },
    "NED-Eredivisie": {
        "Scoresway": "Eredivisie",
        "countryName": "Netherlands",
        "countryCode": "NED",
        "optaEvents": "2013-2014"
    },
    "NED-Eerste Divisie": {
        "Scoresway": "Eerste Divisie",
        "countryName": "Netherlands",
        "countryCode": "NED",
        "optaEvents": "2013-2014"
    },
    "BEL-First Division A": {
        "Scoresway": "First Division A",
        "countryName": "Belgium",
        "countryCode": "BEL",
        "optaEvents": "2013-2014"
    },
    "AUT-Bundesliga": {
        "Scoresway": "Bundesliga",
        "countryName": "Austria",
        "countryCode": "AUT",
        "optaEvents": "2013-2014"
    },
    "AUT-2 Liga": {
        "Scoresway": "2. Liga",
        "countryName": "Austria",
        "countryCode": "AUT",
        "optaEvents": "2020-2021",
    },
    "USA-MLS": {
        "Scoresway": "MLS",
        "countryName": "USA",
        "countryCode": "USA",
        "optaEvents": "2013"
    },
    "JPN-J League": {
        "FotMob": "JPN-J. League",
        "Scoresway": "J1 League",
        "countryName": "Japan",
        "countryCode": "JPN",
        "optaEvents": "2015",
    },
    "KOR-K League 1": {
        "FotMob": "KOR-K League 1",
        "Scoresway": "K League 1",
        "countryName": "Korea Republic",
        "countryCode": "KOR",
        "optaEvents": "2018"
    },
    "POR-Liga Portugal": {
        "FotMob": "POR-Liga Portugal",
        "Scoresway": "Primeira Liga",
        "countryName": "Portugal",
        "countryCode": "POR",
        "optaEvents": "2013-2014",
    },
    "POR-Liga Portugal 2": {
            "FotMob": "POR-Liga Portugal 2",
            "Scoresway": "Segunda Liga",
            "countryName": "Portugal",
            "countryCode": "POR",
            "optaEvents": "2018-2019",
        },
    "CZE-Czech Liga": {
        "FotMob": "CZE-Czech Liga",
        "Scoresway": "Czech Liga",
        "countryName": "Czechia",
        "countryCode": "CZE",
        "optaEvents": "2019-2020"
    },
    "SWE-Allsvenskan": {
        "Scoresway": "Allsvenskan",
        "countryName": "Sweden",
        "countryCode": "SWE",
        "optaEvents": "2016"
    },
    "NOR-Eliteserien": {
        "Scoresway": "Eliteserien",
        "countryName": "Norway",
        "countryCode": "NOR",
        "optaEvents": "2014"
    },
    "INT-Champions League": {
        "FotMob": "INT-Champions League",
        "Scoresway": "UEFA Champions League",
        "countryName": "UEFA",
        "countryCode": "INT"
    },
    "INT-Champions League Qualification": {
        "FotMob": "INT-Champions League Qualification",
        "countryName": "International",
        "countryCode": "INT"
    },
    "INT-Europa League": {
        "FotMob": "INT-Europa League",
        "Scoresway": "UEFA Europa League",
        "countryName": "UEFA",
        "countryCode": "INT"
    },
    "INT-Europa League Qualification": {
        "FotMob": "INT-Europa League Qualification",
        "countryName": "UEFA",
        "countryCode": "INT"
    },
    "INT-Conference League": {
        "FotMob": "INT-Conference League",
        "Scoresway": "UEFA Conference League",
        "countryName": "UEFA",
        "countryCode": "INT"
    },
    "INT-Conference League Qualification": {
        "FotMob": "INT-Conference League Qualification",
        "countryName": "UEFA",
        "countryCode": "INT"
    },
}

# Team name replacements
TEAMNAME_REPLACEMENTS = {}
_f_custom_teamnname_replacements = CONFIG_DIR / "teamname_replacements.json"
if _f_custom_teamnname_replacements.is_file():
    with _f_custom_teamnname_replacements.open(encoding="utf8") as json_file:
        for team, to_replace_list in json.load(json_file).items():
            for to_replace in to_replace_list:
                TEAMNAME_REPLACEMENTS[to_replace] = team

