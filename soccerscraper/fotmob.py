import os
import re
import json
import time
import random

import pandas as pd

from pathlib import Path
from typing import Optional, Callable, Union
from collections.abc import Iterable

from _classes import RequestReader
from _cfg import DATA_DIR, NOCACHE, NOSTORE, TOR_PROXIES, HEADERS, LEAGUE_DICT, logger

FOTMOB_DATADIR = DATA_DIR / "FotMob"
FOTMOB_API = "https://www.fotmob.com/api/"

random.seed(159)

HEADERS["Referer"] = "https://www.fotmob.com/",
HEADERS["Origin"] = "https://www.fotmob.com",


class FotMob(RequestReader):

    def __init__(
            self,
            leagues: Optional[Union[str, list[str]]] = None,
            seasons: Optional[Union[str, int, Iterable[Union[str, int]]]] = None,
            proxy: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            header: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            no_cache: bool = NOCACHE,
            no_store: bool = NOSTORE,
            data_dir: Path = FOTMOB_DATADIR,
    ):
        """Initialize the FotMob reader."""
        super().__init__(
            leagues=leagues,
            proxy=proxy,
            header=header,
            no_cache=no_cache,
            no_store=no_store,
            data_dir=data_dir,
        )
        self.seasons = seasons  # type: ignore
        if not self.no_store:
            (self.data_dir / "leagues").mkdir(parents=True, exist_ok=True)
            (self.data_dir / "seasons").mkdir(parents=True, exist_ok=True)
            (self.data_dir / "matches").mkdir(parents=True, exist_ok=True)

    def _init_session(self) -> requests.Session:
        session = super()._init_session()
        try:
            r = requests.get("http://46.101.91.154:6006/")
            r.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise ConnectionError("Unable to connect to the session cookie server.")
        result = r.json()
        session.headers.update(result)
        return session

    def read_leagues(self) -> pd.DataFrame:
        """Retrieve the selected leagues from the datasource.

        Returns
        -------
        pd.DataFrame
        """
        url = FOTMOB_API + "allLeagues"
        filepath = self.data_dir / "allLeagues.json"
        reader = self.get(url, filepath)
        data = json.load(reader)
        leagues = []
        for k, v in data.items():
            if k == "international":
                for int_league in v[0]["leagues"]:
                    leagues.append(
                        {
                            "league": int_league["name"],
                            "leagueId": int_league["id"],
                            "leagueRegion": v[0]["ccode"],
                            "leagueUrl": "https://fotmob.com" + int_league["pageUrl"],
                        }
                    )
            elif k not in ("favourite", "popular", "userSettings"):
                for country in v:
                    for dom_league in country["leagues"]:
                        leagues.append(
                            {
                                "league": dom_league["name"],
                                "leagueId": dom_league["id"],
                                "leagueRegion": country["ccode"],
                                "leagueUrl": "https://fotmob.com" + dom_league["pageUrl"],
                            }
                        )

        df = (
            pd.DataFrame(leagues)
            .assign(league=lambda x: x.leagueRegion + "-" + x.league)
            .pipe(self._translate_league, level="code")
            .set_index("league")
            .loc[self._selected_leagues.keys()]
            .sort_index()
        )
        return df[df.index.isin(self.leagues)]

    def read_seasons(self) -> pd.DataFrame:
        """Retrieve the selected seasons for the selected leagues.

        Returns
        -------
        pd.DataFrame
        """
        filemask = "leagues/{}.json"
        urlmask = FOTMOB_API + "leagues?id={}"
        df_leagues = self.read_leagues()
        seasons = []
        for lkey, league in df_leagues.iterrows():
            print(lkey)
            url = urlmask.format(league.leagueId)
            filepath = self.data_dir / filemask.format(lkey)
            reader = self.get(url, filepath)
            data = json.load(reader)
            avail_seasons = data["allAvailableSeasons"]
            for season in avail_seasons:
                seasons.append(
                    {
                        "league": lkey,
                        "leagueId": league.leagueId,
                        "seasonId": season,
                        "season": season.replace('/', '-'),
                        "seasonUrl": league.leagueUrl + "?season=" + season,
                    }
                )
        df = pd.DataFrame(seasons).set_index(["league", "seasonId"]).sort_index()
        return df

    def read_schedule(self, force_cache: bool = False) -> pd.DataFrame:

        filemask = "seasons/{}_{}.html"
        urlmask = FOTMOB_API + "leagues?id={}&season={}"

        cols = [
            "league",
            "leagueId",
            "season",
            "seasonId",
            "match",
            "matchId",
            "matchRound",
            "matchDate",
            "matchStatus",
            "homeTeam",
            "homeTeamId",
            "awayTeam",
            "awayTeamId",
            "scoreHomeFullTime",
            "scoreAwayFullTime",
            "url",
        ]

        df_seasons = self.read_seasons()
        all_schedules = []
        for (lkey, skey), season in df_seasons.iterrows():
            season_string = skey.replace('/', '-')

            filepath = self.data_dir / filemask.format(lkey, season_string)
            url = urlmask.format(season.leagueId, skey)
            reader = self.get(url, filepath, no_cache=force_cache)
            season_data = json.load(reader)

            df = pd.json_normalize(season_data["matches"]["allMatches"])
            df["league"] = lkey
            df["leagueId"] = season["leagueId"]
            df["seasonId"] = skey
            df["season"] = skey.replace('/', '-')
            all_schedules.append(df)


        df = (
            pd.concat(all_schedules)
            .rename(
                columns={
                    "roundName": "matchRound",
                    "round": "matchWeek",
                    "home.name": "homeTeam",
                    "away.name": "awayTeam",
                    "status.reason.short": "matchStatus",
                    "pageUrl": "url",
                    "id": "matchId",
                    "home.id": "homeTeamId",
                    "away.id": "awayTeamId",
                }
            )
            .replace({"homeTeam": TEAMNAME_REPLACEMENTS, "awayTeam": TEAMNAME_REPLACEMENTS,})
            .assign(matchDate=lambda x: pd.to_datetime(x["status.utcTime"], format="mixed"))
            .drop(columns=['matchWeek',])
        )
        df["matchDescription"] = df.apply(make_game_id, axis=1)
        df["match"] = df['matchDescription'].copy()
        df["url"] = "https://fotmob.com" + df["url"]
        df[["scoreHomeFullTime", "scoreAwayFullTime"]] = df["status.scoreStr"].str.split("-", expand=True)



        return df.sort_values('matchDate')[cols]

    def read_games(self,
                   team: Optional[Union[str, list[str]]] = None,
                   force_cache: bool = False,
                   ) -> list: 

        filemask = "matches/{}_{}_{}.html"
        urlmask = FOTMOB_API + "matchDetails?matchId={}"

        # Retrieve games for which a match report is available
        df_matches = self.read_schedule(force_cache)
        df_complete = df_matches.loc[df_matches["matchStatus"].isin(["FT", "AET", "Pen"])]

        if team is not None:
            iterator = df_complete.loc[
                (
                        df_complete.home_team.isin(team)
                        | df_complete.away_team.isin(team)
                )
            ]
            if len(iterator) == 0:
                raise ValueError("No data found for the given teams in the selected seasons.")
        else:
            iterator = df_complete

        stats = []
        for i, game in iterator.reset_index().iterrows():
            lkey, skey, gkey = game["league"], game["season"], game["match"]

            season_string = skey.replace('/', '-')

            url = urlmask.format(game.gameId)
            filepath = self.data_dir / filemask.format(lkey, season_string, game.gameId)
            reader = self.get(url, filepath, no_cache=force_cache)
            print(f"[{i + 1}/{len(iterator)}] Retrieving game with id={game['gameId']}")

            game_data = json.load(reader)

            stats.append(game_data)

        return stats

