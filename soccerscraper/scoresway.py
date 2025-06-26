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

SCORESWAY_DATADIR = DATA_DIR / "scoresway"
SCORESWAY_URL = "https://www.scoresway.com"
SCORESWAY_API = "https://api.performfeeds.com/soccerdata"

random.seed(159)

HEADERS["Referer"] = "https://www.scoresway.com/",
HEADERS["Origin"] = "https://www.scoresway.com",


class Scoresway(RequestReader):

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
            data_dir: Path = SCORESWAY_DATADIR,
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

    def read_leagues(self):
        """Retrieve the selected leagues from the datasource.

        Returns
        -------
        pd.DataFrame
        """
        data = self._read_leagues()
        leagues = {}

        continent_data = data["continents"]
        for i, continent_stat in enumerate(continent_data):

            continent_id = continent_stat['id']
            continent = continent_stat['name']
            country_data = continent_stat['countries']

            for ii, country_stat in enumerate(country_data):

                country_id = country_stat['id']
                country = country_stat['name']
                league_data = country_stat['comps']

                for iii, league_stat in enumerate(league_data):

                    league_id = league_stat["id"]

                    if league_id not in leagues:
                        league_name = league_stat["name"]

                        league_url = None
                        if "url" in league_stat.keys():
                            league_url = league_stat["url"]

                        leagues[league_id] = {
                            "continentId": continent_id,
                            "continent": continent,
                            "countryId": country_id,
                            "country": country,
                            "leagueId": league_id,
                            "league": league_name,
                            "url": league_url,
                        }

        index = "league"
        if len(leagues) == 0:
            return pd.DataFrame(index=index)
        # return leagues
        df = (
            pd.DataFrame.from_records(list(leagues.values()))
            .pipe(self._translate_league)
            .set_index(index)
            .sort_index()
            .convert_dtypes()
        )
        valid_leagues = [league for league in self.leagues if league in df.index]
        return df.loc[valid_leagues]

    
    def _read_leagues(self, no_cache: bool = False) -> dict:
        url = SCORESWAY_URL + "/en_GB/soccer/competitions"
        filepath = self.data_dir / "leagues.json"
        response = self.get(url, filepath, no_cache=no_cache, var="continents")
        return json.load(response)

    
    def read_seasons(self) -> pd.DataFrame:
        """Retrieve the selected seasons for the selected leagues.

        Returns
        -------
        pd.DataFrame
        """
        filemask = "leagues/{}.json"
        urlmask = SCORESWAY_URL + "{}"
        df_leagues = self.read_leagues()
        seasons = []
        for lkey, league in df_leagues.iterrows():

            url = urlmask.format(league.url.replace('fixtures', 'results'))
            filepath = self.data_dir / filemask.format(lkey)
            reader = self.get(url, filepath, var="allAvailableSeasons")
            data = json.load(reader)

            avail_seasons = data["allAvailableSeasons"]
            for season in avail_seasons:

                url = season.replace('fixtures', 'results')

                match_ = re.search(pattern=r'/soccer/.*\d{4}/(.*)/[a-z]+', string=url)

                if match_ is None:
                    season_id = None
                else:
                    season_id = match_.group(1)
                seasons.append(
                    {
                        "league": lkey,
                        "leagueId": league.leagueId,
                        "seasonId": season_id,
                        "url": url
                    }
                )

        df = pd.DataFrame(seasons).set_index(["league", "seasonId"])

        df["year"] = df["url"].str.extract(r'soccer/.*(\d{4}?)/').astype(int)
        df["season"] = df.url.str.extract(r'(\b\d{4}(?:-\d{4})?\b)')

        df = df.sort_values(by=["league", "year"], ascending=[True, True]).drop(columns=["year"])

        df = df[["leagueId", "season", "url"]]

        return df

    
    def read_matches(self, force_cache: bool = False,
                     truncated: bool = False,
                     var: bool = False,
                     ) -> pd.DataFrame:

        filemask = "seasons/{}_{}.html"
        urlmask = SCORESWAY_API + "/{}/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp&sps=widgets&_clbk={}"

        df_seasons = self.read_seasons()
        all_schedules = []
        for (lkey, skey), season in df_seasons.iterrows():
            season_string = season['season']
            callback_id = self.generate_callback_id(k=40)


            url = urlmask.format('match', skey, callback_id)
            filepath = self.data_dir / filemask.format(lkey, season_string)
            reader = self.get(url, filepath, no_cache=force_cache, var='allMatches', clbk=callback_id)
            season_data = json.load(reader)

            df = pd.json_normalize(season_data['allMatches'])
            df["league"] = lkey
            df["leagueId"] = season["leagueId"]
            df["seasonId"] = skey
            df["season"] = season["season"]
            df["url"] = url



            df = df.rename(columns=lambda col: re.sub(r"^(matchInfo\.|liveData\.)", "", col))

            teams_data = df['contestant'].apply(pd.Series)
            teams_data.columns = ['home', 'away']
            team_data = pd.concat(objs=[pd.json_normalize(teams_data['home']).add_prefix('homeTeam'),
                                        pd.json_normalize(teams_data['away']).add_prefix('awayTeam')], axis=1)


            team_data = team_data.rename(columns={
                'homeTeamid': 'homeTeamId',
                'awayTeamid': 'awayTeamId',
                'homeTeamname': 'homeTeam',
                'awayTeamname': 'awayTeam',
                'awayTeamshortName': 'awayTeamShort',
                'homeTeamshortName': 'homeTeamShort',
                'awayTeamofficialName': 'awayTeamOfficial',
                'homeTeamofficialName': 'homeTeamOfficial',
                'awayTeamcode': 'awayTeamCode',
                'homeTeamcode': 'homeTeamCode',
                'awayTeamcountry.name': 'awayTeamCountryName',
                'homeTeamcountry.name': 'homeTeamCountryName',
            })

            cols = ['homeTeamId', 'homeTeam', 'homeTeamShort', 'homeTeamOfficial', 'homeTeamCode',
                    'homeTeamCountryName',
                    'awayTeamId', 'awayTeam', 'awayTeamShort', 'awayTeamOfficial', 'awayTeamCode',
                    'awayTeamCountryName']

            all_schedules.append(pd.concat(objs=[df, team_data[cols]], axis=1))

        print(f"All matches are loaded. Preprocess data into dataframe.")

        df = pd.concat(all_schedules)

        df['date'] = df['date'].str.replace('Z', '')
        df['matchDescription'] = df['date'] + ' ' + df['description']
        df["league_index"] = df["league"].copy()
        df["seasonId_index"] = df["seasonId"].copy()
        df["match_index"] = df["matchDescription"].copy()

        df = (
            df.set_index(["league_index", "seasonId_index", "match_index"])
            .sort_values(["league", "season", "date", "time"])
            .rename(columns={'id': 'matchId',
                             'url': 'url',
                             'date': 'matchDate',
                             'time': 'matchTime',
                             'week': 'matchRound',
                             'description': 'match',
                             'sport.id': 'sportId',
                             'sport.name': 'sport',
                             'ruleset.id': 'rulesetId',
                             'ruleset.name': 'rulesetName',
                             'competition.competitionFormat': 'leagueFormat',
                             'competition.country.name': 'countryName',
                             'tournamentCalendar.startDate': 'seasonStartDate',
                             'tournamentCalendar.endDate': 'seasonEndDate',
                             'stage.id': 'stageId',
                             'stage.formatId': 'stageFormatId',
                             'stage.startDate': 'stageStartDate',
                             'stage.endDate': 'stageEndDate',
                             'stage.name': 'stageName',
                             'venue.id': 'venueId',
                             'venue.neutral': 'venueNeutral',
                             'venue.longName': 'venue',
                             'venue.shortName': 'venueShortName',
                             'var': 'matchVar',
                             'numberOfPeriods': 'matchPeriods',
                             'periodLength': 'matchPeriodLength',
                             'overtimeLength': 'matchOvertimeLength',
                             'matchDetails.matchStatus': 'matchStatus',
                             'matchDetails.winner': 'matchWinner',
                             'matchDetails.scores.ft.home': 'scoreHomeFullTime',
                             'matchDetails.scores.ft.away': 'scoreAwayFullTime',
                             'matchDetails.scores.total.home': 'scoreHomeTotal',
                             'matchDetails.scores.total.away': 'scoreAwayTotal',
                             'matchDetails.scores.ht.home': 'scoreHomeHalfTime',
                             'matchDetails.scores.ht.away': 'scoreAwayHalfTime',
                             'matchDetailsExtra.attendance': 'matchAttendance',
                             'matchDetails.matchLengthMin': 'matchLengthMin',
                             'matchDetails.matchLengthSec': 'matchLengthSec',
                             'matchDetails.leg': 'matchLeg',
                             'matchDetails.relatedMatchId': 'matchRelatedMatchId',
                             'matchDetails.aggregateWinnerId': 'matchAggregateWinnerId',
                             'matchDetails.scores.aggregate.home': 'scoreHomeAggregate',
                             'matchDetails.scores.aggregate.away': 'scoreAwayAggregate',
                             'series.name': 'stageGroup',
                             'matchDetails.scores.et.home': 'scoreHomeExtraTime',
                             'matchDetails.scores.et.away': 'scoreAwayExtraTime',
                             'matchDetails.scores.pen.home': 'scoreHomePenalty',
                             'matchDetails.scores.pen.away': 'scoreAwayPenalty',
                             })
        )

        # Mapping of referee types to column name prefixes
        def nested(dictionary: list, val: str):
            """Process match officials into a structured dictionary while handling missing fields dynamically."""
            d_ = {}
            if isinstance(dictionary, list) and val == 'official':  
                type_mapping = {
                    "Main": "refMain",
                    "Assistant referee 1": "refAss1",
                    "Assistant referee 2": "refAss2",
                    "Fourth official": "refFourth",
                    "Video Assistant Referee": "refVar",
                    "Assistant VAR Official": "refAssVar"
                }
                for d in dictionary:
                    ref_type = type_mapping.get(d.get('type', ''), d.get('type', '').replace(" ", ""))
                    for k in ['id', 'firstName', 'lastName']:
                        if k in d:
                            norm_k = k[0].upper() + k[1:]
                            d_[f"{ref_type}{norm_k}"] = d[k]

            elif isinstance(dictionary, list) and val == 'period':
                for i, d in enumerate(dictionary):
                    for k in ['start', 'end', 'lengthMin', 'lengthSec']:
                        if k in d:
                            norm_k = k[0].upper() + k[1:]
                            if 'Start' in norm_k or 'End' in norm_k:
                                norm_k += 'Time'
                            d_[f"matchPeriod{i + 1}{norm_k}"] = d[k]

            return d_

        df_period = df['matchDetails.period'].apply(
            lambda x: nested(x, val='period') if isinstance(x, list) else {}).apply(pd.Series)

        df_ref = df['matchDetailsExtra.matchOfficial'].apply(
            lambda x: nested(x, val='official') if isinstance(x, list) else {}).apply(pd.Series)

        df = (pd.concat([df, df_ref, df_period], axis=1).
              drop(columns=['matchDetailsExtra.matchOfficial', 'matchDetails.period']))


        var_cols = [col for col in ["matchVar", "VAR", "refAssVarId"] if col in df.columns]

        df["matchVar"] = False
        if var_cols:
            df["matchVar"] = df[var_cols].notna().any(axis=1)

        if var:
            var_list = []
            for i, row in df.iterrows():
                if isinstance(row['VAR'], list):
                    for r in row['VAR']:
                        r['league'] = row['league']
                        r['leagueId'] = row['leagueId']
                        r['match'] = row['match']
                        r['matchId'] = row['matchId']
                        r['matchDate'] = row['matchDate']
                        r['season'] = row['season']
                        r['seasonId'] = row['seasonId']
                        var_list += [r]

                elif row['matchVar']:
                    r = {'league': row['league'], 'leagueId': row['leagueId'], 'match': row['match'],
                         'matchId': row['matchId'],
                         'matchDate': row['matchDate'], 'season': row['season'],
                         'seasonId': row['seasonId']}
                    var_list += [r]

            df_var = (pd.DataFrame(var_list)
                      .rename(columns={'contestantId': 'teamId',
                                       'periodId': 'matchPeriod',
                                       'timeMinSec': 'matchTimestamp',
                                       'playerName': 'player',
                                       })
                      )
            df_var = df_var.sort_values(by=["league", "season", "matchDate", "match", "matchTimestamp"])
            cols = ['league', 'leagueId', 'season', 'seasonId', 'match', 'matchId', 'matchDate',
                    'matchPeriod', 'matchTimestamp', 'optaEventId', 'optaEventUnderReviewId', 'player', 'playerId',
                    'type', 'decision', 'outcome', ]
            return df_var[cols]

        ref_cols = [
            'refMainId', 'refMainFirstName', 'refMainLastName',
            'refAss1Id', 'refAss1FirstName', 'refAss1LastName',
            'refereeAss2Id', 'refAss2FirstName', 'refAss2LastName',
            'refFourthId', 'refFourthFirstName', 'refFourthLastName',
            'refAssVarId', 'refAssVarFirstName', 'refAssVarLastName',
            'refAssVar2Id', 'refAssVar2FirstName', 'refAssVar2LastName',
        ]

        cols = ['league', 'leagueId', 'leagueFormat',
                'match', 'matchId', 'matchStatus', 'matchDate', 'matchTime', 'matchRound', 'matchWinner',
                'matchVar', 'matchAttendance', 'matchPeriods', 'matchPeriodLength', 'matchLengthMin', 'matchLengthSec',
                'matchPeriod1StartTime', 'matchPeriod1EndTime', 'matchPeriod1LengthMin', 'matchPeriod1LengthSec',
                'matchPeriod2StartTime', 'matchPeriod2EndTime', 'matchPeriod2LengthMin', 'matchPeriod2LengthSec',
                'season', 'seasonId', 'seasonStartDate', 'seasonEndDate',
                'stage', 'stageId', 'stageFormatId', 'stageStartDate', 'stageEndDate', 'stageGroup',
                'venue', 'venueId', 'venueShortName', 'venueNeutral',
                'homeTeam', 'homeTeamId', 'homeTeamShort', 'homeTeamOfficial', 'homeTeamCode', 'homeTeamCountryName',
                'awayTeam', 'awayTeamId', 'awayTeamShort', 'awayTeamOfficial', 'awayTeamCode', 'awayTeamCountryName',
                'scoreHomeFullTime', 'scoreAwayFullTime', 'scoreHomeHalfTime', 'scoreAwayHalfTime',
                'scoreHomeTotal', 'scoreAwayTotal', 'scoreHomeHalfTime', 'scoreAwayHalfTime',
                'matchRelatedMatchId', 'matchLeg', 'matchOvertimeLength',
                'matchPeriod3StartTime', 'matchPeriod3EndTime', 'matchPeriod3LengthMin', 'matchPeriod3LengthSec',
                'matchPeriod4StartTime', 'matchPeriod4EndTime', 'matchPeriod4LengthMin', 'matchPeriod4LengthSec',
                'matchAggregateWinnerId',
                'scoreHomeAggregate', 'scoreAwayAggregate', 'scoreHomeExtraTime', 'scoreAwayExtraTime',
                'scoreHomePenalty', 'scoreAwayPenalty']

        if truncated:
            cols = ['league', 'leagueId', 'season', 'seasonId', 'match', 'matchId',
                    'matchRound', 'matchDate', 'matchStatus', 'homeTeam', 'homeTeamId',
                    'awayTeam', 'awayTeamId', 'scoreHomeFullTime', 'scoreAwayFullTime',
                    'url']
            ref_cols = []

        return df[[col for col in cols + ref_cols + ['url'] if col in df.columns]]

    @staticmethod
    def truncate_games(df: pd.DataFrame) -> pd.DataFrame:
        cols = ['league', 'leagueId', 'season', 'seasonId', 'match', 'matchId',
                'matchRound', 'matchDate', 'matchStatus', 'homeTeam', 'homeTeamId',
                'awayTeam', 'awayTeamId', 'scoreHomeFullTime', 'scoreAwayFullTime',
                'url']
        return df[cols]

    @staticmethod
    def _opta_event_availability(league):
        season_str = LEAGUE_DICT.get(league, {}).get("optaEvents")
        if season_str:
            return season_str
        else:
            return None

    @staticmethod
    def generate_callback_id(k=40):
        # Return k characters, using letters a-f (hexadecimal range)
        return 'W3' + ''.join(random.choices(population='abcdef0123456789', k=k))

    def _opta_event_files_(self):
        event_leagues = list(self._selected_leagues.keys())
        event_files = os.listdir(self.data_dir / 'events')
        latest_dates = {}
        league_events = []

        for file in event_files:
            for league in event_leagues:
                if league in file:
                    league_events.append(file)
                    rematch = re.search(pattern=r'_(\d{4}-\d{2}-\d{2})', string=file)
                    if rematch:
                        date_str = rematch.group(1)
                        if league not in latest_dates or date_str > latest_dates[league]:
                            latest_dates[league] = date_str
        return latest_dates, league_events

    def read_events(self,
                    force_cache: bool = False,
                    dataframe: Optional[pd.DataFrame] = None,
                    ):
        pass
        filemask = "events/{}_{}_{}.html"
        urlmask = SCORESWAY_API + "/{}/ft1tiv1inq7v1sk3y9tv12yh5/{}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk={}"

        # Retrieve games for which a match report is available
        if not isinstance(dataframe, pd.DataFrame):
            dataframe = self.read_matches(force_cache)

        opta_event_availability = dataframe['league'].map(self._opta_event_availability)
        df_complete = dataframe[
            (dataframe['season'] >= opta_event_availability) & (dataframe["matchStatus"] == "Played")]

        df_complete = df_complete.sort_values(['league', 'season', 'matchDate', 'matchTime', 'match'])

        latest_dates, event_files = self._opta_event_files_()

        df_missing = df_complete[
            df_complete.apply(lambda row: row['matchDate'] >= latest_dates.get(row['league'], row['matchDate']),
                              axis=1)]
                              
        iterator = df_missing
        events = []
        N = len(iterator)
        for i, match in iterator.reset_index().iterrows():

            match_name = match["match"].replace('/', '')
            lkey, skey, gkey = match["league"], match["seasonId"], match["matchDate"] + ' ' + match_name
            callback_id = self.generate_callback_id(k=40)
            url = urlmask.format('matchevent', match['matchId'], callback_id)
            filename = filemask.format(lkey, gkey, match['matchId'])
            filepath = self.data_dir / filename
            if filename.split('events/')[-1] in event_files:
                i -= 1
                continue

            print(f"[{i + 1}/{N}] Retrieving match {match_name} at {match['matchDate']} with id={match['matchId']}")
            _ = self.get(url, filepath, no_cache=force_cache, var='allEvents', clbk=callback_id)

            time.sleep(random.uniform(0.01, 0.25))

            if i % 50 == 0:
                time.sleep(random.uniform(2, 8))

        filemask = "events/{}"
        _, event_files = self._opta_event_files_()
        N = len(event_files)
        for i, file in enumerate(sorted(event_files)):

            print(f"[{i + 1}/{N}] Retrieving {file}")

            filepath = self.data_dir / filemask.format(file)
            try:
                reader = self.get(url='placeholder', filepath=filepath, no_cache=force_cache)
            except (TypeError, json.JSONDecodeError):
                continue

            fspl = file.split('_')
            league, match_id = fspl[0].split('.')[0], fspl[-1].split('.')[0]
            rematch = re.search(pattern=r'(\d{4}-\d{2}-\d{2})\s(.*)$', string=fspl[1])
            date, match = rematch.group(1), rematch.group(2)

            if reader:
                event_data = json.load(reader)
                event_df = pd.json_normalize(event_data['allEvents'])
                event_df['league'] = league
                event_df['match'] = match
                event_df['matchId'] = match_id
                event_df['matchDate'] = date
                events.append(event_df)

        events = pd.concat(events)
        metacols = ['league', 'match', 'matchId', 'matchDate']
        cols = [x for x in events.columns if x not in metacols]
        events = events[[col for col in metacols + cols if col in events.columns]]
        return events

    def read_player_stats(self):
        pass

    def read_match_stats(self):
        pass

    def read_lineup(self):
        pass


