import io
import time
import json
import pprint
import random

import requests
import cloudscraper
from bs4 import BeautifulSoup

from abc import ABC, abstractmethod
from pathlib import Path
from collections.abc import Iterable
from typing import Optional, Callable, Union, IO

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


from configdemo import DATA_DIR, LEAGUE_DICT, MAXAGE, TEAMNAME_REPLACEMENTS, logger


class Reader(ABC):

    def __init__(
            self,
            leagues: Optional[Union[str, list[str]]] = None,
            proxy: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            header: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            no_cache: bool = False,
            no_store: bool = False,
            data_dir: Path = DATA_DIR,
    ):
        """Create a new data reader."""
        if isinstance(proxy, str) and proxy.lower() == "tor":
            self.proxy = lambda: {
                "http": "socks5://127.0.0.1:9050",
                "https": "socks5://127.0.0.1:9050",
            }
        elif isinstance(proxy, dict):
            self.proxy = lambda: proxy
        elif isinstance(proxy, list):
            self.proxy = lambda: random.choice(proxy)
        elif callable(proxy):
            self.proxy = proxy
        else:
            self.proxy = dict

        if isinstance(header, dict):
            self.header = lambda: header

        self._selected_leagues = leagues  # type: ignore
        self.no_cache = no_cache
        self.no_store = no_store
        self.data_dir = data_dir
        self.rate_limit = 0
        self.max_delay = 0
        if self.no_store:
            # logger.info("Caching is disabled")
            print("No caching is used.")
        else:
            print(f"Saving cached data to {self.data_dir}")
            # logger.info("Saving cached data to %s", self.data_dir)
            self.data_dir.mkdir(parents=True, exist_ok=True)

    def get(
            self,
            url: str,
            filepath: Optional[Path] = None,
            max_age: Optional[Union[int, timedelta]] = MAXAGE,
            no_cache: bool = False,
            var: Optional[Union[str, Iterable[str]]] = None,
            clbk: Optional[Union[str, Iterable[str]]] = None,
            message: Optional[Union[str, Iterable[str]]] = None,
    ) -> IO[bytes]:

        is_cached = self._is_cached(filepath, max_age)

        if no_cache or self.no_cache or not is_cached:
            print(f"Scraping {url}")
            return self._download_and_save(url, filepath, var, clbk)
        if not message:
            print(f"Retrieving {url} from cache")
        else:
            print(message)
        if filepath is None:
            raise ValueError("No filepath provided for cached data.")
        return filepath.open(mode="rb")

    @staticmethod
    def _is_cached(
            # self,
            filepath: Optional[Path] = None,
            max_age: Optional[Union[int, timedelta]] = None,
    ) -> bool:

        # Validate inputs
        if max_age is not None:
            if isinstance(max_age, int):
                _max_age = timedelta(days=max_age)
            elif isinstance(max_age, timedelta):
                _max_age = max_age
            else:
                raise TypeError("'max_age' must be of type int or datetime.timedelta")
        else:
            _max_age = None

        cache_invalid = False
        # Check if cached file is too old
        if _max_age is not None and filepath is not None and filepath.exists():
            last_modified = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            if (now - last_modified) > _max_age:
                cache_invalid = True

        return not cache_invalid and filepath is not None and filepath.exists()

    @abstractmethod
    def _download_and_save(
            self,
            url: str,
            filepath: Optional[Path] = None,
            var: Optional[Union[str, Iterable[str]]] = None,
            clbk: Optional[Union[str, Iterable[str]]] = None,
    ) -> IO[bytes]:
        """Download data at `url` to `filepath`.

        Parameters
        ----------
        url : str
            URL to download.
        filepath : Path, optional
            Path to save downloaded file. If None, downloaded data is not cached.
        var : str or list of str, optional
            Return a JavaScript variable instead of the page source.

        Returns
        -------
        io.BufferedIOBase
            File-like object of downloaded data.
        """

    @classmethod
    def available_leagues(cls) -> list[str]:
        """Return a list of league IDs available for this source."""
        return sorted(cls._all_leagues().keys())

    @classmethod
    def _all_leagues(cls) -> dict[str, str]:
        """Return a dict mapping all canonical league IDs to source league IDs."""
        if not hasattr(cls, "_all_leagues_dict"):
            cls._all_leagues_dict = {  # type: ignore
                k: v[cls.__name__] for k, v in LEAGUE_DICT.items() if cls.__name__ in v
            }
        return cls._all_leagues_dict  # type: ignore

    @classmethod
    def _translate_league(cls, df: pd.DataFrame, col: str = "league", level: str = "name") -> pd.DataFrame:
        """Map source league ID to canonical ID."""
        flip = {v: k for k, v in cls._all_leagues().items()}

        if level == "name":
            league_to_country = {k: v["countryName"] for k, v in LEAGUE_DICT.items() if "countryName" in v}
            df["valid_league"] = df.apply(
                lambda row: row[col] in flip and league_to_country.get(flip[row[col]], None) == row["country"],
                axis=1,
            )
        elif level == "code":
            league_to_country = {k: v["countryCode"] for k, v in LEAGUE_DICT.items() if "countryCode" in v}
            df["valid_league"] = df.apply(
                lambda row: row[col] in flip and league_to_country.get(flip[row[col]], None) == row["leagueRegion"],
                axis=1,
            )

        else:
            raise ValueError(f"level must be either 'name' or 'code'")

        df.loc[~df["valid_league"], col] = np.nan  # Invalidate incorrect mappings
        df[col] = df[col].replace(flip)  # Apply valid mappings
        df.drop(columns=["valid_league"], inplace=True)  # Remove helper column
        return df

    @property
    def _selected_leagues(self) -> dict[str, str]:
        """Return a dict mapping selected canonical league IDs to source league IDs."""
        return self._leagues_dict

    @_selected_leagues.setter
    def _selected_leagues(self, ids: Optional[Union[str, list[str]]] = None) -> None:
        if ids is None:
            self._leagues_dict = self._all_leagues()
        else:
            if len(ids) == 0:
                raise ValueError("Empty iterable not allowed for 'leagues'")
            if isinstance(ids, str):
                ids = [ids]
            tmp_league_dict = {}
            for i in ids:
                if i not in self._all_leagues():
                    raise ValueError(
                        f"""
                            Invalid league '{i}'. Valid leagues are:
                            {pprint.pformat(self.available_leagues())}
                            """
                    )
                tmp_league_dict[i] = self._all_leagues()[i]
            self._leagues_dict = tmp_league_dict

    @property
    def leagues(self) -> list[str]:
        """Return a list of selected leagues."""
        return list(self._leagues_dict.keys())


class RequestReader(Reader):
    """Base class for readers that use the Python requests module."""

    def __init__(
            self,
            leagues: Optional[Union[str, list[str]]] = None,
            proxy: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            header: Optional[
                Union[str, dict[str, str], list[dict[str, str]], Callable[[], dict[str, str]]]
            ] = None,
            no_cache: bool = False,
            no_store: bool = False,
            data_dir: Path = DATA_DIR,
    ):
        """Initialize the reader."""
        super().__init__(
            no_cache=no_cache,
            no_store=no_store,
            leagues=leagues,
            proxy=proxy,
            header=header,
            data_dir=data_dir,
        )

        self._session = self._init_session()

    def _init_session(self) -> requests.Session:
        session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "linux", "mobile": False}
        )
        session.proxies.update(self.proxy())
        # if self.header is not None:
        session.headers.update(self.header())
        return session

    def _download_and_save(
            self,
            url: str,
            filepath: Optional[Path] = None,
            var: Optional[Union[str, Iterable[str]]] = None,
            clbk: Optional[Union[str, Iterable[str]]] = None,
    ) -> Optional[IO[bytes]]:
        """Download file at url to filepath. Overwrites if filepath exists."""
        for i in range(5):
            try:
                response = self._session.get(url, stream=True)
                time.sleep(self.rate_limit + random.random() * self.max_delay)
                response.raise_for_status()
                if var is not None:
                    soup = BeautifulSoup(response.text, features="lxml")
                    data = {}

                    for script in soup.find_all(name="script", type="application/json"):
                        try:
                            json_data = json.loads(script.string)
                            if var not in json_data.keys():
                                continue
                            data.update(json_data)
                        except (TypeError, json.JSONDecodeError):
                            pass  # Skip if parsing fails

                    if var not in data.keys():
                        # links = [x['value'] for x in soup.find("div", attrs={'id': 'seasonlist'}).find_all('option')]
                        season_div = soup.find("div", attrs={'id': 'seasonlist'})

                        if season_div:
                            links = [x.get('value', '') for x in season_div.find_all('option')]
                        else:
                            links = []  # Fallback to empty list if div is not found

                        if links:
                            data[var] = links
                        else:
                            soup_text = soup.get_text()
                            before, sep, after = soup_text.partition(f"{clbk}(")
                            soup_after = after.rstrip(')')
                            try:
                                soup_json = json.loads(soup_after)
                            except (TypeError, json.JSONDecodeError):
                                print(f"Could not parse html as json format for {url}.\nProceed to next url.")
                                return None
                                # continue
                            if var == 'allMatches':
                                json_data = soup_json['match']
                                data[var] = json_data
                            if var == 'allEvents':
                                json_data = soup_json['liveData']['event']
                                data[var] = json_data

                    payload = json.dumps(data).encode("utf-8")
                else:
                    payload = response.content

                if not self.no_store and filepath is not None:
                    with filepath.open(mode="wb") as fh:
                        fh.write(payload)
                return io.BytesIO(payload)
            except Exception:
                logger.exception(
                    "Error while scraping %s. Retrying... (attempt %d of 5).",
                    url,
                    i + 1,
                )
                self._session = self._init_session()
                continue

        raise ConnectionError(f"Could not download {url}.")

    @property
    def session(self):
        return self._session


def make_game_id(row: pd.Series) -> str:
    """Return a game id based on date, home and away team."""
    if pd.isnull(row["matchDate"]):
        game_id = "{}-{}".format(
            row["homeTeam"],
            row["awayTeam"],
        )
    else:
        game_id = "{} {}-{}".format(
            row["matchDate"].strftime("%Y-%m-%d"),
            row["homeTeam"],
            row["awayTeam"],
        )
    return game_id


