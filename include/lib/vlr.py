"""vlr.gg scraping + parsing. Parsing is pure (HTML string in, dicts out) so it is testable from fixtures.

Fetching lives in `fetch`/`list_event_match_ids`; everything else takes HTML. Bronze stores the raw HTML,
so parser fixes never require re-scraping.
"""
from __future__ import annotations

import re
import time

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.vlr.gg"
USER_AGENT = "Mozilla/5.0 (compatible; valorant-semantic-scout/0.1; personal data-engineering project)"
REQUEST_DELAY_S = 1.0

# vlr.gg win-type icon (/img/vlr/game/round/<icon>.webp) -> win type
WIN_TYPES = {"elim", "boom", "defuse", "time"}


def fetch(path: str, session: requests.Session | None = None) -> str:
    http = session or requests
    response = http.get(f"{BASE_URL}{path}", headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    time.sleep(REQUEST_DELAY_S)  # be polite: one request per second
    return response.text


def event_matches_path(event_id: int) -> str:
    return f"/event/matches/{event_id}/?series_id=all"


def parse_event_match_ids(html: str) -> list[str]:
    """Match ids listed on an event's matches page, in page order, de-duplicated."""
    soup = BeautifulSoup(html, "html.parser")
    ids: list[str] = []
    for a in soup.select("a.match-item[href]"):
        m = re.match(r"^/(\d+)/", a["href"])
        if m and m.group(1) not in ids:
            ids.append(m.group(1))
    return ids


def _text(node) -> str | None:
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)) if node else None


def _duration_seconds(text: str | None) -> int | None:
    if not text or not re.fullmatch(r"(\d+:)?\d+:\d{2}", text.strip()):
        return None
    seconds = 0
    for part in text.strip().split(":"):
        seconds = seconds * 60 + int(part)
    return seconds


def _half(round_no: int) -> str:
    return "first" if round_no <= 12 else "second" if round_no <= 24 else "overtime"


def parse_match(html: str, match_id: str) -> dict:
    """Parse a match page into {"match": {...}, "maps": [...], "rounds": [...]}.

    Maps with no round data (not played / still live) are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")

    series = _text(soup.select_one(".match-header-event-series"))
    event = _text(soup.select_one(".match-header-event div[style*='font-weight']"))
    ts = soup.select_one(".moment-tz-convert[data-utc-ts]")
    match = {
        "match_id": match_id,
        "event_name": event,
        "stage": series,
        "played_at_raw": ts["data-utc-ts"] if ts else None,
    }

    maps, rounds = [], []
    for order, game in enumerate(soup.select("div.vm-stats-game[data-game-id]"), start=0):
        game_id = game["data-game-id"]
        if not game_id.isdigit() or not game.select_one(".vlr-rounds"):
            continue  # the "All Maps" aggregate block, or a map without round data

        names = [_text(t) for t in game.select(".vm-stats-game-header .team-name")]
        scores = [_text(s) for s in game.select(".vm-stats-game-header .score")]
        map_name_node = game.select_one(".vm-stats-game-header .map-name span")
        map_name = _text(map_name_node.find(string=True, recursive=False)) if map_name_node else None
        duration = _duration_seconds(_text(game.select_one(".map-duration")))

        tag_nodes = game.select(".vlr-rounds-row-col.mod-teams .vlr-rounds-tag")
        tags = [t.get("title") or _text(t) for t in tag_nodes]

        # agents per team tag, from the per-player overview rows
        agents_by_tag: dict[str, list[str]] = {t: [] for t in tags}
        for row in game.select(".ovw-row"):
            tag = _text(row.select_one(".ovw-player-tag"))
            agent_img = row.select_one(".ovw-agents img[title]")
            if tag in agents_by_tag and agent_img:
                agents_by_tag[tag].append(agent_img["title"])

        game_rounds = []
        for col in game.select(".vlr-rounds-row-col[title]"):
            num = col.select_one(".rnd-num")
            squares = col.select(".rnd-sq")
            if not num or len(squares) != 2:
                continue
            round_no = int(_text(num))
            winner_idx = next((i for i, sq in enumerate(squares) if "mod-win" in sq.get("class", [])), None)
            if winner_idx is None:
                continue  # round not played
            winner = squares[winner_idx]
            winner_side = "ct" if "mod-ct" in winner["class"] else "t"
            other_side = "t" if winner_side == "ct" else "ct"
            icon = winner.select_one("img[src]")
            win_type = re.search(r"/round/(\w+)\.webp", icon["src"]).group(1) if icon else None
            if win_type not in WIN_TYPES:
                raise ValueError(f"match {match_id} game {game_id} round {round_no}: unknown win type {win_type!r}")
            team_sides = [None, None]
            team_sides[winner_idx] = winner_side
            team_sides[1 - winner_idx] = other_side
            score1, score2 = (int(x) for x in col["title"].split("-"))
            game_rounds.append(
                {
                    "round_id": f"{game_id}-{round_no:02d}",
                    "match_id": match_id,
                    "game_id": game_id,
                    "map_name": map_name,
                    "round_no": round_no,
                    "half": _half(round_no),
                    "team1_name": names[0],
                    "team2_name": names[1],
                    "team1_side": team_sides[0],
                    "team2_side": team_sides[1],
                    "winner_name": names[winner_idx],
                    "winning_side": winner_side,
                    "win_type": win_type,
                    "team1_score_after": score1,
                    "team2_score_after": score2,
                }
            )
        if not game_rounds:
            continue

        rounds.extend(game_rounds)
        maps.append(
            {
                "game_id": game_id,
                "match_id": match_id,
                "map_order": len(maps) + 1,
                "map_name": map_name,
                "team1_name": names[0],
                "team2_name": names[1],
                "team1_score": int(scores[0]),
                "team2_score": int(scores[1]),
                "duration_s": duration,
                "team1_agents": sorted(agents_by_tag.get(tags[0], [])) if tags else [],
                "team2_agents": sorted(agents_by_tag.get(tags[1], [])) if len(tags) > 1 else [],
                "n_rounds": len(game_rounds),
            }
        )
    return {"match": match, "maps": maps, "rounds": rounds}
