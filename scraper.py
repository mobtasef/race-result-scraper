from __future__ import annotations

import asyncio
import re
import threading

import httpx
from bs4 import BeautifulSoup

from parser import parse_result_html

QUERY_URL = "https://time.feibot.com/live-wire/scores/query"
TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)


def _extract_race_id(race_url: str) -> str:
    match = re.search(r"/race-page/(\d+)/", race_url)
    if not match:
        raise ValueError(f"Could not extract race_id from URL: {race_url}")
    return match.group(1)


async def _fetch_session_and_csrf(race_url: str, client: httpx.AsyncClient) -> tuple[str, str, str]:
    resp = await client.get(race_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        token = meta["content"]
    else:
        inp = soup.find("input", {"name": "_token"})
        if not inp:
            raise ValueError("Could not find CSRF token on race page")
        token = inp["value"]

    race_id = _extract_race_id(race_url)

    h5 = soup.find("h5")
    race_name = h5.get_text(strip=True) if h5 else f"Race {race_id}"

    return token, race_id, race_name


async def _query_bib(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    race_id: str,
    bib: int,
    csrf_token: str,
    jobs: dict,
    job_id: str,
    lock: threading.Lock,
    retry: int = 0,
) -> None:
    async with sem:
        if jobs[job_id].get("cancelled"):
            return
        try:
            resp = await client.post(
                QUERY_URL,
                data={"_token": csrf_token, "bib": str(bib), "race_id": race_id},
            )
            if resp.status_code in (429, 503):
                if retry < 3:
                    await asyncio.sleep(2 ** retry)
                    return await _query_bib(
                        client, sem, race_id, bib, csrf_token, jobs, job_id, lock, retry + 1
                    )
            elif resp.status_code == 200:
                result = parse_result_html(resp.text, bib)
                if result:
                    with lock:
                        jobs[job_id]["results"].append(result)
        except (httpx.TimeoutException, httpx.ConnectError):
            if retry < 1:
                await asyncio.sleep(0.5)
                return await _query_bib(
                    client, sem, race_id, bib, csrf_token, jobs, job_id, lock, retry + 1
                )
        except Exception:
            pass
        finally:
            with lock:
                jobs[job_id]["progress"] += 1


async def scrape_race(
    race_url: str,
    min_bib: int = 1,
    max_bib: int = 9999,
    concurrency: int = 50,
    job_id: str | None = None,
    jobs: dict | None = None,
) -> list[dict]:
    lock = threading.Lock()
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; RaceResultScraper/1.0)"},
    ) as client:
        csrf_token, race_id, race_name = await _fetch_session_and_csrf(race_url, client)

        if job_id and jobs:
            with lock:
                jobs[job_id]["race_id"] = race_id
                jobs[job_id]["race_name"] = race_name

        sem = asyncio.Semaphore(concurrency)
        tasks = [
            _query_bib(client, sem, race_id, bib, csrf_token, jobs, job_id, lock)
            for bib in range(min_bib, max_bib + 1)
        ]
        await asyncio.gather(*tasks)

    results = jobs[job_id]["results"] if job_id and jobs else []

    def rank_key(r):
        val = r.get("overall_gun_rank", "")
        try:
            return int(val)
        except (ValueError, TypeError):
            return 99999

    results.sort(key=rank_key)
    return results
