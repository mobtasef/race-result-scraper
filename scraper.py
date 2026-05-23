from __future__ import annotations

import asyncio
import re
import threading

import httpx
from bs4 import BeautifulSoup

from parser import parse_result_html

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass  # Windows or uvloop not installed — default loop is fine

QUERY_URL = "https://time.feibot.com/live-wire/scores/query"
TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
GAP_THRESHOLD = 2000


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
    if bib > jobs[job_id].get("stop_after", 999999):
        with lock:
            jobs[job_id]["progress"] += 1
        return

    async with sem:
        if jobs[job_id].get("cancelled") or bib > jobs[job_id].get("stop_after", 999999):
            with lock:
                jobs[job_id]["progress"] += 1
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
                        prev_max = jobs[job_id].get("max_found_bib", 0)
                        if bib > prev_max:
                            jobs[job_id]["max_found_bib"] = bib
                            jobs[job_id]["stop_after"] = bib + GAP_THRESHOLD
                            jobs[job_id]["total"] = min(
                                jobs[job_id]["total"], bib + GAP_THRESHOLD
                            )
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


async def _session_worker(
    session_id: int,
    bibs: list[int],
    race_url: str,
    jobs: dict,
    job_id: str,
    lock: threading.Lock,
    concurrency: int,
) -> None:
    limits = httpx.Limits(
        max_connections=concurrency + 5,
        max_keepalive_connections=concurrency + 5,
    )
    async with httpx.AsyncClient(
        http2=False,
        timeout=TIMEOUT,
        follow_redirects=True,
        limits=limits,
        headers={"User-Agent": f"Mozilla/5.0 (RaceResultScraper/S{session_id})"},
    ) as client:
        csrf_token, race_id, race_name = await _fetch_session_and_csrf(race_url, client)

        if session_id == 0 and job_id and jobs:
            with lock:
                jobs[job_id]["race_id"] = race_id
                jobs[job_id]["race_name"] = race_name

        sem = asyncio.Semaphore(concurrency)
        await asyncio.gather(*[
            _query_bib(client, sem, race_id, bib, csrf_token, jobs, job_id, lock)
            for bib in bibs
        ])


def _run_session(
    session_id: int,
    bibs: list[int],
    race_url: str,
    jobs: dict,
    job_id: str,
    lock: threading.Lock,
    concurrency: int,
) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _session_worker(session_id, bibs, race_url, jobs, job_id, lock, concurrency)
        )
    finally:
        loop.close()


def scrape_race(
    race_url: str,
    min_bib: int = 1,
    max_bib: int = 99999,
    concurrency: int = 50,
    sessions: int = 4,
    job_id: str | None = None,
    jobs: dict | None = None,
    bib_ranges=None,
) -> list[dict]:
    lock = threading.Lock()
    if bib_ranges:
        seen = set()
        all_bibs = []
        for lo, hi in bib_ranges:
            for b in range(lo, hi + 1):
                if b not in seen:
                    seen.add(b)
                    all_bibs.append(b)
    else:
        all_bibs = list(range(min_bib, max_bib + 1))

    # Interleave chunks: session 0 gets bibs 1,5,9…; session 1 gets 2,6,10…
    chunks = [all_bibs[i::sessions] for i in range(sessions)]

    threads = [
        threading.Thread(
            target=_run_session,
            args=(i, chunks[i], race_url, jobs, job_id, lock, concurrency),
            daemon=True,
        )
        for i in range(sessions)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    results = jobs[job_id]["results"] if job_id and jobs else []

    def rank_key(r):
        val = r.get("overall_gun_rank", "")
        try:
            return int(val)
        except (ValueError, TypeError):
            return 99999

    results.sort(key=rank_key)
    return results
