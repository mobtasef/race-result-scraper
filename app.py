import asyncio
import csv
import io
import json
import re
import threading
import time
from uuid import uuid4

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

import cache as race_cache
from scraper import scrape_race

app = Flask(__name__)

jobs: dict = {}
jobs_lock = threading.Lock()

FEIBOT_URL_RE = re.compile(r"https?://time\.feibot\.com/live-wire/race-page/\d+/\S+")


def _run_scraper_thread(job_id: str, race_url: str, min_bib: int, max_bib: int, concurrency: int) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            scrape_race(
                race_url,
                min_bib=min_bib,
                max_bib=max_bib,
                concurrency=concurrency,
                job_id=job_id,
                jobs=jobs,
            )
        )
        with jobs_lock:
            jobs[job_id]["status"] = "done"

        # Persist to cache
        job = jobs[job_id]
        results = job["results"]
        categories = sorted({r.get("event", "") for r in results if r.get("event")})
        race_cache.save({
            "race_id":   job.get("race_id", ""),
            "race_name": job.get("race_name", ""),
            "url":       race_url,
            "scraped_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
            "count":     len(results),
            "categories": categories,
            "results":   results,
        })
    except Exception as e:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)
    finally:
        loop.close()


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/scrape")
def start_scrape():
    body = request.get_json(silent=True) or {}
    race_url = (body.get("url") or "").strip()
    min_bib = int(body.get("min_bib") or 1)
    max_bib = int(body.get("max_bib") or 9999)
    concurrency = int(body.get("concurrency") or 50)

    if not FEIBOT_URL_RE.match(race_url):
        return jsonify({"error": "Invalid feibot race URL"}), 400

    min_bib = max(1, min(min_bib, 99999))
    max_bib = max(min_bib, min(max_bib, 99999))
    concurrency = max(1, min(concurrency, 200))

    job_id = str(uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "results": [],
            "progress": 0,
            "total": max_bib - min_bib + 1,
            "race_id": "",
            "cancelled": False,
        }

    t = threading.Thread(
        target=_run_scraper_thread,
        args=(job_id, race_url, min_bib, max_bib, concurrency),
        daemon=True,
    )
    t.start()
    return jsonify({"job_id": job_id})


@app.post("/cancel/<job_id>")
def cancel_job(job_id: str):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["cancelled"] = True
    return jsonify({"ok": True})


@app.get("/progress/<job_id>")
def progress_stream(job_id: str):
    def generate():
        last_count = 0
        while True:
            job = jobs.get(job_id)
            if not job:
                yield f"data: {json.dumps({'error': 'job not found'})}\n\n"
                return

            current_results = job["results"]
            new_results = current_results[last_count:]
            last_count = len(current_results)

            payload = {
                "progress": job["progress"],
                "total": job["total"],
                "found": len(current_results),
                "new_results": new_results,
                "done": job["status"] in ("done", "error"),
                "error": job.get("error"),
            }
            yield f"data: {json.dumps(payload)}\n\n"

            if job["status"] in ("done", "error"):
                return

            time.sleep(0.3)

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/export/<job_id>.csv")
def export_csv(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return "Job not found", 404

    results = job["results"]
    if not results:
        return "No results to export", 404

    fields = [
        "bib", "name", "gender", "event", "status",
        "gun_time", "net_time",
        "overall_gun_rank", "overall_chip_rank",
        "gender_gun_rank", "gender_chip_rank",
        "age_group_gun_rank", "age_group_chip_rank",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

    race_id = job.get("race_id") or "unknown"
    filename = f"race_{race_id}_results.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/cache")
def list_cache():
    return jsonify(race_cache.list_events())


@app.get("/cache/<race_id>")
def load_cache(race_id: str):
    entry = race_cache.get(race_id)
    if not entry:
        return jsonify({"error": "Not found"}), 404
    return jsonify(entry)


@app.get("/export/cache/<race_id>.csv")
def export_cache_csv(race_id: str):
    entry = race_cache.get(race_id)
    if not entry:
        return "Not found", 404
    fields = [
        "bib", "name", "gender", "event", "status",
        "gun_time", "net_time",
        "overall_gun_rank", "overall_chip_rank",
        "gender_gun_rank", "gender_chip_rank",
        "age_group_gun_rank", "age_group_chip_rank",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(entry["results"])
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=race_{race_id}_results.csv"},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)
