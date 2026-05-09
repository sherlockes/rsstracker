"""
main.py - FastAPI application entry point.

Routes
------
GET  /                   -> Dashboard (list of feeds)
GET  /feeds/new          -> Add feed form
POST /feeds/new          -> Create feed
GET  /feeds/{id}/edit    -> Edit feed form
POST /feeds/{id}/edit    -> Update feed
POST /feeds/{id}/delete  -> Delete feed
POST /feeds/{id}/run     -> Trigger immediate run
GET  /config             -> Global config form
POST /config             -> Save global config
POST /api/test-regex     -> JSON endpoint for regex tester
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from models import Feed, GlobalConfig, SentItem, SessionLocal, init_db
from scheduler import (
    reschedule_all_feeds,
    reschedule_feed,
    run_feed_now,
    scheduler,
    start_scheduler,
    stop_scheduler,
    test_regex,
)

# --------------------------------------------------------------------------- #
# Logging                                                                      #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("rsstracker.main")

# --------------------------------------------------------------------------- #
# App lifecycle                                                                #
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("data", exist_ok=True)
    init_db()
    start_scheduler()
    logger.info("RSS Tracker started.")
    
    # Check if run_on_startup is enabled and trigger a run
    db = SessionLocal()
    try:
        config_row = db.get(GlobalConfig, "run_on_startup")
        if config_row and config_row.value == "true":
            logger.info("Triggering RSS poll on startup because run_on_startup is enabled.")
            import threading
            from scheduler import run_all_feeds_job
            threading.Thread(target=run_all_feeds_job, daemon=True).start()
    except Exception as exc:
        logger.error("Error triggering startup poll: %s", exc)
    finally:
        db.close()
        
    yield
    stop_scheduler()
    logger.info("RSS Tracker stopped.")


app = FastAPI(title="RSS Tracker → Telegram", lifespan=lifespan)

# Middleware para capturar cualquier excepción unhandled y mostrar el error real en pantalla
from fastapi.responses import PlainTextResponse
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("Error interno no controlado:")
        return PlainTextResponse(f"Error Interno del Servidor: {exc}\n\nPor favor, revisa los logs de Docker para ver el traceback completo.", status_code=500)

# Static files (CSS / JS assets)
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

# --------------------------------------------------------------------------- #
# DB dependency                                                                #
# --------------------------------------------------------------------------- #


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


DB = Annotated[Session, Depends(get_db)]

# --------------------------------------------------------------------------- #
# Template helpers                                                             #
# --------------------------------------------------------------------------- #


def _global_config(db: Session) -> dict[str, str]:
    rows = db.query(GlobalConfig).all()
    return {row.key: (row.value or "") for row in rows}


def _fmt_dt(dt: Optional[datetime]) -> str:
    try:
        if dt is None:
            return "—"
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception as exc:
        logger.error("Error formatting datetime '%s': %s", dt, exc)
        return str(dt)


templates.env.filters["fmt_dt"] = _fmt_dt  # type: ignore[attr-defined]

# --------------------------------------------------------------------------- #
# Dashboard                                                                    #
# --------------------------------------------------------------------------- #


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db: DB):
    feeds = db.query(Feed).order_by(Feed.name).all()
    global_job = scheduler.get_job("poll_all_feeds")
    next_run = "—"
    if global_job and global_job.next_run_time:
        try:
            next_run = global_job.next_run_time.astimezone().strftime("%H:%M:%S")
        except Exception as exc:
            logger.error("Error formatting next run time: %s", exc)
            next_run = str(global_job.next_run_time)

    # Calcular estadísticas en Python para evitar fallos de renderizado en Jinja2
    from datetime import datetime as dt, timedelta
    now_utc = dt.utcnow()
    last_day = now_utc - timedelta(days=1)
    last_week = now_utc - timedelta(days=7)
    last_month = now_utc - timedelta(days=30)

    sent_day = db.query(SentItem).filter(SentItem.sent_at >= last_day).count()
    sent_week = db.query(SentItem).filter(SentItem.sent_at >= last_week).count()
    sent_month = db.query(SentItem).filter(SentItem.sent_at >= last_month).count()

    # Mini weekly chart stats
    chart_data = []
    weekdays_es = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
    for i in range(6, -1, -1):
        day_date = (dt.now() - timedelta(days=i)).date()
        day_start = dt.combine(day_date, dt.min.time())
        day_end = dt.combine(day_date, dt.max.time())
        count = db.query(SentItem).filter(SentItem.sent_at >= day_start, SentItem.sent_at <= day_end).count()
        weekday_idx = int(day_date.strftime("%w"))
        day_name = weekdays_es[weekday_idx]
        chart_data.append({"day": day_name, "count": count})

    stats = {
        "total": len(feeds),
        "active": sum(1 for f in feeds if f.enabled),
        "inactive": sum(1 for f in feeds if not f.enabled),
        "error": sum(1 for f in feeds if (f.consecutive_errors or 0) > 0),
        "sent_day": sent_day,
        "sent_week": sent_week,
        "sent_month": sent_month,
        "chart_data": chart_data,
    }

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "feeds": feeds,
            "next_run": next_run,
            "global_cfg": _global_config(db),
            "stats": stats,
        },
    )



# --------------------------------------------------------------------------- #
# Add feed                                                                     #
# --------------------------------------------------------------------------- #


@app.get("/feeds/new", response_class=HTMLResponse)
async def feed_new_form(request: Request, db: DB):
    return templates.TemplateResponse(
        request=request,
        name="edit.html",
        context={
            "feed": None,
            "action": "/feeds/new",
            "global_cfg": _global_config(db),
        },
    )


@app.post("/feeds/new")
async def feed_new(
    db: DB,
    name: str = Form(...),
    url: str = Form(...),
    title_regex_clean: str = Form(""),
    size_regex_extract: str = Form(""),
    link_transform_regex: str = Form(""),
    link_transform_replace: str = Form(""),
    enabled: bool = Form(False),
):
    feed = Feed(
        name=name,
        url=url,
        interval=60,  # default placeholder in DB, scheduler uses global check_interval
        title_regex_clean=title_regex_clean or None,
        size_regex_extract=size_regex_extract or None,
        link_transform_regex=link_transform_regex or None,
        link_transform_replace=link_transform_replace or None,
        telegram_token=None,
        chat_id=None,
        enabled=enabled,
    )
    db.add(feed)
    db.commit()
    db.refresh(feed)
    reschedule_feed(feed.id)
    return RedirectResponse("/", status_code=303)


# --------------------------------------------------------------------------- #
# Edit feed                                                                    #
# --------------------------------------------------------------------------- #


@app.get("/feeds/{feed_id}/edit", response_class=HTMLResponse)
async def feed_edit_form(feed_id: int, request: Request, db: DB):
    feed = db.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    return templates.TemplateResponse(
        request=request,
        name="edit.html",
        context={
            "feed": feed,
            "action": f"/feeds/{feed_id}/edit",
            "global_cfg": _global_config(db),
        },
    )


@app.post("/feeds/{feed_id}/edit")
async def feed_edit(
    feed_id: int,
    db: DB,
    name: str = Form(...),
    url: str = Form(...),
    title_regex_clean: str = Form(""),
    size_regex_extract: str = Form(""),
    link_transform_regex: str = Form(""),
    link_transform_replace: str = Form(""),
    enabled: bool = Form(False),
):
    feed = db.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    feed.name = name
    feed.url = url
    feed.title_regex_clean = title_regex_clean or None
    feed.size_regex_extract = size_regex_extract or None
    feed.link_transform_regex = link_transform_regex or None
    feed.link_transform_replace = link_transform_replace or None
    feed.enabled = enabled
    db.commit()
    reschedule_feed(feed_id)
    return RedirectResponse("/", status_code=303)


# --------------------------------------------------------------------------- #
# Delete feed                                                                  #
# --------------------------------------------------------------------------- #


@app.post("/feeds/{feed_id}/delete")
async def feed_delete(feed_id: int, db: DB):
    feed = db.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    jid = f"feed_{feed_id}"
    if scheduler.get_job(jid):
        scheduler.remove_job(jid)
    db.delete(feed)
    db.commit()
    return RedirectResponse("/", status_code=303)


# --------------------------------------------------------------------------- #
# Run now                                                                      #
# --------------------------------------------------------------------------- #


@app.post("/feeds/{feed_id}/run")
async def feed_run(feed_id: int, db: DB):
    feed = db.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    from fastapi.concurrency import run_in_threadpool
    await run_in_threadpool(run_feed_now, feed_id)
    return {"success": True}


# --------------------------------------------------------------------------- #
# Global config                                                                #
# --------------------------------------------------------------------------- #


@app.get("/config", response_class=HTMLResponse)
async def config_form(request: Request, db: DB):
    return templates.TemplateResponse(
        request=request,
        name="config.html",
        context={"global_cfg": _global_config(db)},
    )


@app.post("/config")
async def config_save(
    db: DB,
    telegram_token: str = Form(""),
    chat_id: str = Form(""),
    check_interval: str = Form("60"),
    max_items_per_message: str = Form("100"),
    silent_mode_start: str = Form(""),
    silent_mode_end: str = Form(""),
    run_on_startup: bool = Form(False),
):
    run_on_startup_val = "true" if run_on_startup else "false"
    for key, value in [
        ("telegram_token", telegram_token),
        ("chat_id", chat_id),
        ("check_interval", check_interval),
        ("max_items_per_message", max_items_per_message),
        ("silent_mode_start", silent_mode_start),
        ("silent_mode_end", silent_mode_end),
        ("run_on_startup", run_on_startup_val),
    ]:
        row = db.get(GlobalConfig, key)
        if row:
            row.value = value
        else:
            db.add(GlobalConfig(key=key, value=value))
    db.commit()
    reschedule_all_feeds()
    return RedirectResponse("/config", status_code=303)


from fastapi.responses import FileResponse
import shutil

@app.get("/config/backup")
async def config_backup():
    db_path = "data/rsstracker.db"
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database file not found")
    return FileResponse(
        path=db_path,
        filename="rsstracker_backup.db",
        media_type="application/x-sqlite3",
    )


@app.post("/config/restore")
async def config_restore(db_file: UploadFile = File(...)):
    db_path = "data/rsstracker.db"
    from models import engine
    engine.dispose()
    
    try:
        with open(db_path, "wb") as buffer:
            shutil.copyfileobj(db_file.file, buffer)
        logger.info("Database successfully restored from uploaded backup.")
    except Exception as exc:
        logger.error("Failed to restore database backup: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to restore backup: {exc}")
        
    return RedirectResponse("/config", status_code=303)



# --------------------------------------------------------------------------- #
# Regex tester API                                                             #
# --------------------------------------------------------------------------- #


@app.post("/api/test-regex")
async def api_test_regex(request: Request):
    body = await request.json()
    pattern = body.get("pattern", "")
    replacement = body.get("replacement")
    text = body.get("text", "")
    mode = body.get("mode", "clean")  # clean | extract | transform

    if not pattern:
        return JSONResponse({"result": "", "error": "Pattern is empty"})

    result = test_regex(pattern, replacement, text, mode)
    return JSONResponse(result)


@app.get("/api/fetch-feed-items")
async def api_fetch_feed_items(url: str):
    import feedparser
    import httpx

    if not url:
        return JSONResponse({"items": [], "error": "La URL no puede estar vacía"})

    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
        
        parsed = feedparser.parse(resp.text)
        if parsed.bozo and not parsed.entries:
            return JSONResponse({"items": [], "error": f"Error al parsear el feed: {parsed.bozo_exception}"})

        items = []
        for entry in parsed.entries[:15]:  # Devolver hasta 15 elementos para elegir
            content = ""
            for field in ("content", "summary", "description"):
                val = entry.get(field)
                if isinstance(val, list) and val:
                    content = val[0].get("value", "")
                elif isinstance(val, str):
                    content = val
                if content:
                    break
            
            items.append({
                "title": entry.get("title", "Sin título"),
                "content": content or "Sin descripción",
                "link": entry.get("link", "")
            })

        return JSONResponse({"items": items, "error": None})
    except Exception as exc:
        return JSONResponse({"items": [], "error": str(exc)})

