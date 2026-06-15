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

from models import Feed, GlobalConfig, SentItem, SessionLocal, init_db, SessionLocalHistorico, Serie, Enlace
from scheduler import (
    reschedule_all_feeds,
    reschedule_feed,
    run_all_feeds_job,
    run_feed_now,
    scheduler,
    start_scheduler,
    stop_scheduler,
    test_regex,
    parse_dirty_title_on_the_fly,
    sync_historical_db_job,
)
from translations import get_lang

# --------------------------------------------------------------------------- #
# Logging                                                                      #
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Logging                                                                      #
# --------------------------------------------------------------------------- #

import logging.handlers

# Ensure data directory exists for logs
os.makedirs("data", exist_ok=True)
log_file = "data/app.log"

log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

file_handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=1024 * 1024 * 5, backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Clear existing handlers to avoid duplicate logging
if root_logger.handlers:
    root_logger.handlers.clear()
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

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

        # Always trigger database alignment (sync/restore) on startup in background
        import threading
        from scheduler import sync_historical_db_job
        logger.info("Triggering database alignment (archive sync/restore) on startup.")
        threading.Thread(target=sync_historical_db_job, kwargs={"force": True}, daemon=True).start()
    except Exception as exc:
        logger.error("Error during startup triggers: %s", exc)
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


def _get_active_lang_dict(db: Session):
    lang_row = db.get(GlobalConfig, "language")
    lang_code = lang_row.value if lang_row and lang_row.value else "en"
    return get_lang(lang_code)


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
async def dashboard(request: Request, db: DB, q: Optional[str] = None):
    feeds = db.query(Feed).order_by(Feed.name).all()
    global_job = scheduler.get_job("poll_all_feeds")
    next_run = "—"
    if global_job and global_job.next_run_time:
        try:
            next_run = global_job.next_run_time.astimezone().strftime("%H:%M")
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
    total_items = db.query(SentItem).count()

    # Mini weekly chart stats
    chart_data = []
    
    lang_row = db.get(GlobalConfig, "language")
    active_code = lang_row.value if lang_row and lang_row.value else "en"
    
    if active_code == "es":
        weekdays = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
    else:
        weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    for i in range(6, -1, -1):
        day_date = (dt.now() - timedelta(days=i)).date()
        day_start = dt.combine(day_date, dt.min.time())
        day_end = dt.combine(day_date, dt.max.time())
        count = db.query(SentItem).filter(SentItem.sent_at >= day_start, SentItem.sent_at <= day_end).count()
        weekday_idx = int(day_date.strftime("%w"))
        day_name = weekdays[weekday_idx]
        chart_data.append({"day": day_name, "count": count})

    # Count of archived items
    db_hist = SessionLocalHistorico()
    try:
        archive_count = db_hist.query(Enlace).count()
    except Exception as e:
        logger.error("Error query archive count for dashboard: %s", e)
        archive_count = 0
    finally:
        db_hist.close()

    def format_k(val: int) -> str:
        if val == 0:
            return "0"
        divided = val / 1000
        if val % 1000 == 0:
            return f"{int(divided)}k"
        else:
            return f"{divided:.1f}k"

    archive_value = f"{format_k(total_items)} / {format_k(archive_count)}"

    stats = {
        "total": len(feeds),
        "active": sum(1 for f in feeds if f.enabled),
        "inactive": sum(1 for f in feeds if not f.enabled),
        "error": sum(1 for f in feeds if (f.consecutive_errors or 0) > 0),
        "total_items": total_items,
        "archive_count": archive_count,
        "archive_value": archive_value,
        "sent_day": sent_day,
        "sent_week": sent_week,
        "sent_month": sent_month,
        "chart_data": chart_data,
    }

    # Obtener los últimos 500 items enviados con el nombre de su feed, filtrando por búsqueda
    query = db.query(SentItem, Feed.name, Feed.acronym).join(Feed, SentItem.feed_id == Feed.id)
    
    if q and q.strip():
        query = query.filter(SentItem.title.ilike(f"%{q}%"))

    recent_items = query.order_by(SentItem.sent_at.desc()).limit(500).all()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "feeds": feeds,
            "next_run": next_run,
            "global_cfg": _global_config(db),
            "stats": stats,
            "recent_items": recent_items,
            "last_day": last_day,
            "q": q or "",
            "i18n": _get_active_lang_dict(db),
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
            "i18n": _get_active_lang_dict(db),
        },
    )


@app.post("/feeds/new")
async def feed_new(
    db: DB,
    name: str = Form(...),
    acronym: str = Form(""),
    url: str = Form(...),
    title_regex_clean: str = Form(""),
    title_transform_regex: str = Form(""),
    title_transform_replace: str = Form(""),
    size_regex_extract: str = Form(""),
    link_transform_regex: str = Form(""),
    link_transform_replace: str = Form(""),
    link_use_guid: bool = Form(False),
    link_guid_prefix: str = Form(""),
    enabled: bool = Form(False),
):
    feed = Feed(
        name=name,
        acronym=acronym or None,
        url=url,
        interval=60,  # default placeholder in DB, scheduler uses global check_interval
        title_regex_clean=title_regex_clean or None,
        title_transform_regex=title_transform_regex or None,
        title_transform_replace=title_transform_replace or None,
        size_regex_extract=size_regex_extract or None,
        link_transform_regex=link_transform_regex or None,
        link_transform_replace=link_transform_replace or None,
        link_use_guid=link_use_guid,
        link_guid_prefix=link_guid_prefix or None,
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
            "i18n": _get_active_lang_dict(db),
        },
    )


@app.post("/feeds/{feed_id}/edit")
async def feed_edit(
    feed_id: int,
    db: DB,
    name: str = Form(...),
    acronym: str = Form(""),
    url: str = Form(...),
    title_regex_clean: str = Form(""),
    title_transform_regex: str = Form(""),
    title_transform_replace: str = Form(""),
    size_regex_extract: str = Form(""),
    link_transform_regex: str = Form(""),
    link_transform_replace: str = Form(""),
    link_use_guid: bool = Form(False),
    link_guid_prefix: str = Form(""),
    enabled: bool = Form(False),
):
    feed = db.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    feed.name = name
    feed.acronym = acronym or None
    feed.url = url
    feed.title_regex_clean = title_regex_clean or None
    feed.title_transform_regex = title_transform_regex or None
    feed.title_transform_replace = title_transform_replace or None
    feed.size_regex_extract = size_regex_extract or None
    feed.link_transform_regex = link_transform_regex or None
    feed.link_transform_replace = link_transform_replace or None
    feed.link_use_guid = link_use_guid
    feed.link_guid_prefix = link_guid_prefix or None
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


@app.post("/feeds/run-all")
async def feed_run_all():
    from fastapi.concurrency import run_in_threadpool
    await run_in_threadpool(run_all_feeds_job)
    return {"success": True}


# --------------------------------------------------------------------------- #
# Global config                                                                #
# --------------------------------------------------------------------------- #


@app.get("/config", response_class=HTMLResponse)
async def config_form(request: Request, db: DB):
    return templates.TemplateResponse(
        request=request,
        name="config.html",
        context={
            "global_cfg": _global_config(db),
            "i18n": _get_active_lang_dict(db),
        },
    )


@app.post("/config")
async def config_save(
    db: DB,
    telegram_token: str = Form(""),
    chat_id: str = Form(""),
    check_interval: str = Form("60"),
    max_items_per_message: str = Form("100"),
    history_limit_total: str = Form("10000"),
    silent_mode_start: str = Form(""),
    silent_mode_end: str = Form(""),
    run_on_startup: bool = Form(False),
    language: str = Form("en"),
    openrouter_api_key: str = Form(""),
    openrouter_model: str = Form("qwen/qwen3.6-35b-a3b"),
    sync_history_enabled: bool = Form(False),
):
    run_on_startup_val = "true" if run_on_startup else "false"
    sync_history_enabled_val = "true" if sync_history_enabled else "false"
    for key, value in [
        ("telegram_token", telegram_token),
        ("chat_id", chat_id),
        ("check_interval", check_interval),
        ("max_items_per_message", max_items_per_message),
        ("history_limit_total", history_limit_total),
        ("silent_mode_start", silent_mode_start),
        ("silent_mode_end", silent_mode_end),
        ("run_on_startup", run_on_startup_val),
        ("language", language),
        ("openrouter_api_key", openrouter_api_key.strip()),
        ("openrouter_model", openrouter_model.strip()),
        ("sync_history_enabled", sync_history_enabled_val),
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
# Logs Viewer                                                                  #
# --------------------------------------------------------------------------- #

@app.get("/logs", response_class=HTMLResponse)
async def logs_view(request: Request, db: DB, lines: int = 500):
    log_path = "data/app.log"
    log_content = ""
    if os.path.exists(log_path):
        try:
            # Read the last N lines
            from collections import deque
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                log_content = "".join(deque(f, lines))
        except Exception as exc:
            log_content = f"Error leyendo archivo de log: {exc}"
    else:
        log_content = "Archivo de registro no encontrado aún."
        
    return templates.TemplateResponse(
        request=request,
        name="logs.html",
        context={
            "global_cfg": _global_config(db),
            "log_content": log_content,
            "lines": lines,
            "i18n": _get_active_lang_dict(db),
        },
    )


@app.post("/logs/clear")
async def logs_clear():
    log_path = "data/app.log"
    try:
        open(log_path, "w").close()
    except Exception:
        pass
    return RedirectResponse("/logs", status_code=303)


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
                "link": entry.get("link", ""),
                "guid": entry.get("id") or entry.get("guid", "")
            })

        return JSONResponse({"items": items, "error": None})
    except Exception as exc:
        return JSONResponse({"items": [], "error": str(exc)})


# --------------------------------------------------------------------------- #
# Series Finder API & Manual Sync                                              #
# --------------------------------------------------------------------------- #

@app.get("/api/series/search")
async def api_search_series(db: DB, q: str = ""):
    if not q or len(q.strip()) < 2:
        return []
    
    q_clean = q.strip()
    
    # 1. Search in historical database
    db_hist = SessionLocalHistorico()
    hist_series = []
    try:
        hist_rows = db_hist.query(Serie).filter(Serie.nombre_limpio.ilike(f"%{q_clean}%")).all()
        hist_series = [row.nombre_limpio for row in hist_rows]
    except Exception as e:
        logger.error("Error al buscar en base de datos histórica: %s", e)
    finally:
        db_hist.close()
        
    # 2. Search in active database
    active_series = set()
    try:
        active_rows = db.query(SentItem).filter(SentItem.title.ilike(f"%{q_clean}%")).all()
        for row in active_rows:
            parsed = parse_dirty_title_on_the_fly(row.title or "")
            if q_clean.lower() in parsed["serie_limpia"].lower():
                active_series.add(parsed["serie_limpia"])
    except Exception as e:
        logger.error("Error al buscar en base de datos activa: %s", e)
        
    # Merge and deduplicate
    all_names = set(hist_series) | active_series
    sorted_names = sorted(list(all_names))
    return [{"name": name} for name in sorted_names]


@app.get("/api/series/seasons")
async def api_series_seasons(name: str, db: DB):
    if not name:
        return []
        
    seasons = set()
    
    # 1. Seasons from historical database
    db_hist = SessionLocalHistorico()
    try:
        serie = db_hist.query(Serie).filter(Serie.nombre_limpio == name).first()
        if serie:
            rows = db_hist.query(Enlace.temporada).filter(Enlace.serie_id == serie.id).distinct().all()
            for r in rows:
                seasons.add(r[0])
    except Exception as e:
        logger.error("Error al obtener temporadas del histórico: %s", e)
    finally:
        db_hist.close()
        
    # 2. Seasons from active database
    try:
        matching_items = db.query(SentItem).filter(SentItem.title.ilike(f"%{name}%")).all()
        for item in matching_items:
            parsed = parse_dirty_title_on_the_fly(item.title or "")
            if parsed["serie_limpia"].lower() == name.lower():
                seasons.add(parsed["temporada"])
    except Exception as e:
        logger.error("Error al obtener temporadas de activa: %s", e)
        
    # Sort with None (specials/no season) handled properly
    sorted_seasons = []
    has_null = False
    for s in seasons:
        if s is None:
            has_null = True
        else:
            sorted_seasons.append(s)
    sorted_seasons.sort()
    
    # Return formatted list
    result = []
    for s in sorted_seasons:
        result.append({"value": s, "label": str(s)})
    if has_null:
        result.append({"value": "null", "label": "null"})
        
    return result


@app.get("/api/series/episodes")
async def api_series_episodes(name: str, db: DB, season: str = None):
    if not name:
        return []
        
    episodes = []
    
    # 1. Episodes from historical DB
    db_hist = SessionLocalHistorico()
    try:
        serie = db_hist.query(Serie).filter(Serie.nombre_limpio == name).first()
        if serie:
            query = db_hist.query(Enlace).filter(Enlace.serie_id == serie.id)
            if season is not None:
                if season == "null":
                    query = query.filter(Enlace.temporada.is_(None))
                else:
                    try:
                        s_val = int(season)
                        query = query.filter(Enlace.temporada == s_val)
                    except ValueError:
                        pass
                
            enlaces = query.all()
            for e in enlaces:
                episodes.append({
                    "temporada": e.temporada,
                    "episodio": e.episodio,
                    "texto_original": e.texto_original,
                    "url": e.url,
                    "source": "historial",
                    "feed_acronym": e.feed_acronym or ""
                })
    except Exception as e:
        logger.error("Error al obtener episodios del histórico: %s", e)
    finally:
        db_hist.close()
        
    # 2. Episodes from active DB (unprocessed)
    try:
        matching_items = db.query(SentItem, Feed.acronym, Feed.name).join(Feed, SentItem.feed_id == Feed.id).filter(SentItem.title.ilike(f"%{name}%")).all()
        for item, acronym, feed_name in matching_items:
            parsed = parse_dirty_title_on_the_fly(item.title or "")
            if parsed["serie_limpia"].lower() == name.lower():
                # If season is specified, filter by it
                match_season = True
                if season is not None:
                    if season == "null":
                        match_season = (parsed["temporada"] is None)
                    else:
                        try:
                            s_val = int(season)
                            match_season = (parsed["temporada"] == s_val)
                        except ValueError:
                            pass
                
                if match_season:
                    episodes.append({
                        "temporada": parsed["temporada"],
                        "episodio": parsed["episodio"],
                        "texto_original": parsed["texto_original"],
                        "url": item.link,
                        "source": "reciente",
                        "feed_acronym": acronym or feed_name[:4]
                    })
    except Exception as e:
        logger.error("Error al obtener episodios de activa: %s", e)
        
    # Sort episodes by season and then episode. Specials/null season go first.
    def sort_key(x):
        t = x.get("temporada")
        ep = x.get("episodio")
        t_val = 0 if t is None else t
        ep_val = -1 if ep is None else ep
        return (t_val, ep_val, x["texto_original"])
        
    episodes.sort(key=sort_key)
    return episodes


@app.post("/api/sync-history")
async def api_sync_history():
    from fastapi.concurrency import run_in_threadpool
    try:
        synced = await run_in_threadpool(sync_historical_db_job, force=True)
        return {"success": True, "synced": synced}
    except Exception as exc:
        logger.error("Error manual trigger sync: %s", exc)
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


@app.post("/api/clear-historical-db")
async def api_clear_historical_db():
    db_hist = SessionLocalHistorico()
    try:
        db_hist.query(Enlace).delete()
        db_hist.query(Serie).delete()
        db_hist.commit()
        logger.info("Base de datos histórica vaciada por petición del usuario.")
        return {"success": True, "message": "Historical DB cleared"}
    except Exception as e:
        db_hist.rollback()
        logger.error("Error al vaciar base de datos histórica: %s", e)
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)
    finally:
        db_hist.close()

