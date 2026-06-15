"""
scheduler.py - APScheduler-based RSS polling logic.

Each active feed gets its own IntervalTrigger job.  When a new item is
found the scheduler:
  1. Cleans the title with title_regex_clean.
  2. Extracts the file size with size_regex_extract.
  3. Transforms the download link with link_transform_regex / replace.
  4. Sends a Markdown message to Telegram.
  5. Updates last_pub_date and resets error counter.

If a feed fails 3+ consecutive times an error alert is sent to Telegram.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import feedparser
import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import os
import json
from models import Feed, GlobalConfig, SentItem, SessionLocal, SessionLocalHistorico, Serie, Enlace

logger = logging.getLogger("rsstracker.scheduler")

# --------------------------------------------------------------------------- #
# Scheduler singleton                                                          #
# --------------------------------------------------------------------------- #

scheduler = BackgroundScheduler(timezone="UTC")


def start_scheduler() -> None:
    """Initialize the APScheduler and schedule the global poll job."""
    scheduler.start()
    _schedule_all_feeds()


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)


# --------------------------------------------------------------------------- #
# Job management helpers                                                       #
# --------------------------------------------------------------------------- #

def _schedule_all_feeds() -> None:
    """Schedule the single global poll job and the history sync job."""
    db = SessionLocal()
    try:
        global_cfg = _get_global_config(db)
        try:
            interval_mins = int(global_cfg.get("check_interval", "60"))
        except ValueError:
            interval_mins = 60
    finally:
        db.close()

    # Schedule the feed polling job
    jid = "poll_all_feeds"
    if scheduler.get_job(jid):
        scheduler.remove_job(jid)

    scheduler.add_job(
        func=run_all_feeds_job,
        trigger=IntervalTrigger(minutes=interval_mins),
        id=jid,
        name="Poll all RSS feeds",
        replace_existing=True,
        misfire_grace_time=60,
    )
    logger.info("Scheduled global poll job every %d min", interval_mins)

    # Schedule the history sync job (every 30 minutes)
    sync_jid = "sync_history_job"
    if scheduler.get_job(sync_jid):
        scheduler.remove_job(sync_jid)

    scheduler.add_job(
        func=sync_historical_db_job,
        trigger=IntervalTrigger(minutes=30),
        id=sync_jid,
        name="Sync active items to historical DB",
        replace_existing=True,
        misfire_grace_time=120,
    )
    logger.info("Scheduled historical DB sync job every 30 min")


def reschedule_all_feeds() -> None:
    """Reschedule the global job when the interval changes."""
    _schedule_all_feeds()


def reschedule_feed(feed_id: int) -> None:
    """No-op since feeds are processed in a single global job."""
    pass


def run_feed_now(feed_id: int) -> None:
    """Trigger an immediate execution outside the normal schedule for a single feed."""
    db = SessionLocal()
    try:
        feed = db.get(Feed, feed_id)
        if feed is None or not feed.enabled:
            return

        global_cfg = _get_global_config(db)
        token = global_cfg.get("telegram_token", "")
        chat_id = global_cfg.get("chat_id", "")
        try:
            max_items = int(global_cfg.get("max_items_per_message", "100"))
        except ValueError:
            max_items = 100

        logger.info("Manually running feed: %s", feed.name)

        try:
            entries = _fetch_rss(feed.url)
        except Exception as exc:
            logger.exception("RSS fetch failed for feed '%s'", feed.name)
            _handle_feed_error(db, feed, token, chat_id, f"RSS fetch error: {exc}")
            return

        # Get the set of already sent links for this feed
        sent_links = {
            item.link
            for item in db.query(SentItem)
            .filter(SentItem.feed_id == feed.id)
            .all()
        }

        new_entries = []
        for entry in reversed(entries):
            link = _get_final_link(entry, feed)
            if link not in sent_links:
                new_entries.append(entry)

        if not new_entries:
            _mark_success(db, feed)
            return

        feed_new_messages = []
        for entry in new_entries:
            try:
                msg = _build_message(entry, feed)
                feed_new_messages.append((entry, msg))
            except Exception as exc:
                logger.error("Build message error for feed %s entry '%s': %s", feed.name, entry.get('title', '?'), exc)

        if feed_new_messages:
            msg_texts = [msg for _, msg in feed_new_messages]
            silent_start = global_cfg.get("silent_mode_start", "")
            silent_end = global_cfg.get("silent_mode_end", "")
            is_silent = _is_silent_hour(silent_start, silent_end)

            for i in range(0, len(msg_texts), max_items):
                chunk = msg_texts[i : i + max_items]
                combined_msg = "\n".join(chunk)
                if token and chat_id:
                    try:
                        _send_telegram(token, chat_id, combined_msg, disable_notification=is_silent)
                    except Exception as exc:
                        logger.error("Failed to send manual combined Telegram message: %s", exc)

            for entry, _ in feed_new_messages:
                link = _get_final_link(entry, feed)
                title = _clean_title(entry.get("title", "Sin título"), feed.title_regex_clean)
                title = _transform_title(title, feed.title_transform_regex, feed.title_transform_replace)
                db.add(SentItem(feed_id=feed.id, link=link, title=title))

            newest_date = feed.last_pub_date
            if newest_date and newest_date.tzinfo is None:
                newest_date = newest_date.replace(tzinfo=timezone.utc)

            for entry, _ in feed_new_messages:
                pub = _parse_published(entry)
                if pub and (newest_date is None or pub > newest_date):
                    newest_date = pub
            if newest_date:
                feed.last_pub_date = newest_date

            _mark_success(db, feed)

        # Check global limit and trigger sync if needed instead of per-feed deletion
        trigger_sync = False
        try:
            raw_limit = global_cfg.get("history_limit_total", "10000")
            total_limit = int(raw_limit) if str(raw_limit).isdigit() else 10000
            total_count = db.query(SentItem).count()
            if total_count > total_limit:
                trigger_sync = True
        except Exception as exc:
            logger.error("Failed to check limit in run_feed_now: %s", exc)

    finally:
        db.close()

    if trigger_sync:
        try:
            logger.info("Total items in active DB exceeds limit after manual run. Triggering historical sync...")
            sync_historical_db_job(force=True)
        except Exception as e:
            logger.error("Failed to run historical DB sync: %s", e)


# --------------------------------------------------------------------------- #
# Core RSS processing                                                          #
# --------------------------------------------------------------------------- #

def run_all_feeds_job() -> None:
    """Polles all enabled feeds, combines their new items, and sends them in grouped Telegram messages."""
    db = SessionLocal()
    try:
        feeds = db.query(Feed).filter(Feed.enabled.is_(True)).all()
        if not feeds:
            logger.info("No enabled feeds to run.")
            return

        global_cfg = _get_global_config(db)
        token = global_cfg.get("telegram_token", "")
        chat_id = global_cfg.get("chat_id", "")
        try:
            max_items = int(global_cfg.get("max_items_per_message", "100"))
        except ValueError:
            max_items = 100

        all_new_item_messages = []
        feed_errors = []

        for feed in feeds:
            logger.info("Polling feed in global job: %s", feed.name)
            try:
                entries = _fetch_rss(feed.url)
            except Exception as exc:
                logger.exception("RSS fetch failed in global job for feed '%s'", feed.name)
                _handle_feed_error(db, feed, token, chat_id, f"RSS fetch error: {exc}", send_alert=False)
                feed_errors.append(f"Feed '{feed.name}': RSS fetch error: {exc}")
                continue

            # Get the set of already sent links for this feed
            sent_links = {
                item.link
                for item in db.query(SentItem)
                .filter(SentItem.feed_id == feed.id)
                .all()
            }

            new_entries = []
            for entry in reversed(entries):
                link = _get_final_link(entry, feed)
                if link not in sent_links:
                    new_entries.append(entry)

            if not new_entries:
                _mark_success(db, feed)
                continue

            feed_new_messages = []
            for entry in new_entries:
                try:
                    msg = _build_message(entry, feed)
                    feed_new_messages.append((entry, msg))
                except Exception as exc:
                    logger.error("Build message error for feed %s entry '%s': %s", feed.name, entry.get('title', '?'), exc)

            if feed_new_messages:
                for entry, msg in feed_new_messages:
                    all_new_item_messages.append(msg)
                    # Record this item as sent
                    link = _get_final_link(entry, feed)
                    title = _clean_title(entry.get("title", "Sin título"), feed.title_regex_clean)
                    title = _transform_title(title, feed.title_transform_regex, feed.title_transform_replace)
                    db.add(SentItem(feed_id=feed.id, link=link, title=title))

                # Update last pub date
                newest_date = feed.last_pub_date
                if newest_date and newest_date.tzinfo is None:
                    newest_date = newest_date.replace(tzinfo=timezone.utc)

                for entry, _ in feed_new_messages:
                    pub = _parse_published(entry)
                    if pub and (newest_date is None or pub > newest_date):
                        newest_date = pub
                if newest_date:
                    feed.last_pub_date = newest_date

                _mark_success(db, feed)

        # Now send all collected messages in groups of max_items
        if all_new_item_messages:
            logger.info("Sending %d combined new items", len(all_new_item_messages))
            silent_start = global_cfg.get("silent_mode_start", "")
            silent_end = global_cfg.get("silent_mode_end", "")
            is_silent = _is_silent_hour(silent_start, silent_end)

            # Group into chunks of size max_items
            for i in range(0, len(all_new_item_messages), max_items):
                chunk = all_new_item_messages[i : i + max_items]
                combined_msg = "\n".join(chunk)
                if token and chat_id:
                    try:
                        _send_telegram(token, chat_id, combined_msg, disable_notification=is_silent)
                    except Exception as exc:
                        logger.error("Failed to send combined Telegram message: %s", exc)

        if feed_errors:
            for feed in feeds:
                if feed.consecutive_errors >= 3:
                    _send_telegram_alert(token, chat_id, feed.name, feed.last_error or "Unknown error")

        # Check global limit and trigger sync if needed instead of deletion
        trigger_sync = False
        try:
            raw_limit = global_cfg.get("history_limit_total", "10000")
            total_limit = int(raw_limit) if str(raw_limit).isdigit() else 10000
            total_count = db.query(SentItem).count()
            if total_count > total_limit:
                trigger_sync = True
        except Exception as exc:
            logger.error("Failed to check limit in run_all_feeds_job: %s", exc)

    finally:
        db.close()

    if trigger_sync:
        try:
            logger.info("Total items in active DB exceeds limit after global run. Triggering historical sync...")
            sync_historical_db_job(force=True)
        except Exception as e:
            logger.error("Failed to run historical DB sync at the end of feed job: %s", e)



# --------------------------------------------------------------------------- #
# RSS helpers                                                                  #
# --------------------------------------------------------------------------- #

def _fetch_rss(url: str) -> list[dict]:
    """Fetch and parse an RSS feed, raising on HTTP / parse errors."""
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()

    parsed = feedparser.parse(response.text)
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"Feed parse error: {parsed.bozo_exception}")

    return parsed.entries  # type: ignore[return-value]


def _filter_new_entries(entries: list, last_pub_date: Optional[datetime]) -> list:
    """Return only entries newer than last_pub_date."""
    if last_pub_date is None:
        # On first run, only process the single most recent entry
        return entries[:1] if entries else []

    new = []
    for entry in entries:
        pub = _parse_published(entry)
        if pub and pub > last_pub_date.replace(tzinfo=timezone.utc):
            new.append(entry)
    return new


def _parse_published(entry) -> Optional[datetime]:
    """Extract publish datetime from an entry (returns UTC-aware or None)."""
    import time as _time

    struct = entry.get("published_parsed") or entry.get("updated_parsed")
    if struct:
        ts = _time.mktime(struct)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    return None


# --------------------------------------------------------------------------- #
# Transformation helpers                                                       #
# --------------------------------------------------------------------------- #

def _build_message(entry, feed: Feed) -> str:
    """Build the Telegram Markdown message for a single RSS entry."""
    title = _clean_title(entry.get("title", "Sin título"), feed.title_regex_clean)
    title = _transform_title(title, feed.title_transform_regex, feed.title_transform_replace)
    size = _extract_size(entry, feed.size_regex_extract)
    link = _get_final_link(entry, feed)

    display = f"{title} ({size})" if size else title
    return f"[{display}]({link})"


def _clean_title(title: str, pattern: Optional[str]) -> str:
    """Remove the matched portion from the title."""
    if not pattern:
        return title.strip()
    try:
        return re.sub(pattern, "", title, flags=re.IGNORECASE).strip()
    except re.error as exc:
        logger.warning("title_regex_clean compile error: %s", exc)
        return title.strip()


def _transform_title(title: str, pattern: Optional[str], replacement: Optional[str]) -> str:
    """Replace pattern with replacement in the title (e.g. swap characters)."""
    if not pattern:
        return title
    try:
        return re.sub(pattern, replacement or "", title).strip()
    except re.error as exc:
        logger.warning("title_transform_regex compile error: %s", exc)
        return title


def _extract_size(entry, pattern: Optional[str]) -> str:
    """Extract file size string from entry content / description."""
    if not pattern:
        return ""

    # Look in multiple fields where trackers place the size info
    content = ""
    for field in ("content", "summary", "description"):
        val = entry.get(field)
        if isinstance(val, list) and val:
            content = val[0].get("value", "")
        elif isinstance(val, str):
            content = val
        if content:
            break

    if not content:
        return ""

    try:
        match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else ""
    except re.error as exc:
        logger.warning("size_regex_extract compile error: %s", exc)
        return ""


def _get_final_link(entry, feed: Feed) -> str:
    """Determines final URL for the item factoring in custom prefix, GUID or standard link and transforms."""
    base_link = ""
    
    if feed.link_use_guid:
        # Use GUID / ID from RSS
        val = entry.get("id") or entry.get("guid")
        if val:
            prefix = (feed.link_guid_prefix or "").strip()
            base_link = prefix + str(val).strip()
    
    if not base_link:
        base_link = entry.get("link") or ""
        
    if not base_link:
        base_link = entry.get("title", "sin-enlace")
        
    # Apply final optional regex transform
    return _transform_link(base_link, feed.link_transform_regex, feed.link_transform_replace)


def _transform_link(link: str, pattern: Optional[str], replacement: Optional[str]) -> str:
    """Transform a download link into a tracker page link."""
    if not pattern or not replacement:
        return link
    try:
        transformed = re.sub(pattern, replacement, link)
        return transformed
    except re.error as exc:
        logger.warning("link_transform_regex compile error: %s", exc)
        return link


# --------------------------------------------------------------------------- #
# Telegram                                                                     #
# --------------------------------------------------------------------------- #

def _send_telegram(token: str, chat_id: str, message: str, disable_notification: bool = False) -> None:
    """Send a Markdown message via the Telegram Bot API."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
        "disable_notification": disable_notification,
    }
    with httpx.Client(timeout=15) as client:
        resp = client.post(url, json=payload)
        resp.raise_for_status()


def _is_silent_hour(start: str, end: str) -> bool:
    """
    Check if the current local time is within the silent hour range.
    start and end are in 'HH:MM' format (e.g., '22:00', '08:00').
    """
    if not start or not end:
        return False
    try:
        from datetime import datetime, time
        now_time = datetime.now().time()
        
        sh, sm = map(int, start.split(":"))
        eh, em = map(int, end.split(":"))
        
        start_time = time(sh, sm)
        end_time = time(eh, em)
        
        if start_time <= end_time:
            return start_time <= now_time <= end_time
        else:
            # Crosses midnight
            return now_time >= start_time or now_time <= end_time
    except Exception:
        return False


def _send_telegram_alert(token: str, chat_id: str, feed_name: str, error: str) -> None:
    """Send a plain-text error alert to Telegram."""
    if not token or not chat_id:
        return
    msg = f"⚠️ *RSS Tracker Error* — `{feed_name}`\n\n{error}"
    try:
        _send_telegram(token, chat_id, msg)
    except Exception as exc:
        logger.error("Failed to send Telegram alert: %s", exc)


# --------------------------------------------------------------------------- #
# DB state helpers                                                             #
# --------------------------------------------------------------------------- #

def _mark_success(db, feed: Feed) -> None:
    feed.last_run = datetime.now(tz=timezone.utc)
    feed.consecutive_errors = 0
    feed.last_error = None
    db.commit()


def _handle_feed_error(db, feed: Feed, token: str, chat_id: str, error: str, send_alert: bool = True) -> None:
    feed.last_run = datetime.now(tz=timezone.utc)
    feed.last_error = error
    feed.consecutive_errors = (feed.consecutive_errors or 0) + 1
    db.commit()
    logger.error("Feed '%s' error (consecutive=%d): %s", feed.name, feed.consecutive_errors, error)

    if send_alert and feed.consecutive_errors >= 3:
        _send_telegram_alert(token, chat_id, feed.name, error)


def _get_global_config(db) -> dict[str, str]:
    rows = db.query(GlobalConfig).all()
    return {row.key: (row.value or "") for row in rows}


# --------------------------------------------------------------------------- #
# Regex test utility (used by the web UI)                                      #
# --------------------------------------------------------------------------- #

def test_regex(pattern: str, replacement: Optional[str], text: str, mode: str) -> dict:
    """
    Test a regex pattern against sample text.

    mode: 'clean' | 'extract' | 'transform'
    Returns: { "result": str, "error": str|None }
    """
    try:
        compiled = re.compile(pattern, re.IGNORECASE | re.DOTALL)
    except re.error as exc:
        return {"result": "", "error": f"Regex compile error: {exc}"}

    try:
        if mode == "clean":
            result = re.sub(compiled, replacement or "", text).strip()
        elif mode == "extract":
            m = compiled.search(text)
            result = m.group(1).strip() if m and m.lastindex else (m.group(0) if m else "")
        elif mode == "transform":
            result = re.sub(compiled, replacement or "", text)
        else:
            result = ""
        return {"result": result, "error": None}
    except Exception as exc:
        return {"result": "", "error": str(exc)}


# --------------------------------------------------------------------------- #
# Historical DB Sync Utilities                                                 #
# --------------------------------------------------------------------------- #

def parse_dirty_title_on_the_fly(title: str) -> dict:
    """
    Analiza provisionalmente un título sucio usando expresiones regulares
    para extraer el nombre de la serie, temporada y episodio.
    """
    if not title:
        return {"serie_limpia": "Sin título", "temporada": None, "episodio": None, "texto_original": ""}

    # 1. Patrón con E/e explícito: S01E02, S01 E02, s01.e02, etc.
    se_explicit = re.search(r'(?i)s(\d+)\s*e\s*(\d+)', title)
    if se_explicit:
        try:
            season = int(se_explicit.group(1))
            episode = int(se_explicit.group(2))
        except (ValueError, TypeError):
            season, episode = None, None
        name_part = title[:se_explicit.start()]
        return {
            "serie_limpia": _clean_on_the_fly_name(name_part),
            "temporada": season,
            "episodio": episode,
            "texto_original": title
        }

    # 2. Patrón con x/X: 01x02, 1x02
    x_match = re.search(r'(?i)\b(\d+)\s*x\s*(\d+)\b', title)
    if x_match:
        try:
            season = int(x_match.group(1))
            episode = int(x_match.group(2))
        except (ValueError, TypeError):
            season, episode = None, None
        name_part = title[:x_match.start()]
        # Evitar falsos positivos con resoluciones como 1920x1080 o 1280x720
        if season not in (1920, 1280, 2560, 3840) and episode not in (1080, 720, 1440, 2160):
            return {
                "serie_limpia": _clean_on_the_fly_name(name_part),
                "temporada": season,
                "episodio": episode,
                "texto_original": title
            }

    # 3. Patrón con separador implícito: S01-02, S01.02, S01 02, S01_02
    se_implicit = re.search(r'(?i)s(\d+)\s*[\.\-\_ ]\s*(\d+)\b', title)
    if se_implicit:
        try:
            season = int(se_implicit.group(1))
            episode = int(se_implicit.group(2))
        except (ValueError, TypeError):
            season, episode = None, None
            
        # Descartar si el número de episodio parece una resolución de pantalla o es irrazonablemente grande
        if episode in (1080, 720, 2160, 576, 480, 540, 360, 240, 1440):
            episode = None
        elif episode is not None and episode >= 150:
            # Si el episodio es >= 150 y no tiene un 'E' explícito, es probable que no sea un número de episodio
            episode = None
            
        if season is not None:
            name_part = title[:se_implicit.start()]
            if episode is not None:
                return {
                    "serie_limpia": _clean_on_the_fly_name(name_part),
                    "temporada": season,
                    "episodio": episode,
                    "texto_original": title
                }
            else:
                # Si descartamos el episodio, pero la temporada sigue siendo válida, la tratamos como sólo temporada
                return {
                    "serie_limpia": _clean_on_the_fly_name(name_part),
                    "temporada": season,
                    "episodio": None,
                    "texto_original": title
                }

    # Patrón: "Temporada X Completa"
    temp_completa = re.search(r'(?i)temporada\s*(\d+)\s*completa', title)
    if temp_completa:
        try:
            season = int(temp_completa.group(1))
        except (ValueError, TypeError):
            season = None
        name_part = title[:temp_completa.start()]
        return {
            "serie_limpia": _clean_on_the_fly_name(name_part),
            "temporada": season,
            "episodio": None,
            "texto_original": title
        }

    # Patrón: "Temporada X"
    temp_match = re.search(r'(?i)temporada\s*(\d+)', title)
    if temp_match:
        try:
            season = int(temp_match.group(1))
        except (ValueError, TypeError):
            season = None
        name_part = title[:temp_match.start()]
        
        # Buscar episodio en la parte restante
        rest = title[temp_match.end():]
        ep_match = re.search(r'(?i)(?:capitulo|episodio|ep|c)\s*(\d+)', rest)
        episode = None
        if ep_match:
            try:
                episode = int(ep_match.group(1))
            except (ValueError, TypeError):
                pass
        return {
            "serie_limpia": _clean_on_the_fly_name(name_part),
            "temporada": season,
            "episodio": episode,
            "texto_original": title
        }

    # Patrón: S01, s02, S1, s2, etc. (solo temporada)
    s_only_match = re.search(r'(?i)\bs(\d+)\b', title)
    if s_only_match:
        try:
            season = int(s_only_match.group(1))
        except (ValueError, TypeError):
            season = None
        name_part = title[:s_only_match.start()]
        return {
            "serie_limpia": _clean_on_the_fly_name(name_part),
            "temporada": season,
            "episodio": None,
            "texto_original": title
        }

    # Si no coincide nada, limpiamos el título completo como nombre de la serie
    return {
        "serie_limpia": _clean_on_the_fly_name(title),
        "temporada": None,
        "episodio": None,
        "texto_original": title
    }


def _clean_on_the_fly_name(name: str) -> str:
    """Elimina residuos comunes en títulos de series al vuelo."""
    # Eliminar corchetes, paréntesis y su contenido
    name = re.sub(r'\[[^\]]*\]', '', name)
    name = re.sub(r'\([^\)]*\)', '', name)
    # Reemplazar puntos, guiones bajos, barras por espacios
    name = re.sub(r'[\.\_\/\-\+]', ' ', name)
    # Quitar palabras de calidad o ripeo comunes
    name = re.sub(r'(?i)\b(?:1080p|720p|hdtv|x264|h264|dual|ac3|bluray|rip|webrip|web-dl|xvid|multisub)\b', '', name)
    # Limpiar espacios múltiples y extremos
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Quitar año de 4 dígitos (1900-2099) si no es lo único en el nombre
    name_without_year = re.sub(r'\b(19\d\d|20\d\d)\b', '', name).strip()
    if name_without_year:
        name = name_without_year
        
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "Sin título"


def clean_titles_with_openrouter(dirty_titles: list[dict], api_key: str, model_name: str) -> list[dict]:
    if not api_key:
        logger.error("La clave API de OpenRouter está vacía. No se puede realizar la consulta.")
        return []

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/sherlockes/rsstracker",
        "X-Title": "RSS Tracker",
    }

    system_prompt = (
        "Eres un extractor de datos estructurados para series de televisión.\n"
        "Se te proporcionará una lista de objetos JSON con un ID y un título sucio de una serie.\n"
        "Tu tarea es analizar cada título y devolver una lista de objetos JSON. Cada objeto debe tener exactamente estas claves:\n"
        '- "id": El ID numérico original provisto para ese título.\n'
        '- "serie_limpia": El nombre limpio de la serie (ej. "Deadloch" para "Deadloch S01", "Breaking Bad" para "Breakin Bad s02e05", "Los Simpson" para "Los Simpson - Temporada 4 Completa"). Elimina temporada, episodio, calidad, grupo, etc. Corrige erratas menores si es obvio y pon mayúsculas correctamente.\n'
        '- "temporada": El número de temporada como entero (ej. 1 para "Deadloch S01"), o null si no se especifica.\n'
        '- "episodio": El número de episodio como entero, o null si es temporada completa o no se especifica (ej. null para "Deadloch S01").\n\n'
        "Devuelve ÚNICAMENTE un array JSON válido. No incluyas explicaciones, bloques de código markdown, ni texto adicional."
    )

    user_content = json.dumps(dirty_titles, ensure_ascii=False)

    payload = {
        "model": model_name or "qwen/qwen3.6-35b-a3b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ]
    }

    try:
        with httpx.Client(timeout=45) as client:
            response = client.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                logger.error("OpenRouter API returned status %s: %s", response.status_code, response.text)
            response.raise_for_status()
            res_data = response.json()
            content = res_data["choices"][0]["message"]["content"].strip()
            
            # Limpiar posibles bloques de código markdown si la IA no siguió las instrucciones
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            parsed = json.loads(content)
            
            # Ajustar en caso de que la respuesta sea un diccionario que envuelva la lista
            if isinstance(parsed, dict):
                for key in ["items", "data", "results", "series"]:
                    if key in parsed and isinstance(parsed[key], list):
                        return parsed[key]
            if isinstance(parsed, list):
                return parsed
            return []
    except Exception as e:
        logger.error("Error al llamar a OpenRouter o procesar la respuesta: %s", e)
        return []


def sync_historical_db_job(force: bool = False) -> int:
    """
    Sincroniza y alinea la base de datos activa con la base de datos histórica (archivo):
    - Si el número de registros en la activa es MENOR que el límite (Recent items),
      mueve registros del histórico de vuelta a la activa (revisando duplicados por URL).
    - Si el número de registros en la activa es MAYOR que el límite (Recent items),
      migra los registros más antiguos (exceso) al histórico a través de la IA (o parseo local).
    """
    logger.info("Iniciando tarea de alineación/sincronización con base de datos histórica...")
    
    db_active = SessionLocal()
    global_cfg = _get_global_config(db_active)
    
    # Verificar si la sincronización automática está habilitada (a menos que se force)
    sync_enabled = global_cfg.get("sync_history_enabled", "true").strip().lower() == "true"
    if not force and not sync_enabled:
        logger.info("Sincronización automática de histórico desactivada. Omitiendo tarea.")
        db_active.close()
        return 0
        
    db_hist = SessionLocalHistorico()
    
    synced_count = 0
    try:
        # Obtener límite configurado
        raw_limit = global_cfg.get("history_limit_total", "10000")
        limit = int(raw_limit) if str(raw_limit).isdigit() else 10000
        
        active_count = db_active.query(SentItem).count()
        
        # CASO 1: Llevar registros del archivo de vuelta a la base de datos principal
        if active_count < limit:
            free_space = limit - active_count
            logger.info("Base de datos activa con %d items (límite %d). Espacio libre: %d.", active_count, limit, free_space)
            
            # Obtener enlaces de la base histórica ordenados por id
            enlaces_to_restore = db_hist.query(Enlace).order_by(Enlace.id.asc()).limit(free_space).all()
            if not enlaces_to_restore:
                logger.info("No hay registros en la base de datos de archivo para restaurar.")
                db_active.close()
                db_hist.close()
                return 0
                
            logger.info("Restaurando %d registros desde el archivo a la principal...", len(enlaces_to_restore))
            
            # Pre-cargar URLs existentes en activa para evitar duplicados rápidamente
            active_urls = {row[0] for row in db_active.query(SentItem.link).all()}
            
            # Mapear feeds para determinar el feed_id correspondiente al acronym del Enlace
            feeds = db_active.query(Feed).all()
            reverse_feeds_map = {}
            for f in feeds:
                if f.acronym:
                    reverse_feeds_map[f.acronym] = f.id
            for f in feeds:
                prefix = f.name[:4]
                if prefix not in reverse_feeds_map:
                    reverse_feeds_map[prefix] = f.id
            
            default_feed_id = feeds[0].id if feeds else 1
            
            restored_in_batch = 0
            for enlace in enlaces_to_restore:
                if enlace.url in active_urls:
                    # Duplicado: lo eliminamos de la histórica pero no lo añadimos a la activa
                    db_hist.delete(enlace)
                    continue
                
                feed_id = reverse_feeds_map.get(enlace.feed_acronym, default_feed_id)
                new_item = SentItem(
                    feed_id=feed_id,
                    link=enlace.url,
                    title=enlace.texto_original,
                    sent_at=datetime.utcnow()
                )
                db_active.add(new_item)
                active_urls.add(enlace.url)
                db_hist.delete(enlace)
                restored_in_batch += 1
                synced_count += 1
            
            db_active.commit()
            db_hist.commit()
            logger.info("Restaurados %d registros exitosamente.", restored_in_batch)
            
            # Limpiar series huérfanas en histórico
            try:
                active_serie_ids = {row[0] for row in db_hist.query(Enlace.serie_id).distinct().all()}
                db_hist.query(Serie).filter(~Serie.id.in_(list(active_serie_ids))).delete(synchronize_session=False)
                db_hist.commit()
            except Exception as e:
                logger.error("Error al limpiar series huérfanas: %s", e)
                db_hist.rollback()
                
        # CASO 2: Migrar registros de la base activa a la de archivo (exceso de límite)
        elif active_count > limit:
            excess_count = active_count - limit
            logger.info("Base de datos activa con %d items (límite %d). Exceso: %d.", active_count, limit, excess_count)
            
            # Obtener configuración de OpenRouter
            api_key = global_cfg.get("openrouter_api_key", "").strip() or os.environ.get("OPENROUTER_API_KEY", "")
            model_name = global_cfg.get("openrouter_model", "").strip() or os.environ.get("OPENROUTER_MODEL", "qwen/qwen3.6-35b-a3b")
            
            # Procesar en bucle con lotes de hasta 50 elementos para evitar saturar OpenRouter
            while excess_count > 0:
                batch_size = min(50, excess_count)
                items = db_active.query(SentItem).order_by(SentItem.sent_at.asc()).limit(batch_size).all()
                if not items:
                    break
                    
                # Mapear acrónimos de feeds
                feeds_map = {f.id: (f.acronym or f.name[:4]) for f in db_active.query(Feed).all()}
                
                logger.info("Sincronizando lote de %d registros excedentes...", len(items))
                dirty_titles = [{"id": item.id, "title": item.title or ""} for item in items]
                
                cleaned_results = []
                if api_key:
                    cleaned_results = clean_titles_with_openrouter(dirty_titles, api_key, model_name)
                else:
                    logger.warning("OPENROUTER_API_KEY no configurada. Se usará el parseador local al vuelo.")
                
                results_map = {}
                for r in cleaned_results:
                    try:
                        r_id = int(r.get("id"))
                        results_map[r_id] = r
                    except (ValueError, TypeError):
                        continue
                
                # Pre-cargar URLs existentes en la base de datos histórica para evitar duplicados en archivo
                hist_urls = {row[0] for row in db_hist.query(Enlace.url).all()}
                
                success_ids = []
                for item in items:
                    # Evitar duplicados por URL en la base histórica
                    if item.link in hist_urls:
                        # Si ya está en la de archivo, simplemente lo eliminamos del activo para no duplicar
                        success_ids.append(item.id)
                        continue
                        
                    res = results_map.get(item.id)
                    if not res:
                        # Fallback al parseador al vuelo si falló/no hay IA
                        res = parse_dirty_title_on_the_fly(item.title or "")
                        
                    nombre_limpio = res.get("serie_limpia")
                    if not nombre_limpio or not nombre_limpio.strip():
                        nombre_limpio = item.title or "Desconocido"
                    nombre_limpio = nombre_limpio.strip()
                    
                    # Buscar o crear la serie
                    serie = db_hist.query(Serie).filter(Serie.nombre_limpio == nombre_limpio).first()
                    if not serie:
                        serie = Serie(nombre_limpio=nombre_limpio)
                        db_hist.add(serie)
                        db_hist.commit()
                        db_hist.refresh(serie)
                        
                    temp_val = res.get("temporada")
                    ep_val = res.get("episodio")
                    
                    try:
                        temporada = int(temp_val) if temp_val is not None else None
                    except (ValueError, TypeError):
                        temporada = None
                        
                    try:
                        episodio = int(ep_val) if ep_val is not None else None
                    except (ValueError, TypeError):
                        episodio = None
                        
                    feed_acronym = feeds_map.get(item.feed_id, "")
                    
                    # Insertar en Enlaces
                    enlace = Enlace(
                        serie_id=serie.id,
                        temporada=temporada,
                        episodio=episodio,
                        texto_original=item.title or "",
                        url=item.link,
                        feed_acronym=feed_acronym
                    )
                    db_hist.add(enlace)
                    # Añadir a hist_urls local para evitar insertar duplicados en el mismo bucle/lote
                    hist_urls.add(item.link)
                    success_ids.append(item.id)
                    
                # Confirmar en base histórica
                db_hist.commit()
                
                # Eliminar de la base activa
                if success_ids:
                    db_active.query(SentItem).filter(SentItem.id.in_(success_ids)).delete(synchronize_session=False)
                    db_active.commit()
                    synced_count += len(success_ids)
                    logger.info("Migrados %d registros excedentes de activa a histórica.", len(success_ids))
                    excess_count -= len(success_ids)
                else:
                    break
                    
        else:
            logger.info("La base de datos activa tiene exactamente el límite configurado (%d). No se requieren cambios.", limit)
            
    except Exception as e:
        db_active.rollback()
        db_hist.rollback()
        logger.error("Error durante el job de sincronización/alineación histórica: %s", e)
    finally:
        db_active.close()
        db_hist.close()
        
    return synced_count
