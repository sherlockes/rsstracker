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

from models import Feed, GlobalConfig, SentItem, SessionLocal

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
    """Schedule the single global poll job."""
    db = SessionLocal()
    try:
        global_cfg = _get_global_config(db)
        try:
            interval_mins = int(global_cfg.get("check_interval", "60"))
        except ValueError:
            interval_mins = 60
    finally:
        db.close()

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

        # Prune to keep only the last 100 sent items for this feed
        excess = (
            db.query(SentItem)
            .filter(SentItem.feed_id == feed.id)
            .order_by(SentItem.id.desc())
            .offset(100)
            .all()
        )
        for old_item in excess:
            db.delete(old_item)
        db.commit()

    finally:
        db.close()


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

        # Keep global SentItem count capped to configurable limit
        try:
            raw_limit = global_cfg.get("history_limit_total", "10000")
            total_limit = int(raw_limit) if str(raw_limit).isdigit() else 10000
            
            # Subquery to find the IDs that we should DELETE
            # (everything beyond the newest N elements)
            excess_ids = (
                db.query(SentItem.id)
                .order_by(SentItem.id.desc())
                .offset(total_limit)
                .all()
            )
            
            if excess_ids:
                ids_list = [row[0] for row in excess_ids]
                # SQLite limits maximum number of variables in query (999), so batching or simple count delete
                db.query(SentItem).filter(SentItem.id.in_(ids_list)).delete(synchronize_session=False)
                db.commit()
                logger.info("Purged %d excess total historical items.", len(ids_list))
        except Exception as exc:
            logger.error("Failed to global purge items: %s", exc)

    finally:
        db.close()



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
