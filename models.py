"""
models.py - SQLAlchemy ORM models for RSS Tracker.
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = "sqlite:///./data/rsstracker.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


# --- Historical Database Configuration ---------------------------------------
HISTORICO_DATABASE_URL = "sqlite:///./data/historico.db"

engine_historico = create_engine(
    HISTORICO_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

SessionLocalHistorico = sessionmaker(autocommit=False, autoflush=False, bind=engine_historico)


class BaseHistorico(DeclarativeBase):
    pass


class Serie(BaseHistorico):
    """Represents a clean TV show series name in the historical DB."""

    __tablename__ = "Series"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    nombre_limpio: str = Column(String(255), unique=True, nullable=False, index=True)


class Enlace(BaseHistorico):
    """Represents a parsed link corresponding to a season and episode of a series."""

    __tablename__ = "Enlaces"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    serie_id: int = Column(Integer, ForeignKey("Series.id"), nullable=False, index=True)
    temporada: int | None = Column(Integer, nullable=True)
    episodio: int | None = Column(Integer, nullable=True)
    texto_original: str = Column(Text, nullable=False)
    url: str = Column(Text, nullable=False)
    feed_acronym: str | None = Column(String(50), nullable=True)

    __table_args__ = (
        Index("idx_enlaces_busqueda", "serie_id", "temporada", "episodio"),
    )



class Feed(Base):
    """Represents a single RSS feed / tracker configuration."""

    __tablename__ = "feeds"

    id: int = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name: str = Column(String(255), nullable=False)
    acronym: str | None = Column(String(20), nullable=True)
    url: str = Column(Text, nullable=False)
    interval: int = Column(Integer, default=60, nullable=False)  # minutes

    # --- Regex fields --------------------------------------------------------
    title_regex_clean: str | None = Column(Text, nullable=True)
    title_transform_regex: str | None = Column(Text, nullable=True)
    title_transform_replace: str | None = Column(Text, nullable=True)
    size_regex_extract: str | None = Column(Text, nullable=True)
    link_transform_regex: str | None = Column(Text, nullable=True)
    link_transform_replace: str | None = Column(Text, nullable=True)
    link_use_guid: bool = Column(Boolean, default=False, nullable=False)
    link_guid_prefix: str | None = Column(Text, nullable=True)


    # --- Telegram credentials ------------------------------------------------
    telegram_token: str | None = Column(Text, nullable=True)
    chat_id: str | None = Column(Text, nullable=True)

    # --- State ---------------------------------------------------------------
    last_pub_date: datetime | None = Column(DateTime, nullable=True)
    last_run: datetime | None = Column(DateTime, nullable=True)
    last_error: str | None = Column(Text, nullable=True)
    consecutive_errors: int = Column(Integer, default=0, nullable=False)
    enabled: bool = Column(Boolean, default=True, nullable=False)


class GlobalConfig(Base):
    """Key-value store for global application settings."""

    __tablename__ = "global_config"

    key: str = Column(String(100), primary_key=True)
    value: str | None = Column(Text, nullable=True)


class SentItem(Base):
    """Tracks previously sent RSS items for deduplication, capped at 100 per feed."""

    __tablename__ = "sent_items"

    id: int = Column(Integer, primary_key=True, index=True, autoincrement=True)
    feed_id: int = Column(Integer, index=True, nullable=False)
    link: str = Column(Text, index=True, nullable=False)
    title: str | None = Column(Text, nullable=True)
    sent_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)


def init_db() -> None:
    """Create all tables and seed default config if needed."""
    Base.metadata.create_all(bind=engine)
    BaseHistorico.metadata.create_all(bind=engine_historico)
    
    # Automatic migration for 'title' column in 'sent_items'
    import sqlite3
    try:
        conn = sqlite3.connect("./data/rsstracker.db")
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(sent_items)")
        columns = [row[1] for row in cursor.fetchall()]
        if "title" not in columns:
            cursor.execute("ALTER TABLE sent_items ADD COLUMN title TEXT")
            conn.commit()

        # Automatic migration for title transform columns in 'feeds'
        cursor.execute("PRAGMA table_info(feeds)")
        feed_columns = [row[1] for row in cursor.fetchall()]
        for col in ("title_transform_regex", "title_transform_replace", "acronym", "link_guid_prefix"):
            if col not in feed_columns:
                cursor.execute(f"ALTER TABLE feeds ADD COLUMN {col} TEXT")
                conn.commit()
        if "link_use_guid" not in feed_columns:
            cursor.execute("ALTER TABLE feeds ADD COLUMN link_use_guid INTEGER DEFAULT 0")
            conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

    # Automatic migration for 'feed_acronym' column in 'Enlaces' of historico.db
    try:
        conn_hist = sqlite3.connect("./data/historico.db")
        cursor_hist = conn_hist.cursor()
        cursor_hist.execute("PRAGMA table_info(Enlaces)")
        enlaces_columns = [row[1] for row in cursor_hist.fetchall()]
        if "feed_acronym" not in enlaces_columns:
            cursor_hist.execute("ALTER TABLE Enlaces ADD COLUMN feed_acronym TEXT")
            conn_hist.commit()
    except Exception:
        pass
    finally:
        conn_hist.close()
    db = SessionLocal()
    try:
        keys_and_defaults = {
            "telegram_token": "",
            "chat_id": "",
            "check_interval": "60",
            "max_items_per_message": "100",
            "silent_mode_start": "",
            "silent_mode_end": "",
            "run_on_startup": "false",
            "language": "en",
            "openrouter_api_key": "",
            "openrouter_model": "qwen/qwen3.6-35b-a3b",
            "sync_history_enabled": "true"
        }
        for key, default_val in keys_and_defaults.items():
            row = db.get(GlobalConfig, key)
            if not row:
                db.add(GlobalConfig(key=key, value=default_val))
            else:
                if key == "openrouter_model" and row.value in ("meta-llama/llama-3-8b-instruct", "qwen/qwen-3.6-35b-a3b"):
                    row.value = "qwen/qwen3.6-35b-a3b"
        db.commit()
    except Exception as exc:
        import logging
        logging.getLogger("rsstracker.models").error("Error in init_db seeding: %s", exc)
    finally:
        db.close()
