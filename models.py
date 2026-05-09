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


class Feed(Base):
    """Represents a single RSS feed / tracker configuration."""

    __tablename__ = "feeds"

    id: int = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name: str = Column(String(255), nullable=False)
    url: str = Column(Text, nullable=False)
    interval: int = Column(Integer, default=60, nullable=False)  # minutes

    # --- Regex fields --------------------------------------------------------
    title_regex_clean: str | None = Column(Text, nullable=True)
    size_regex_extract: str | None = Column(Text, nullable=True)
    link_transform_regex: str | None = Column(Text, nullable=True)
    link_transform_replace: str | None = Column(Text, nullable=True)


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
    sent_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)


def init_db() -> None:
    """Create all tables and seed default config if needed."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        for key in ("telegram_token", "chat_id", "check_interval", "max_items_per_message", "silent_mode_start", "silent_mode_end", "run_on_startup"):
            if not db.get(GlobalConfig, key):
                if key == "check_interval":
                    default_val = "60"
                elif key == "max_items_per_message":
                    default_val = "100"
                elif key == "run_on_startup":
                    default_val = "false"
                else:
                    default_val = ""
                db.add(GlobalConfig(key=key, value=default_val))
        db.commit()
    finally:
        db.close()

