import os
import sqlite3
import re
import httpx
import feedparser

DB_PATH = "data/rsstracker.db"

def run_regeneration():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Base de datos no encontrada en {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Verificar si las columnas nuevas existen en feeds
    cursor.execute("PRAGMA table_info(feeds)")
    cols = [row["name"] for row in cursor.fetchall()]

    has_guid = "link_use_guid" in cols
    has_prefix = "link_guid_prefix" in cols
    has_t_tr = "title_transform_regex" in cols

    print(f"[INFO] Conectado. Analizando configuración actual...")

    cursor.execute("SELECT * FROM feeds WHERE enabled = 1")
    feeds = cursor.fetchall()

    total_items_touched = 0

    for f in feeds:
        f_dict = dict(f)
        fid = f_dict["id"]
        name = f_dict["name"]
        url = f_dict["url"]

        print(f" -> Procesando Feed: {name}")

        # Parámetros de Link
        use_guid = bool(f_dict.get("link_use_guid", 0)) if has_guid else False
        prefix = (f_dict.get("link_guid_prefix") or "").strip() if has_prefix else ""
        l_rx = f_dict.get("link_transform_regex")
        l_repl = f_dict.get("link_transform_replace")

        # Parámetros de Título
        t_clean = f_dict.get("title_regex_clean")
        t_rx = f_dict.get("title_transform_regex") if has_t_tr else None
        t_repl = f_dict.get("title_transform_replace") if has_t_tr else None

        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                resp = client.get(url)
                resp.raise_for_status()
            
            parsed = feedparser.parse(resp.text)
            if not parsed.entries:
                print(f"    (!) No se obtuvieron items de este RSS actualmente.")
                continue

            c = 0
            for entry in parsed.entries:
                raw_link = entry.get("link") or ""
                raw_guid = entry.get("id") or entry.get("guid") or ""
                raw_title = entry.get("title", "Sin título")

                # A. CALCULAR NUEVO LINK
                final_link = ""
                if use_guid and raw_guid:
                    final_link = prefix + str(raw_guid).strip()
                
                if not final_link:
                    final_link = raw_link or ""
                
                if not final_link:
                    final_link = raw_title # fallback
                
                # Aplicar regex transform link si existe
                if l_rx and l_repl:
                    try:
                        final_link = re.sub(l_rx, l_repl, final_link)
                    except Exception: pass

                # B. CALCULAR NUEVO TÍTULO
                final_title = raw_title
                if t_clean:
                    try:
                        final_title = re.sub(t_clean, "", final_title, flags=re.IGNORECASE)
                    except Exception: pass
                if t_rx:
                    try:
                        final_title = re.sub(t_rx, t_repl or "", final_title)
                    except Exception: pass
                
                final_title = final_title.strip()

                # C. ACTUALIZAR BASE DE DATOS
                # Buscamos por el raw_link, raw_guid o incluso final_link actual para atraparlo
                cursor.execute("""
                    UPDATE sent_items 
                    SET link = ?, title = ?
                    WHERE feed_id = ? 
                      AND (link = ? OR (link = ? AND ? != ''))
                """, (
                    final_link, 
                    final_title, 
                    fid, 
                    raw_link, 
                    raw_guid, raw_guid
                ))
                
                if cursor.rowcount > 0:
                    c += cursor.rowcount

            conn.commit()
            print(f"    [OK] Regenerados/Actualizados {c} items para este feed.")
            total_items_touched += c

        except Exception as exc:
            print(f"    [ERROR] Falló al procesar {name}: {exc}")

    conn.close()
    print(f"\n[FINALIZADO] Éxito total. Se han reescrito {total_items_touched} registros históricos con las configuraciones de Link y Título actuales.")

if __name__ == "__main__":
    run_regeneration()
