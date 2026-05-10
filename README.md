# 📡 RSS Tracker → Telegram

![Licencia](https://img.shields.io/badge/licencia-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)
![Estilo](https://img.shields.io/badge/UI-Dark%20Glassmorphism-cyan.svg)

**RSS Tracker → Telegram** es un microservicio ligero y autónomo en Python diseñado para monitorear feeds RSS de trackers privados (o públicos), procesar sus contenidos dinámicamente y enviar notificaciones formateadas, consolidadas y personalizadas directamente a tus canales de Telegram. 

Incluye una interfaz web moderna con diseño **Premium Dark Glassmorphism** y estadísticas interactivas.

---

## ✨ Características Principales

- 📊 **Dashboard Interactivo**: Visualización del estado de tus feeds en tiempo real con una gráfica de actividad semanal dinámica alimentada por *Chart.js*.
- 🧠 **Motor de Regex Inteligente**: Potente sistema integrado para limpiar títulos, extraer tamaños de archivos y transformar enlaces de descarga en fichas informativas.
- 🛡️ **Sandbox de Pruebas**: Herramienta en tiempo real para testear tus expresiones regulares contra código HTML antes de guardarlas.
- 🤖 **Consolidación Inteligente**: Agrupa múltiples actualizaciones en mensajes combinados de Telegram para evitar el spam de notificaciones.
- 🌙 **Modo Silencioso Inteligente**: Rango horario nocturno personalizable en el que el bot silenciará el sonido y la vibración, soportando incluso tramos que cruzan la medianoche.
- ⚡ **Actualización en Arranque**: Opción para forzar un escaneo masivo e inmediato de todos los trackers en el segundo exacto en el que se inicia el contenedor Docker.
- 💾 **Copias de Seguridad**: Sistema de exportación e importación de la base de datos SQLite completo y seguro integrado en el panel de control.
- 🔍 **Buscador Global**: Motor de búsqueda de historial integrado en el Dashboard con soporte para encontrar cualquier registro entre miles de entradas instantáneamente.
- 📁 **Gestión de Historial configurable**: Límite global de conservación de registros configurable para mantener la base de datos ligera borrando excedentes antiguos automáticamente.
- 🐋 **Despliegue en 10 Segundos**: Contenerizado nativo con Docker y Docker Compose.

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología Empleada |
| :--- | :--- |
| **Backend Core** | FastAPI (Asíncrono) + Uvicorn |
| **Capa de Datos** | SQLite + SQLAlchemy ORM |
| **Motor de Tareas** | APScheduler (Background Threaded Jobs) |
| **Interfaz de Usuario** | Jinja2 + Bootstrap 5 + Vanilla JavaScript |
| **Visualizaciones** | Chart.js (Neo-Cyan Theme) |
| **Contenedores** | Docker Engine + Compose V2 |

---

## 🚀 Inicio Rápido

Despliega la aplicación completa ejecutando un solo comando en tu terminal:

```bash
# Construir la imagen e iniciar en segundo plano
docker compose up -d --build

# Monitorizar la salida de los logs para ver la ejecución del planificador
docker compose logs -f rsstracker
```

La interfaz de gestión del panel de control estará disponible de inmediato en:  
👉 **[http://localhost:8765](http://localhost:8765)**

---

## 🔧 Configuración Avanzada de Feeds

Cada tracker individual que añadas al sistema te permite parametrizar su comportamiento al milímetro:

| Parámetro | Propósito y Ejemplo |
| :--- | :--- |
| **`Nombre`** | Identificador visual en el dashboard (Ej: *TorrentTracker-HD*). |
| **`URL del Feed`** | Dirección RSS completa (normalmente incluye tu `passkey` privada). |
| **`Title Regex Clean`** | Patrón para remover basura visual como `[1080p]`, `[PACK]`, etc. |
| **`Size Regex Extract`** | Grupo de captura (1) para capturar los GBs/MBs del HTML de la descripción. |
| **`Transform Regex`** | Patrón para cazar el ID de descarga de la URL nativa del tracker. |
| **`Transform Replace`** | Plantilla destino para reescribir la URL hacia la ficha descriptiva usando `$1`. |
| **`Siglas`** | Código corto opcional (Ej: *HDZ*) para identificar de forma compacta el origen en la lista de items. |
| **`Telegram Token`** | Permite sobreescribir las credenciales globales para este feed específico. |
| **`Transformación de Títulos`** | Capa secundaria de limpieza que permite sustituciones específicas (ej: reemplazar puntos `\.` por espacios). |
| **`Modo Enlace (GUID)`** | Alternativa para forzar que el enlace apunte a la ficha del torrent usando el ID (`guid`) en lugar del enlace directo. |
| **`Prefijo URL`** | Dominio/Ruta base para combinar con el ID (autodetección mágica disponible en el asistente). |

### 🧪 Ejemplos Prácticos de Expresiones Regulares

* **Limpieza de corchetes iniciales:**  
  `\[PACK\].*|\[.*?\]\s*`
  
* **Extracción de peso del HTML:**  
  `<strong>Tamaño<\/strong>:\s*([\d.]+\s*[KMGT]iB)`

* **Conversión de URL de descarga a Ficha Web:**  
  - Patrón: `/download/(\d+)\.torrent`  
  - Reemplazo: `/details.php?id=$1`

---

## ⚙️ Ajustes Globales

El menú **Configuración** centraliza la gestión avanzada del motor:

1. **Credenciales por Defecto**: Tu Token de @BotFather y el ChatID receptor para todos los feeds que no especifiquen uno propio.
2. **Intervalo de Sincronización**: Frecuencia en minutos del ciclo de refresco automático global.
3. **Límite de Saturación API**: Tope de elementos máximos agregados en un único bloque de mensaje de Telegram.
4. **Gestión del Sueño (Horas Silenciosas)**: Define un rango (Ej: `23:00` a `08:00`). Los mensajes enviados en esta ventana temporal se envían a Telegram con la bandera `disable_notification=true` activa.

---

## 📂 Estructura del Repositorio

```text
.
├── main.py              # FastAPI App: Enrutador, vistas API y lógica de arranque.
├── models.py            # Modelado declarativo SQL e inicialización de base de datos.
├── scheduler.py         # El cerebro: bucle de scrapeo, parseo RSS y cliente Telegram.
├── static/              # Recursos gráficos y estilos CSS Glassmorphic.
├── templates/           # Plantillas HTML Jinja2 (Modularizadas y Responsive).
├── data/                # Volumen persistente (Almacena rsstracker.db).
├── Dockerfile           # Receta de empaquetado de la imagen Python.
├── populate_titles.py   # Script de utilidad para reparar y regenerar títulos y enlaces históricos.
└── docker-compose.yml   # Orquestador de servicios y volúmenes locales.
```

---

## 📜 Licencia

Este proyecto se distribuye bajo licencia de software libre MIT. Eres libre de utilizarlo, modificarlo y desplegarlo en tus servidores domésticos o servidores en la nube.

---

<p align="center">
  Desarrollado con ❤️ y arquitectura asíncrona moderna para optimizar tu flujo de descargas.
</p>
