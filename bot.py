# bot.py
#hello
import os
import io
import logging
from urllib.parse import urlparse, parse_qs
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command

# ---------- Конфиг и логирование ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
YT_API_KEY = os.getenv("YT_API_KEY", "")

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")
if not YT_API_KEY:
    logging.warning("⚠️ Не задан YT_API_KEY в .env — парсинг YouTube работать не будет.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("yt-bot")

# ---------- Утилиты для YouTube ----------
def extract_video_id(url: str) -> str:
    """Достаём videoId из ссылки"""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()

    if host == "youtu.be":
        return parsed.path.lstrip("/")
    if host in ("www.youtube.com", "youtube.com", "m.youtube.com"):
        if parsed.path == "/watch":
            qs = parse_qs(parsed.query)
            if "v" in qs and qs["v"]:
                return qs["v"][0]
        if parsed.path.startswith("/embed/"):
            return parsed.path.split("/")[2]
        if parsed.path.startswith("/v/"):
            return parsed.path.split("/")[2]
    raise ValueError("Не удалось извлечь videoId из ссылки")

def get_video_channel_id(video_id: str, api_key: str) -> str:
    """Получаем channelId владельца видео"""
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {"part": "snippet", "id": video_id, "key": api_key}
    r = requests.get(url, params=params, timeout=30).json()

    if "items" not in r or not r["items"]:
        raise ValueError(f"❌ Ошибка при получении видео {video_id}: {r}")
    return r["items"][0]["snippet"]["channelId"]

def parse_comments(video_url: str, api_key: str) -> pd.DataFrame:
    """Парсим треды комментариев и ответы; игнорим автора-канал"""
    video_id = extract_video_id(video_url)
    channel_id = get_video_channel_id(video_id, api_key)

    rows = []
    next_page_token = ""
    pk_counter = 1  # автоинкремент id

    while True:
        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": 50,
            "pageToken": next_page_token,
            "key": api_key
        }
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/commentThreads",
            params=params,
            timeout=30
        ).json()

        if "error" in r:
            raise ValueError(f"❌ API Error: {r['error']['message']}")

        for item in r.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            top_id = pk_counter

            # игнорируем комменты владельца канала
            if top.get("authorChannelId", {}).get("value") != channel_id:
                rows.append({
                    "id": top_id,
                    "link": f"https://www.youtube.com/watch?v={video_id}&lc={item['id']}",
                    "username": top.get("authorDisplayName"),
                    "text": top.get("textDisplay"),
                    "parsed_at": datetime.utcnow().isoformat(),
                    "created_at": top.get("publishedAt"),
                    "parent_id": None
                })
                pk_counter += 1

            # ответы
            if "replies" in item:
                for reply in item["replies"]["comments"]:
                    r_snip = reply["snippet"]
                    if r_snip.get("authorChannelId", {}).get("value") == channel_id:
                        continue
                    rows.append({
                        "id": pk_counter,
                        "link": f"https://www.youtube.com/watch?v={video_id}&lc={reply['id']}",
                        "username": r_snip.get("authorDisplayName"),
                        "text": r_snip.get("textDisplay"),
                        "parsed_at": datetime.utcnow().isoformat(),
                        "created_at": r_snip.get("publishedAt"),
                        "parent_id": top_id
                    })
                    pk_counter += 1

        next_page_token = r.get("nextPageToken", "")
        if not next_page_token:
            break

    df = pd.DataFrame(rows, columns=[
        "id", "link", "username", "text", "parsed_at", "created_at", "parent_id"
    ])
    if df.empty:
        # Пустой каркас с нужными типами
        df = pd.DataFrame(columns=[
            "id", "link", "username", "text", "parsed_at", "created_at", "parent_id"
        ])

    # делаем parent_id nullable integer
    if "parent_id" in df.columns:
        df["parent_id"] = df["parent_id"].astype("Int64")

    return df

# ---------- Детектор платформы ----------
def detect_platform(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return "unknown"

    if any(h in (host or "") for h in ["youtube.com", "youtu.be", "m.youtube.com"]):
        return "youtube"
    return "unknown"

# ---------- Телеграм-бот ----------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Отправь мне ссылку. Я определю платформу и, если это YouTube, "
        "спаршу комментарии и пришлю Excel-файл.\n\n"
        "Пример: https://www.youtube.com/watch?v=B9oIps6Cb50"
    )

@dp.message(F.text)
async def handle_link(message: Message):
    url = (message.text or "").strip()
    if not url.startswith("http"):
        await message.answer("Отправь, пожалуйста, корректную ссылку (начинается с http/https).")
        return

    platform = detect_platform(url)
    if platform != "youtube":
        await message.answer("Пока поддерживаю только YouTube. Отправь ссылку на видео YouTube.")
        return

    if not YT_API_KEY:
        await message.answer("YT_API_KEY не настроен на сервере. Добавь его в .env и перезапусти бота.")
        return

    await message.answer("Паршу комментарии… Это может занять немного времени.")

    try:
        # Парсинг в отдельном потоке, чтобы не блокировать loop
        import asyncio
        df = await asyncio.to_thread(parse_comments, url, YT_API_KEY)

        # Готовим Excel в память
        buffer = io.BytesIO()
        # sheet_name короткий, index=False чтобы не мешать
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="comments")
        buffer.seek(0)

        # Имя файла
        safe_name = "youtube_comments.xlsx"
        doc = BufferedInputFile(buffer.read(), filename=safe_name)

        caption = (
            f"Готово ✅\n"
            f"Всего строк: {len(df)}\n"
            f"Столбцы: {', '.join(df.columns)}"
        )
        await message.answer_document(document=doc, caption=caption)

    except ValueError as ve:
        logger.exception("ValueError")
        await message.answer(f"Ошибка: {ve}")
    except Exception as e:
        logger.exception("Unexpected error")
        await message.answer("Произошла непредвиденная ошибка при парсинге. Попробуй другую ссылку или позже.")

# ---------- Запуск ----------
def main():
    import asyncio
    asyncio.run(dp.start_polling(bot))

if __name__ == "__main__":
    main()
