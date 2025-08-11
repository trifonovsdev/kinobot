<div align="center">

# KinoBot

Современный и простой проект для залива трафика.

[![Python](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.x-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Aiogram3](https://img.shields.io/badge/aiogram-3.x-2CA5E0?logo=telegram&logoColor=white)](https://docs.aiogram.dev/)

</div>

KinoBot — состоит из: веб‑админка (FastAPI + Socket.IO) и бот (Aiogram3). Поддерживаются импорт из TMDb, загрузка постеров, базовая статистика, поиск и управление пользователями.

— Быстрый старт —

1) Клонировать репозиторий

```bash
git clone https://github.com/trifonovsdev/kinobot
cd kinobot
```

2) Установить зависимости

macOS / Linux:
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Windows:
```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

3) Создать `.env` в корне (пример ниже) и запустить

```bash
python main.py
# Админка: http://localhost:5555
```

Содержание

- Возможности
- Конфигурация (.env)
- Авто‑обновление
- Установка и запуск (подробно)
- Частые вопросы

Возможности

- Админ‑панель (FastAPI + Jinja2), уведомления Socket.IO
- Импорт из TMDb: поиск по названию/ID, популярное, защита от дублей, загрузка постеров
- Работа с фильмами: добавление/редактирование, коды и обложки
- Пользователи: роли (траффер/юзер), бан/разбан
- Уведомление об обновлении и кнопка «Обновить сейчас» в админке
- Telegram‑бот: поиск по коду, подбор по жанру, реферальная система

Доступ в админ‑панель

- Логин: root
- Пароль: root
- Изменение логики логина: `app/web/app.py`

Конфигурация env

Создайте файл `.env` в корне проекта и заполните необходимые параметры:

```bash
# Телеграм‑бот
BOT_TOKEN=0              # Токен вашего бота

# Веб‑сервер
HOST=0.0.0.0             # Адрес прослушивания
PORT=5555                # Порт
SECRET_KEY=01c4041d20caa191

UPLOAD_FOLDER=static/uploads

# TMDb
TMDB_API_KEY=0           # Ключ API v3 (если будете использовать авто-залив)
TMDB_LANGUAGE=ru-RU
TMDB_IMAGE_BASE=https://image.tmdb.org/t/p

# Обновления
AUTO_UPDATE=1            # Включить проверку/предложение обновления при старте
```

Авто‑обновление

- При запуске бота, происходит проверка обновлений, при успехе предлагается обновиться (y/n)
- По желанию авто-обновление можно отключить указав в .env `AUTO_UPDATE=0`

Установка и запуск

macOS / Linux

```bash
# 1) Зависимости
python -m pip install --upgrade pip
pip install -r requirements.txt

# 2) .env
# Создайте .env (см. раздел «Конфигурация» выше)

# 3) Запуск
python main.py
```

Windows

```powershell
# 1) Зависимости
python -m pip install --upgrade pip
pip install -r requirements.txt

# 2) .env
# Создайте .env (см. раздел «Конфигурация» выше)

# 3) Запуск
python main.py
```

Возможные проблемы

- `TMDB_API_KEY не задан в .env` — зарегайтесь в TMDb и получите апикей, заполните `.env` и перезапустите.

MIT License

Copyright (c) 2025 TRIFONOVSDEV

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
