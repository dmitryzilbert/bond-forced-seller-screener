# Bond Forced Seller Screener (MVP)

Асинхронный mock MVP для поиска вынужденных продавцов облигаций на рынке РФ. Приложение работает в mock-режиме без токенов, прогоняя поток обновлений стакана из фикстур и выдавая события в дашборд и логируемые Telegram-уведомления.

## Быстрый старт (mock mode)

1. Скопируйте `.env.example` в `.env` и оставьте `app_env=mock`. Токены не требуются.
2. Запустите контейнеры: `docker-compose up -d`
3. Выполните CLI: `make run` (или `poetry run app run` если используете свой env).
4. Откройте дашборд: http://localhost:8000

## Local dev (pip install -e)

1. Создайте окружение: `python -m venv .venv` и активируйте его (`source .venv/bin/activate` или `.venv\Scripts\activate`).
2. Установите зависимости для разработки: `pip install -e ".[dev]"`.
3. Скопируйте пример env: `cp .env.example .env` и установите `app_env=mock`.
4. Запустите приложение: `python -m app.main`.
5. Запустите тесты: `pytest -q`.
6. Проверьте загрузку env:

```bash
python -c "from app.settings import Settings; s=Settings(); print(s.app_env, bool(s.tinvest_token), bool(s.telegram_bot_token), s.telegram_chat_id)"
```

## Prod mode (реальный поток T-Invest)

1. Задайте окружение: `app_env=prod`, `tinvest_token=<tinkoff_api_token>` (readonly достаточно), при необходимости `orderbook_depth`, `database_url`.
2. Поток стаканов использует gRPC MarketDataServerSideStream (с fallback на MarketDataStream).
   Все подписки выполняются через `instrument_id` (предпочтительно `instrument_uid`).
3. Любое сообщение из стрима (включая служебные ACK/ping) обновляет heartbeat.
   В SDK `t_tech.invest.grpc` может отсутствовать `PingDelaySettings`, поэтому код не должен его требовать.
4. Запустите миграции/БД: `make db` (создаст SQLite по умолчанию) или задайте свой Postgres URL.
5. Стартуйте сервисы: `docker-compose up -d` и `make run`.
6. Дашборд и API будут использовать реальные инструменты и стаканы через REST/gRPC T-Invest.

## Переменные окружения
См. `.env.example`. К ключевым параметрам добавлены `TINVEST_TOKEN`, `TINVEST_ACCOUNT_ID` (опционально для лимитов), `TINVEST_GRPC_TARGET_PROD`, `TINVEST_GRPC_TARGET_SANDBOX`, `ORDERBOOK_DEPTH`.
Для gRPC SSL:
- `TINVEST_SSL_CA_BUNDLE` — путь к PEM-бандлу для корпоративного прокси/Windows.
Для MVP, чтобы shortlist не опустел из-за пропусков данных, можно включить `allow_missing_data_to_shortlist=true` (алерты по таким инструментам подавляются настройками `suppress_alerts_when_missing_data` и `suppress_alerts_when_offer_unknown`).

## Тесты

```bash
make test
```

## Архитектура

- `app/main.py` — точка входа, поднимает FastAPI и воркер обработки потока (mock или prod каркас).
- `app/services` — оркестрация загрузки инструментов, подписок на стаканы и сохранение событий.
- `app/domain` — расчёт YTM, скоринг, стресс и детектор кандидатов.
- `app/web` — дашборд и JSON API.
- `app/adapters` — клиенты T-Invest (REST/stream) и Telegram (mock/реальный).
- `app/storage` — SQLAlchemy ORM, репозитории и миграции Alembic.
- `app/cli` — Typer CLI с командами run/shortlist/backtest.

### Ask window notional

Notional в AskVolWindow трактуется как денежная стоимость в стакане: `lots * nominal * price_percent / 100`.
Это определение зафиксировано в детекторе и используется во всех проверках и метриках.

## Prod каркас

Prod-режим содержит заглушки клиентов T-Invest и может быть расширен при наличии токенов. Все параметры управляются через env.
