# Bond Forced Seller Screener (MVP)

Асинхронный mock MVP для поиска вынужденных продавцов облигаций на рынке РФ. Приложение работает в mock-режиме без токенов, прогоняя поток обновлений стакана из фикстур и выдавая события в дашборд и логируемые Telegram-уведомления.

## Быстрый старт (mock mode)

1. Скопируйте `.env.example` в `.env` и оставьте `APP_ENV=mock`. Токены не требуются.
2. Запустите контейнеры: `docker-compose up -d`
3. Выполните CLI: `make run` (или `poetry run app run` если используете свой env).
4. Откройте дашборд: http://localhost:8000

## Prod mode (реальный поток T-Invest)

1. Задайте окружение: `APP_ENV=prod`, `TINVEST_TOKEN=<tinkoff_api_token>` (достаточно readonly), при необходимости `ORDERBOOK_DEPTH` и `DATABASE_URL`.
2. Запустите миграции/БД: `make db` (создаст SQLite по умолчанию) или задайте свой Postgres URL.
3. Стартуйте сервисы: `docker-compose up -d` и `make run`.
4. Дашборд и API будут использовать реальные инструменты и стаканы через REST/WebSocket T-Invest.

## Переменные окружения
См. `.env.example`. К ключевым параметрам добавлены `TINVEST_TOKEN`, `TINVEST_ACCOUNT_ID` (опционально для лимитов), `ORDERBOOK_DEPTH`.

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
