# Сравнение моделей Cursor

Composer 2.5, Grok 4.5, Opus 4.8, GPT-5.3 Codex, GPT-5.5 — актуально на июль 2026.

| | **Composer 2.5** | **Grok 4.5** | **Opus 4.8** | **GPT-5.3 Codex** | **GPT-5.5** |
|---|---|---|---|---|---|
| **Кто делает** | Cursor (first-party) | Cursor + SpaceXAI (first-party) | Anthropic (third-party) | OpenAI (third-party) | OpenAI (third-party) |
| **Роль** | Компактный coding-специалист | Крупная frontier-модель «для всего на компьютере» | Флагман Anthropic для production-кода и агентов | Флагман OpenAI **только для кода** | Самая «умная» модель GPT-5 для сложного reasoning |
| **Weight class** | Малый / low-latency | Большой (MoE) | Большой (frontier) | Средний / coding-focused | Большой (frontier) |
| **Обучение** | Код, RL в Cursor-like средах | Код + STEM, papers, knowledge work; триллионы токенов Cursor-данных | Универсальный frontier; coding, agents, knowledge work | Agentic coding, terminal/tool use, Codex-окружения | Универсальный frontier; длинные multi-step задачи |
| **Контекст** | Ограниченный (компактная модель) | ~500K токенов | **1M** | **400K** | **1M** (дороже при >272K input) |
| **Сильные стороны** | Быстрые правки, дешёвый дневной драйвер | Длинные агентные задачи, tool use, не только SE | Максимальное качество кода, vision, computer-use | Terminal/agent coding, debugging, ~⅓ цены Opus при сопоставимом качестве | Длинные сессии без раннего «сдаюсь», сильный reasoning |
| **Слабые стороны** | Слабее на длинных/сложных задачах | На части coding-бенчмарков уступает Opus 4.8 | Самый дорогой tier среди coding-флагманов | Стиль кода слабее Opus на architecture-heavy задачах | Дороже Codex; медленнее на простых задачах |
| **Цена (API, in/out за 1M)** | ~$0.50 / $2.50 | $2 / $6 (Fast: $4 / $18) | $5 / $25 (Fast: $10 / $50) | **$1.75 / $14** | **$5 / $30** (Fast: выше) |
| **В Cursor** | First-party pool, дёшево | First-party pool | Premium third-party, Max Mode | API pool, Max Mode | API pool, Max Mode |
| **SWE-Bench Pro** | ~54% | ~65% | **~69%** | ~57% | ~59% |
| **SWE-bench Verified** | — | — | **~89%** | — | **~89%** |
| **SWE-Bench Multilingual** | ~72% | ~78% | **~84%** | — | ~78% |
| **Terminal-Bench** | ~73% (2.1) | ~83% (2.1) | ~75% (2.1) | **~77% (2.0)** | ~83% (2.1) |
| **CursorBench** | ~63% | advantage* | лучше всех Opus | — | ~64% |

\* Cursor отмечает, что Grok 4.5 имел преимущество на CursorBench из‑за случайного попадания snapshot кодовой базы в обучение.

> Бенчмарки с разными версиями (Terminal-Bench 2.0 vs 2.1) и разными сетups (xhigh, effort level) **не всегда напрямую сопоставимы** — используйте как ориентир, не как строгий рейтинг.

## Как читать пять моделей вместе

### First-party (дешевле в included usage)

- **Composer 2.5** — самый дешёвый и быстрый для рутины в репо.
- **Grok 4.5** — middle ground: сильнее Composer, дешевле Opus/GPT-5.5. На Terminal-Bench обгоняет Opus 4.8, но на SWE-Bench Pro/Multilingual — уступает.

### Third-party coding / frontier

- **GPT-5.3 Codex** — лучший выбор OpenAI **именно для кода**: agentic tasks, debugging, terminal. Дешевле GPT-5.5 и Opus при хорошем качестве на большинстве задач.
- **GPT-5.5** — когда нужен максимум «интеллекта» GPT-семейства: длинные multi-step сессии, сложный reasoning, knowledge work. Дороже Codex, но упорнее на незавершённых задачах.
- **Opus 4.8** — эталон качества на SWE-Bench Pro и agentic coding у Anthropic; сильное vision и computer-use. Цена как у GPT-5.5 на input, но output дешевле ($25 vs $30).

### Быстрые ориентиры по цене (input / output)

| Модель | Относительная стоимость |
|---|---|
| Composer 2.5 | Самая дешёвая |
| GPT-5.3 Codex | ~3.5× дороже Composer |
| Grok 4.5 | ~4× дороже Composer |
| Opus 4.8 / GPT-5.5 | ~10× дороже Composer (output GPT-5.5 ещё +20%) |

## Практический выбор

| Задача | Что брать |
|---|---|
| Мелкие правки, автокомплит, быстрые итерации | Composer 2.5 |
| Сложный агентный прогон, бюджет важен | Grok 4.5 |
| Ежедневный coding, debugging, terminal/agent work | GPT-5.3 Codex |
| Длинная сложная сессия, максимум reasoning (GPT) | GPT-5.5 |
| Production-ready код, архитектура, vision, legal/finance | Opus 4.8 |

## Источники

- [Introducing Grok 4.5](https://cursor.com/blog/grok-4-5)
- [Anthropic Claude Opus 4.8](https://www.anthropic.com/claude/opus)
- [GPT-5.3 Codex — Cursor Docs](https://cursor.com/docs/models/gpt-5-3-codex)
- [GPT-5.5 — Cursor Docs](https://cursor.com/docs/models/gpt-5-5)
- [Models & Pricing — Cursor Docs](https://cursor.com/docs/models-and-pricing)
- [Introducing GPT-5.3-Codex — OpenAI](https://openai.com/index/introducing-gpt-5-3-codex/)
