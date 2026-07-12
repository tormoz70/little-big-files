# Сравнение моделей Cursor: Composer 2.5, Grok 4.5, Opus 4.8

| | **Composer 2.5** | **Grok 4.5** | **Opus 4.8** |
|---|---|---|---|
| **Кто делает** | Cursor (first-party) | Cursor + SpaceXAI (first-party) | Anthropic (third-party) |
| **Роль** | Компактный coding-специалист | Крупная frontier-модель «для всего на компьютере» | Флагман Anthropic для production-кода и агентов |
| **Weight class** | Малый / low-latency | Большой (MoE) | Большой (frontier) |
| **Обучение** | Код, RL в Cursor-like средах | Код + STEM, papers, knowledge work; триллионы токенов Cursor-данных | Универсальный frontier; сильный упор на coding, agents, knowledge work |
| **Контекст** | Ограниченный (компактная модель) | ~500K токенов | **1M токенов** |
| **Сильные стороны** | Быстрые правки, дешёвый дневной драйвер | Длинные агентные задачи, tool use, не только SE | Максимальное качество кода, vision, computer-use, legal/finance |
| **Слабые стороны** | Слабее на длинных/сложных задачах | На части coding-бенчмарков уступает Opus 4.8 | Самый дорогой из трёх; расходует quota быстрее |
| **Цена (API, in/out за 1M)** | ~$0.50 / $2.50 | $2 / $6 (Fast: $4 / $18) | **$5 / $25** (Fast: $10 / $50) |
| **В Cursor** | First-party pool, дёшево | First-party pool | Premium third-party |
| **SWE-Bench Pro** | ~54% | ~65% | **~69%** |
| **SWE-bench Verified** | — | — | **~89%** |
| **SWE-Bench Multilingual** | ~72% | ~78% | **~84%** |
| **Terminal-Bench 2.1** | ~73% | ~83% | **~75%** |
| **CursorBench** | ниже | advantage* | **лучше всех Opus** |

\* Cursor отмечает, что Grok 4.5 имел преимущество на CursorBench из‑за случайного попадания snapshot кодовой базы в обучение.

## Как читать три модели вместе

- **Composer 2.5** — самый дешёвый и быстрый для рутины в репо.
- **Grok 4.5** — middle ground по цене; сильнее Composer, дешевле Opus (~2.5× по input, ~4× по output). На Terminal-Bench обгоняет Opus 4.8, но на SWE-Bench Pro/Multilingual — уступает.
- **Opus 4.8** — эталон качества среди трёх: лучший на «тяжёлых» coding-бенчмарках, 1M контекст, сильное vision и computer-use. Платите ценой и расходом quota.

## Практический выбор

| Задача | Что брать |
|---|---|
| Мелкие правки, автокомплит, быстрые итерации | Composer 2.5 |
| Сложный агентный прогон, но бюджет важен | Grok 4.5 |
| Production-ready код, архитектура, vision, максимальное качество | Opus 4.8 |

## Источники

- [Introducing Grok 4.5](https://cursor.com/blog/grok-4-5)
- [Anthropic Claude Opus 4.8](https://www.anthropic.com/claude/opus)
