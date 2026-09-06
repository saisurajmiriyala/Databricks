# 🧰 Databricks Toolkit

Reusable Python utilities and accelerators for Databricks developers — built to cut boilerplate and add observability to everyday data engineering work.

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Lakebase-336791?logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 📦 What's Inside

| Module | Purpose |
|---|---|
| [`lakebase-utils`](#-1-lakebase-utils) | Import-ready Python functions for building data engineering pipelines on **Postgres / Lakebase** |
| [`Notebooklogger`](#-2-notebooklogger) | One-line, drop-in logger that captures a detailed action trail for **any Databricks notebook** |

---

## 🐘 1. `lakebase-utils`

A lightweight utility package that gives data engineers **ready-to-import Python functions** for building pipelines against Postgres and Databricks Lakebase — so you're not rewriting connection handling, query execution, and pipeline scaffolding for every new project.

### Why use it
- ⚡ Skip boilerplate connection/session setup for Postgres & Lakebase
- 🔁 Reusable functions across projects instead of copy-pasting utility code
- 🧩 Designed to slot directly into existing Databricks notebooks or jobs

### Quick Start

```python
from lakebase_utils.postgres.connect import ptk_execute, ptk_read_dataframe, ptk_write_dataframe

# Connect once
conn = ptk_connect(host="...", database="...", credentials="...")

# Build pipeline steps using shared utilities
df = ptk_execute("SELECT * FROM raw_events")
```

> 💡 Replace the example above with your actual public function names/signatures once finalized — happy to update this once you share the real API.

### Project structure
```
lakebase-utils/
└── src/
    └── lakebase_utils/
        └── postgres/
            ├── __init__.py
            ├── connection.py     # connection & session handling
            └── pipeline.py       # reusable pipeline building blocks
```

---

## 📝 2. `Notebooklogger`

A **universal logging accelerator** for Databricks notebooks. One function call gives every developer on the team automatic, detailed visibility into what a notebook actually did — no manual `print()` statements or custom logging setup required.

### Why use it
- 🪄 **One line to activate** — no logging framework to configure
- 🔍 Captures a detailed trail of notebook actions for debugging, audits, and reproducibility
- 👥 Standardizes logging across every developer's notebooks — consistent format, zero setup cost
- 🧵 Works globally across the notebook session once started

### Quick Start

```python
from notebooklogger import start_global_notebook_logger

# Start it once at the top of your notebook
start_global_notebook_logger()

# ...continue your notebook as normal —
# every action is now being logged automatically
```

That's it. Every cell execution and action from this point forward is captured in a structured log for the session.

### Example use cases
- 🐛 Debugging failed pipeline runs without re-running from scratch
- 📜 Producing an audit trail for regulated/enterprise data workflows
- 👀 Onboarding new engineers by showing them exactly what a notebook did, step by step

---

## 🗂️ Repo Structure

```
Databricks/
├── Notebooklogger/
│   └── ...                       # universal notebook logging accelerator
├── lakebase-utils/
│   └── src/
│       └── lakebase_utils/
│           └── postgres/         # Postgres/Lakebase pipeline utilities
└── README.md
```

---

## 🚀 Getting Started

```bash
git clone https://github.com/saisurajmiriyala/Databricks.git
cd Databricks
```

Then import whichever module you need directly into your notebook or pipeline code.

---

## 🛣️ Roadmap
- [ ] Add PyPI packaging for both modules
- [ ] Add unit tests
- [ ] Add usage examples notebook
- [ ] Add log export (JSON/Delta table) for `Notebooklogger`

---

## 🤝 Contributing
Issues and PRs are welcome — this toolkit is built to grow with real-world Databricks engineering needs.

---

## 📄 License
MIT © [Sai Suraj Miriyala](https://www.linkedin.com/in/sai-surajmiriyala-203537103)
