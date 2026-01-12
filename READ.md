# Simple Python RDBMS

A **Complete, Simple Relational Database Management System** built entirely in pure Python — no external databases required!

This project implements core RDBMS features from scratch, making it perfect for:
- Learning database internals
- Demonstrating CRUD, constraints, indexing, and joins

## Features
- **Table creation** with column types: `INT`, `TEXT`, `BOOLEAN`
- **Primary Key** and **UNIQUE** constraints with enforcement
- **Simple in-memory indexing** (hash-based) on PK and unique columns
- **Full CRUD operations** (Create, Read, Update, Delete)
- **Simple INNER JOIN** with proper type coercion (handles INT vs string comparisons)
- **Interactive SQL-like REPL** with robust parsing and error handling
- **Full persistence** to disk using JSON files (data survives restarts!)
- **Beautiful Flask web demo** with Tailwind CSS UI
- **Professional logging** throughout (no more `print` statements)
- **Type-safe value conversion** for INSERT and WHERE clauses

## Project Structure

```
simple_rdbms/
├── database.py
├── repl.py
├── app.py
├── templates/
│   └── index.html
├── tests/
│   ├── __init__.py
│   ├── test_database.py
│   └── test_repl.py          # Optional: integration tests for parser
└── data/                     # Auto-created
```

## 🚀 Development with DevContainer (Recommended)

This project includes full **VS Code DevContainer** support for a consistent, zero-setup development environment.

### How to Use

1. Install:
   - [VS Code](https://code.visualstudio.com/)
   - [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

2. Open the project in VS Code:
   ```bash
   code simple_rdbms

3. When prompted, click "Reopen in Container"
(or use Command Palette → "Dev Containers: Rebuild and Reopen in Container")

4. Once ready:
Open a terminal in VS Code

## How to Run

### 1. Interactive REPL

```bash
python repl.py
```

**Supported commands:**

```sql
CREATE TABLE users (id INT, username TEXT, email TEXT) PRIMARY KEY (id) UNIQUE (username)

INSERT INTO users (id, username, email) VALUES (1, 'bob', 'bob@example.com')
INSERT INTO users (id, username, email) VALUES (2, 'Alice', 'alice@gmail.com')

SELECT * FROM users


CREATE TABLE todos (id INT, task TEXT, done BOOLEAN, user_id INT) PRIMARY KEY (id)

INSERT INTO todos (id, task, done, user_id) VALUES (1, 'Study RDBMS', False, 1)
INSERT INTO todos (id, task, done, user_id) VALUES (2, 'Build project', True, 2)

SELECT * FROM users JOIN todos ON users.id = todos.user_id

-- Type 'exit' to quit
```

All data is **automatically saved** to `data/` and loaded on restart.

### 2. Web Demo (Beautiful Todo App)

```bash
python app.py
```

Then open your browser to: [http://localhost:5000](http://localhost:5000)

Features:
- Modern, responsive design using **Tailwind CSS**
- Add, toggle, and delete todos
- View all users in a stylish sidebar
- Full persistence — refresh or restart, your data remains!

## Example Data Flow

1. Start the web app → creates `users` and `todos` tables if needed
2. Inserts demo data (Alice & Bob + sample todos)
3. Use the UI to add/modify todos
4. Close and restart → all your changes are still there (thanks to JSON persistence!)

## Why This Project Rocks

- 100% pure Python — no dependencies beyond Flask (for the web demo)
- Real implementation of key database concepts
- Clean, commented, production-style code with logging and error handling
- Practical demonstration via both CLI and web interface

## Author

Dennis Muga
dennis@gmail.com
+254 105 565 532
Nairobi, Kenya

---

**Built with passion to learn how databases really work under the hood.**

Enjoy exploring your own RDBMS! 🚀

