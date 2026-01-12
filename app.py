import logging
from flask import Flask, render_template, request, redirect, url_for
from database import Database

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SimpleRDBMS")

app = Flask(__name__)
db = Database("local_db")

# Setup tables (idempotent)
try:
    users = db.create_table(
        "users",
        columns={"id": "INT", "username": "TEXT", "email": "TEXT"},
        primary_key="id",
        unique=["username"]
    )
    todos = db.create_table(
        "todos",
        columns={"id": "INT", "task": "TEXT", "done": "BOOLEAN", "user_id": "INT"},
        primary_key="id"
    )
except Exception as e:
    logger.info(f"Tables likely already exist: {e}")
    users = db.get_table("users")
    todos = db.get_table("todos")

# Insert demo data if no users exist
if not users.select():
    logger.info("Inserting demo data...")
    users.insert({"id": 1, "username": "alice", "email": "alice@example.com"})
    users.insert({"id": 2, "username": "bob", "email": "bob@example.com"})
    todos.insert({"id": 1, "task": "Learn RDBMS", "done": False, "user_id": 1})
    todos.insert({"id": 2, "task": "Build project", "done": True, "user_id": 1})
    todos.insert({"id": 3, "task": "Deploy app", "done": False, "user_id": 2})


@app.route("/")
def index():
    todos_list = todos.select()
    users_list = users.select()
    return render_template("index.html", todos=todos_list, users=users_list)


@app.route("/", methods=["POST"])
def add():
    task = request.form.get("task")
    if not task:
        return redirect(url_for("index"))

    max_id = max((t["id"] for t in todos.select()), default=0)
    todos.insert({
        "id": max_id + 1,
        "task": task.strip(),
        "done": False,
        "user_id": 1  # Hardcoded to Alice for demo
    })
    logger.info(f"New todo added: {task}")
    return redirect(url_for("index"))


@app.route("/toggle/<int:todo_id>")
def toggle(todo_id):
    todo = next((t for t in todos.select() if t["id"] == todo_id), None)
    if todo:
        new_status = not todo["done"]
        todos.update({"id": todo_id}, {"done": new_status})
        logger.info(f"Todo {todo_id} toggled to {'done' if new_status else 'pending'}")
    return redirect(url_for("index"))


@app.route("/delete/<int:todo_id>")
def delete(todo_id):
    deleted = todos.delete({"id": todo_id})
    if deleted:
        logger.info(f"Todo {todo_id} deleted")
    return redirect(url_for("index"))


if __name__ == "__main__":
    print("🌟 Simple RDBMS Todo App is running!")
    print("👉 Open http://localhost:5000 in your browser")
    app.run(debug=True)