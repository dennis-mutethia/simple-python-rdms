import re
import logging
from database import Database

# Use the shared logger from database.py
logger = logging.getLogger("SimpleRDBMS")

# Global database instance
db = Database("local_db")


def _parse_value(value_str: str, col_type: str):
    """
    Convert a raw string value from SQL to the appropriate Python type
    based on the column's declared type.
    """
    v = value_str.strip().strip("'\"")
    if col_type == "INT":
        try:
            return int(v)
        except ValueError:
            raise ValueError(f"Cannot convert '{v}' to INT for column of type INT")
    elif col_type == "BOOLEAN":
        v_lower = v.lower()
        if v_lower in ("true", "1", "yes"):
            return True
        elif v_lower in ("false", "0", "no"):
            return False
        else:
            raise ValueError(f"Invalid BOOLEAN value: '{v}'")
    else:  # TEXT or unknown
        return v


def parse_create(sql: str) -> None:
    """
    Parse and execute CREATE TABLE statement.
    Supports: column definitions, optional PRIMARY KEY (col), UNIQUE (col1, col2, ...)
    """
    pattern = r"""
        CREATE\s+TABLE\s+(\w+)\s*                  # Table name
        \(\s*([^\)]+?)\s*\)                         # Column list (non-greedy)
        (?:\s*PRIMARY\s+KEY\s*\(\s*(\w+)\s*\))?     # Optional PK
        (?:\s*UNIQUE\s*\(\s*([^)]+)\s*\))?          # Optional UNIQUE columns
    """

    try:
        match = re.match(pattern, sql.strip(), re.IGNORECASE | re.VERBOSE)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")

        table_name = match.group(1)
        cols_part = match.group(2).strip()
        pk = match.group(3)
        unique_part = match.group(4)

        # Parse column definitions
        columns = {}
        for col_def in cols_part.split(","):
            col_def = col_def.strip()
            if not col_def:
                continue
            parts = col_def.split()
            if len(parts) != 2:
                raise ValueError(f"Invalid column definition: '{col_def}'. Expected 'name TYPE'")
            name, typ = parts
            if typ.upper() not in {"INT", "TEXT", "BOOLEAN"}:
                raise ValueError(f"Unsupported data type: {typ}")
            columns[name] = typ.upper()

        # Parse UNIQUE constraint columns
        unique = [col.strip() for col in unique_part.split(",") if col.strip()] if unique_part else []

        db.create_table(table_name, columns, primary_key=pk, unique=unique)
        print(f"Table `{table_name}` created successfully with {len(columns)} columns.")

    except ValueError as ve:
        logger.error("CREATE error: %s", ve)
    except Exception as e:
        logger.error("Unexpected error during CREATE TABLE: %s", e)


def parse_insert(sql: str) -> None:
    """
    Parse and execute INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
    Performs proper type conversion based on column types.
    """
    pattern = r"INSERT INTO (\w+)\s*\((.+)\)\s*VALUES\s*\((.+)\)"

    try:
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT syntax")

        table_name, cols_part, vals_part = match.groups()
        col_names = [c.strip() for c in cols_part.split(",")]
        val_strings = [v.strip() for v in vals_part.split(",")]

        if len(col_names) != len(val_strings):
            raise ValueError("Number of columns does not match number of values")

        table = db.get_table(table_name)

        # Convert values to correct types
        values = {}
        for col, val_str in zip(col_names, val_strings):
            col_type = table.columns.get(col)
            if col_type is None:
                raise ValueError(f"Unknown column: {col}")
            values[col] = _parse_value(val_str, col_type)

        table.insert(values)
        print(f"Row inserted into table `{table_name}`")

    except ValueError as ve:
        logger.error("INSERT error: %s", ve)
    except Exception as e:
        logger.error("Failed to insert row: %s", e)


def parse_where_clause(where_clause: str) -> dict:
    """Parse simple WHERE clause with AND-separated conditions."""
    if not where_clause:
        return {}

    conditions = {}
    try:
        for cond in where_clause.split(" AND "):
            if "=" not in cond:
                raise ValueError(f"Condition missing '=': {cond}")
            key, val_str = [part.strip() for part in cond.split("=", 1)]
            conditions[key] = val_str  # Keep raw string; type conversion in SELECT
        return conditions
    except Exception as e:
        raise ValueError(f"Invalid WHERE clause: {e}")


def parse_select(sql: str) -> None:
    """
    Parse and execute SELECT * FROM table [WHERE conditions]
    Supports basic equality conditions with AND.
    """
    pattern = r"SELECT \* FROM (\w+)(?:\s+WHERE\s+(.+))?"

    try:
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid SELECT syntax")

        table_name = match.group(1)
        where_raw = match.group(2)

        table = db.get_table(table_name)
        raw_conditions = parse_where_clause(where_raw) if where_raw else {}

        # Convert condition values to proper types
        conditions = {}
        for col, val_str in raw_conditions.items():
            col_type = table.columns.get(col)
            if col_type is None:
                raise ValueError(f"Unknown column in WHERE clause: {col}")
            conditions[col] = _parse_value(val_str, col_type)

        rows = table.select(conditions)

        if rows:
            headers = " | ".join(rows[0].keys())
            print(headers)
            print("-" * max(len(headers), 60))
            for row in rows:
                print(" | ".join(str(v) if v is not None else "NULL" for v in row.values()))
            print(f"{len(rows)} row(s) returned")
        else:
            print("No rows found.")

    except ValueError as ve:
        logger.error("SELECT error: %s", ve)
    except Exception as e:
        logger.error("Failed to execute SELECT: %s", e)


def parse_join(sql: str) -> None:
    """
    Parse and execute SELECT * FROM table1 JOIN table2 ON condition
    Uses the improved join() from database.py with type coercion.
    """
    pattern = r"SELECT \* FROM (\w+) JOIN (\w+) ON (.+)"

    try:
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid JOIN syntax")

        left_table, right_table, on_clause = match.groups()
        results = db.join(left_table, right_table, on_clause.strip())

        if results:
            headers = " | ".join(results[0].keys())
            print(headers)
            print("-" * max(len(headers), 80))
            for row in results:
                print(" | ".join(str(v) if v is not None else "NULL" for v in row.values()))
            print(f"{len(results)} row(s) returned from JOIN")
        else:
            print("No matching rows from JOIN.")

    except ValueError as ve:
        logger.error("JOIN error: %s", ve)
    except Exception as e:
        logger.error("Failed to execute JOIN: %s", e)


def repl() -> None:
    """Main interactive REPL loop for the simple RDBMS."""
    print("=" * 50)
    print("Simple RDBMS REPL Started")
    print("Supported commands:")
    print("  CREATE TABLE ...")
    print("  INSERT INTO ... VALUES ...")
    print("  SELECT * FROM table [WHERE ...]")
    print("  SELECT * FROM t1 JOIN t2 ON ...")
    print("Type 'exit' to quit")
    print("=" * 50)

    while True:
        try:
            command = input("sql> ").strip()

            if not command:
                continue
            if command.lower() == "exit":
                print("Goodbye! Session ended.")
                break

            upper_cmd = command.upper()

            if upper_cmd.startswith("CREATE TABLE"):
                parse_create(command)
            elif upper_cmd.startswith("INSERT INTO"):
                parse_insert(command)
            elif "JOIN" in upper_cmd and upper_cmd.startswith("SELECT"):
                parse_join(command)
            elif upper_cmd.startswith("SELECT"):
                parse_select(command)
            else:
                logger.warning("Unsupported or unrecognized command.")

        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Goodbye!")
            break
        except EOFError:
            print("\nGoodbye!")
            break
        except Exception as e:
            logger.error("Unexpected error in REPL: %s", e)


if __name__ == "__main__":
    repl()