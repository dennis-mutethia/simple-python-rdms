import json
import os
import logging
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SimpleRDBMS")

# Directory to store all database files
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


class Index:
    """
    Simple in-memory index structure.
    Maps a column value to a list of row offsets (positions in the table's row list).
    Used for fast lookups on primary key and unique columns.
    """
    def __init__(self):
        self.index: Dict[Any, List[int]] = {}  # value -> [offset1, offset2, ...]

    def insert(self, value: Any, offset: int) -> None:
        """Add a value-offset mapping."""
        if value not in self.index:
            self.index[value] = []
        self.index[value].append(offset)
        logger.debug(f"Indexed value '{value}' at offset {offset}")

    def search(self, value: Any) -> List[int]:
        """Return all offsets where the value appears."""
        return self.index.get(value, [])

    def delete(self, value: Any, offset: int) -> None:
        """Remove a specific offset for a value."""
        if value in self.index:
            original_count = len(self.index[value])
            self.index[value] = [o for o in self.index[value] if o != offset]
            if not self.index[value]:
                del self.index[value]
            logger.debug(f"Removed offset {offset} for value '{value}' "
                         f"({original_count} → {len(self.index.get(value, []))})")


class Table:
    """
    Represents a single database table with schema, data, indexes, and persistence.
    """

    def __init__(
        self,
        name: str,
        columns: Dict[str, str],
        primary_key: Optional[str] = None,
        unique_cols: Optional[List[str]] = None
    ):
        self.name = name
        self.columns = columns                    # e.g., {"id": "INT", "name": "TEXT"}
        self.column_order = list(columns.keys())  # Preserves insertion order
        self.primary_key = primary_key
        self.unique_cols = unique_cols or []
        self.rows: List[Dict[str, Any]] = []
        self.indexes: Dict[str, Index] = {}
        self.next_offset = 0

        # Initialize indexes for primary key and unique columns
        if primary_key:
            self.indexes[primary_key] = Index()
        for col in self.unique_cols:
            if col not in self.indexes:  # Avoid duplicate if PK is also unique
                self.indexes[col] = Index()

        logger.info(f"Table '{name}' initialized with columns: {list(columns.keys())}")

    def insert(self, values: Dict[str, Any]) -> int:
        """
        Insert a new row into the table.
        Returns the offset (internal row ID) of the inserted row.
        """
        try:
            # Validate all provided columns exist
            for col in values:
                if col not in self.columns:
                    raise ValueError(f"Unknown column: {col}")

            # Create full row with default None for missing columns
            row = {col: values.get(col, None) for col in self.column_order}

            # Enforce primary key and unique constraints
            constrained_cols = ([self.primary_key] if self.primary_key else []) + self.unique_cols
            for col in constrained_cols:
                if col and row[col] is not None:
                    if self.indexes[col].search(row[col]):
                        raise ValueError(f"Duplicate value '{row[col]}' for unique column '{col}'")

            # Append row and update offset
            offset = self.next_offset
            self.rows.append(row)
            self.next_offset += 1

            # Update indexes
            for col, idx in self.indexes.items():
                val = row.get(col)
                if val is not None:
                    idx.insert(val, offset)

            # Persist to disk
            self.save()
            logger.info(f"Inserted row into table '{self.name}' (offset: {offset})")

            return offset

        except Exception as e:
            logger.error(f"Failed to insert into table '{self.name}': {e}")
            raise

    def select(
        self,
        conditions: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Select rows matching optional conditions.
        Returns a list of row dictionaries.
        """
        results = []
        for row in self.rows:
            if conditions is None or all(row.get(k) == v for k, v in conditions.items()):
                results.append(row.copy())
                if limit and len(results) >= limit:
                    break
        logger.debug(f"SELECT on '{self.name}' returned {len(results)} row(s)")
        return results

    def update(self, conditions: Dict[str, Any], updates: Dict[str, Any]) -> int:
        """
        Update rows matching conditions with new values.
        Returns number of updated rows.
        """
        updated_count = 0
        try:
            for i, row in enumerate(self.rows):
                if all(row.get(k) == v for k, v in conditions.items()):
                    # Validate unique constraints before applying update
                    for col in self.unique_cols:
                        if col in updates and updates[col] != row[col]:
                            if self.indexes[col].search(updates[col]):
                                raise ValueError(
                                    f"Update would violate unique constraint on '{col}' "
                                    f"with value '{updates[col]}'"
                                )

                    old_row = row.copy()
                    row.update(updates)

                    # Rebuild index entries for affected indexed columns
                    for col in self.indexes:
                        if col in updates:
                            old_val = old_row.get(col)
                            new_val = row.get(col)
                            if old_val is not None:
                                self.indexes[col].delete(old_val, i)
                            if new_val is not None:
                                self.indexes[col].insert(new_val, i)

                    updated_count += 1

            if updated_count > 0:
                self.save()
                logger.info(f"Updated {updated_count} row(s) in table '{self.name}'")

            return updated_count

        except Exception as e:
            logger.error(f"Failed to update table '{self.name}': {e}")
            raise

    def delete(self, conditions: Dict[str, Any]) -> int:
        """
        Delete rows matching conditions.
        Returns number of deleted rows.
        """
        try:
            to_delete = [(i, row) for i, row in enumerate(self.rows)
                         if all(row.get(k) == v for k, v in conditions.items())]

            deleted_count = len(to_delete)

            # Delete in reverse order to avoid index shifting issues
            for i, row in reversed(to_delete):
                for col in self.indexes:
                    val = row.get(col)
                    if val is not None:
                        self.indexes[col].delete(val, i)
                del self.rows[i]

            if deleted_count > 0:
                self.save()
                logger.info(f"Deleted {deleted_count} row(s) from table '{self.name}'")

            return deleted_count

        except Exception as e:
            logger.error(f"Failed to delete from table '{self.name}': {e}")
            raise

    def save(self) -> None:
        """Persist table schema and data to JSON file."""
        path = os.path.join(DATA_DIR, f"{self.name}.json")
        data = {
            "columns": self.columns,
            "column_order": self.column_order,
            "primary_key": self.primary_key,
            "unique_cols": self.unique_cols,
            "rows": self.rows,
            "next_offset": self.next_offset
        }
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Table '{self.name}' saved to '{path}'")
        except Exception as e:
            logger.error(f"Failed to save table '{self.name}' to disk: {e}")
            raise

    @classmethod
    def load(cls, name: str) -> 'Table':
        """Load a table from its JSON file."""
        path = os.path.join(DATA_DIR, f"{name}.json")
        try:
            with open(path, "r") as f:
                data = json.load(f)

            table = cls(
                name=name,
                columns=data["columns"],
                primary_key=data["primary_key"],
                unique_cols=data["unique_cols"]
            )
            table.rows = data["rows"]
            table.next_offset = data["next_offset"]

            # Rebuild indexes from loaded rows
            indexed_cols = ([table.primary_key] if table.primary_key else []) + table.unique_cols
            for col in indexed_cols:
                if col:
                    table.indexes[col] = Index()
                    for offset, row in enumerate(table.rows):
                        val = row.get(col)
                        if val is not None:
                            table.indexes[col].insert(val, offset)

            logger.info(f"Table '{name}' loaded from '{path}' with {len(table.rows)} rows")
            return table

        except FileNotFoundError:
            logger.error(f"Table file not found: {path}")
            raise
        except Exception as e:
            logger.error(f"Failed to load table '{name}': {e}")
            raise


class Database:
    """
    Main database object managing multiple tables and metadata.
    Supports lazy loading and persistence.
    """

    def __init__(self, name: str):
        self.name = name
        self.tables: Dict[str, Table] = {}
        self.meta_path = os.path.join(DATA_DIR, f"{name}_meta.json")
        logger.info(f"Database '{name}' initialized")

    def create_table(
        self,
        name: str,
        columns: Dict[str, str],
        primary_key: Optional[str] = None,
        unique: Optional[List[str]] = None
    ) -> Table:
        """Create a new table and persist its structure."""
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists in memory")

        try:
            table = Table(name, columns, primary_key, unique)
            self.tables[name] = table
            self._save_meta()
            table.save()
            logger.info(f"Table '{name}' created successfully")
            return table
        except Exception as e:
            logger.error(f"Failed to create table '{name}': {e}")
            raise

    def get_table(self, name: str) -> Table:
        """Retrieve a table, loading from disk if not in memory."""
        if name not in self.tables:
            try:
                self.tables[name] = Table.load(name)
                self._save_meta()  # Update meta in case new table was created externally
            except FileNotFoundError:
                raise ValueError(f"Table '{name}' not found")
            except Exception as e:
                logger.error(f"Error loading table '{name}': {e}")
                raise
        return self.tables[name]

    def _save_meta(self) -> None:
        """Save list of known tables to metadata file."""
        data = {"tables": list(self.tables.keys())}
        try:
            with open(self.meta_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Database metadata saved: {self.meta_path}")
        except Exception as e:
            logger.error(f"Failed to save database metadata: {e}")
            raise

    @classmethod
    def load(cls, name: str) -> 'Database':
        """Load or create a database instance (metadata loaded if exists)."""
        db = cls(name)
        if os.path.exists(db.meta_path):
            try:
                with open(db.meta_path) as f:
                    data = json.load(f)
                logger.info(f"Database '{name}' metadata loaded")
                # Tables will be loaded lazily via get_table()
            except Exception as e:
                logger.warning(f"Could not load metadata for '{name}': {e}")
        return db

    def join(self, left_table: str, right_table: str, on: str) -> List[Dict[str, Any]]:
        """
        Perform a simple INNER JOIN on two tables using a single equality condition.
        Format for 'on': "left_col = right_col" or "table.col = table.col"
        Handles type coercion for INT columns.
        """
        try:
            left = self.get_table(left_table)
            right = self.get_table(right_table)

            if "=" not in on:
                raise ValueError("JOIN condition must contain '='")

            left_on, right_on = [part.strip() for part in on.split("=", 1)]
            # Support optional table prefix: users.id or just id
            left_col = left_on.split(".")[-1]
            right_col = right_on.split(".")[-1]

            left_col_type = left.columns.get(left_col)
            right_col_type = right.columns.get(right_col)

            results = []

            for lrow in left.rows:
                lval = lrow.get(left_col)
                for rrow in right.rows:
                    rval = rrow.get(right_col)

                    # Type coercion for comparison
                    if lval is None or rval is None:
                        if lval == rval:  # both None
                            results.append(self._merge_rows(left_table, lrow, right_table, rrow))
                        continue

                    # Coerce to int if either column is INT
                    if left_col_type == "INT" or right_col_type == "INT":
                        try:
                            lval_cmp = int(lval)
                            rval_cmp = int(rval)
                        except (ValueError, TypeError):
                            continue  # can't compare as int
                    else:
                        lval_cmp = lval
                        rval_cmp = rval

                    if lval_cmp == rval_cmp:
                        results.append(self._merge_rows(left_table, lrow, right_table, rrow))

            logger.info(f"JOIN completed: {len(results)} row(s) from {left_table} ⋈ {right_table}")
            return results

        except Exception as e:
            logger.error(f"JOIN failed between {left_table} and {right_table}: {e}")
            raise

    def _merge_rows(self, left_table: str, lrow: dict, right_table: str, rrow: dict) -> dict:
        """Helper to merge two rows with table prefixes."""
        merged = {f"{left_table}_{k}": v for k, v in lrow.items()}
        merged.update({f"{right_table}_{k}": v for k, v in rrow.items()})
        return merged