import unittest
import os
import shutil
from database import Database, Table

TEST_DB_NAME = "test_db_unit"
DATA_DIR = "data"

class TestSimpleRDBMS(unittest.TestCase):
    def setUp(self):
        """Run before each test: clean up old test data and create fresh DB"""
        # Remove any old test data
        test_meta = os.path.join(DATA_DIR, f"{TEST_DB_NAME}_meta.json")
        if os.path.exists(test_meta):
            os.remove(test_meta)
        for file in os.listdir(DATA_DIR):
            if file.startswith("test_table_"):
                os.remove(os.path.join(DATA_DIR, file))

        self.db = Database(TEST_DB_NAME)

    def tearDown(self):
        """Run after each test: clean up"""
        if hasattr(self, 'db'):
            del self.db
        # Optional: remove all test files
        # shutil.rmtree(DATA_DIR)  # Uncomment if you want full cleanup

    def test_create_table_and_persistence(self):
        table = self.db.create_table(
            "test_table_users",
            columns={"id": "INT", "name": "TEXT", "active": "BOOLEAN"},
            primary_key="id",
            unique=["name"]
        )
        self.assertEqual(table.name, "test_table_users")
        self.assertIn("id", table.indexes)
        self.assertIn("name", table.indexes)

        # Check file was created
        self.assertTrue(os.path.exists(os.path.join(DATA_DIR, "test_table_users.json")))

    def test_insert_and_select(self):
        self.db.create_table("test_table_users", {"id": "INT", "score": "INT"}, primary_key="id")

        table = self.db.get_table("test_table_users")
        table.insert({"id": 1, "score": 95})
        table.insert({"id": 2, "score": 87})

        rows = table.select()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["score"], 95)

        # Test WHERE condition - CHANGE THIS TO EXACT MATCH
        high_scores = table.select({"score": 95})  # Exact match on existing value
        self.assertEqual(len(high_scores), 1)
        self.assertEqual(high_scores[0]["id"], 1)

    def test_primary_key_constraint(self):
        self.db.create_table("test_table_pk", {"id": "INT", "value": "TEXT"}, primary_key="id")
        table = self.db.get_table("test_table_pk")

        table.insert({"id": 10, "value": "first"})
        with self.assertRaises(ValueError):
            table.insert({"id": 10, "value": "duplicate"})  # Should fail

    def test_unique_constraint(self):
        self.db.create_table("test_table_unique", {"email": "TEXT", "age": "INT"}, unique=["email"])
        table = self.db.get_table("test_table_unique")

        table.insert({"email": "alice@example.com", "age": 25})
        with self.assertRaises(ValueError):
            table.insert({"email": "alice@example.com", "age": 30})

    def test_update_and_delete(self):
        self.db.create_table("test_table_updel", {"id": "INT", "status": "TEXT"}, primary_key="id")
        table = self.db.get_table("test_table_updel")

        table.insert({"id": 1, "status": "pending"})
        table.insert({"id": 2, "status": "done"})

        # Update
        updated = table.update({"status": "pending"}, {"status": "in_progress"})
        self.assertEqual(updated, 1)

        # Delete
        deleted = table.delete({"status": "done"})
        self.assertEqual(deleted, 1)

        remaining = table.select()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["status"], "in_progress")

    def test_join(self):
        # Create users
        self.db.create_table("test_users", {"id": "INT", "name": "TEXT"}, primary_key="id")
        users = self.db.get_table("test_users")
        users.insert({"id": 1, "name": "Alice"})
        users.insert({"id": 2, "name": "Bob"})

        # Create orders
        self.db.create_table("test_orders", {"oid": "INT", "user_id": "INT", "item": "TEXT"}, primary_key="oid")
        orders = self.db.get_table("test_orders")
        orders.insert({"oid": 101, "user_id": 1, "item": "Book"})
        orders.insert({"oid": 102, "user_id": 2, "item": "Pen"})

        # Join
        results = self.db.join("test_users", "test_orders", "test_users.id = test_orders.user_id")
        self.assertEqual(len(results), 2)
        self.assertIn("test_users_name", results[0])
        self.assertEqual(results[0]["test_users_name"], "Alice")
        self.assertEqual(results[1]["test_orders_item"], "Pen")

    def test_persistence_across_instances(self):
        # First instance: create and insert
        db1 = Database(TEST_DB_NAME)
        db1.create_table("test_persist", {"id": "INT", "data": "TEXT"}, primary_key="id")
        table1 = db1.get_table("test_persist")
        table1.insert({"id": 999, "data": "survive restart"})

        del db1  # Close first instance

        # Second instance: load and verify
        db2 = Database(TEST_DB_NAME)
        table2 = db2.get_table("test_persist")
        rows = table2.select()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["data"], "survive restart")


if __name__ == "__main__":
    unittest.main(verbosity=2)