"""
Generic Database Layer
Abstracted database access supporting multiple backends (PostgreSQL, SQLite, MySQL)
with connection pooling and proper resource management
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Union
from contextlib import contextmanager
import logging
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==========================================
# DATABASE CONFIGURATION
# ==========================================

class DatabaseType(str, Enum):
    """Supported database types"""
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MYSQL = "mysql"


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: DatabaseType
    host: Optional[str] = None
    port: Optional[int] = None
    database: str = None
    user: Optional[str] = None
    password: Optional[str] = None
    # For SQLite
    db_file: Optional[str] = None
    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30


# ==========================================
# BASE DATABASE INTERFACE
# ==========================================

class DatabaseInterface(ABC):
    """Abstract base class for database operations"""
    
    @abstractmethod
    def connect(self):
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results"""
        pass
    
    @abstractmethod
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows"""
        pass
    
    @abstractmethod
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute query with multiple parameter sets"""
        pass


# ==========================================
# POSTGRESQL IMPLEMENTATION
# ==========================================

class PostgreSQLDatabase(DatabaseInterface):
    """PostgreSQL database implementation"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.conn = None
        self._pool = None
    
    def connect(self):
        """Establish connection"""
        try:
            import psycopg2
            from psycopg2 import pool
            
            self._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.config.pool_size,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            logger.info(f"PostgreSQL connection pool created: {self.config.database}")
            
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise
    
    def disconnect(self):
        """Close all connections"""
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool"""
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Transaction error: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            
            # Get column names
            columns = [desc[0] for desc in cur.description]
            
            # Convert rows to dictionaries
            results = [dict(zip(columns, row)) for row in cur.fetchall()]
            cur.close()
            
            return results
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            affected = cur.rowcount
            cur.close()
            return affected
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute batch operations"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.executemany(query, params_list)
            affected = cur.rowcount
            cur.close()
            return affected


# ==========================================
# SQLITE IMPLEMENTATION
# ==========================================

class SQLiteDatabase(DatabaseInterface):
    """SQLite database implementation"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.conn = None
    
    def connect(self):
        """Establish connection"""
        try:
            import sqlite3
            
            self.conn = sqlite3.connect(
                self.config.db_file,
                check_same_thread=False  # Allow multi-threaded access
            )
            self.conn.row_factory = sqlite3.Row  # Enable column access by name
            logger.info(f"SQLite connection established: {self.config.db_file}")
            
        except Exception as e:
            logger.error(f"SQLite connection error: {e}")
            raise
    
    def disconnect(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed")
    
    @contextmanager
    def get_connection(self):
        """Get connection (SQLite uses single connection)"""
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction error: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params or ())
            
            # Convert Row objects to dictionaries
            results = [dict(row) for row in cur.fetchall()]
            cur.close()
            
            return results
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params or ())
            affected = cur.rowcount
            cur.close()
            return affected
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute batch operations"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.executemany(query, params_list)
            affected = cur.rowcount
            cur.close()
            return affected


# ==========================================
# DATABASE FACTORY
# ==========================================

class DatabaseFactory:
    """Factory for creating database instances"""
    
    @staticmethod
    def create(config: DatabaseConfig) -> DatabaseInterface:
        """Create appropriate database instance"""
        if config.db_type == DatabaseType.POSTGRESQL:
            return PostgreSQLDatabase(config)
        elif config.db_type == DatabaseType.SQLITE:
            return SQLiteDatabase(config)
        elif config.db_type == DatabaseType.MYSQL:
            # Could implement MySQL here
            raise NotImplementedError("MySQL not yet implemented")
        else:
            raise ValueError(f"Unsupported database type: {config.db_type}")


# ==========================================
# GENERIC REPOSITORY PATTERN
# ==========================================

class Repository:
    """
    Generic repository for database operations.
    Provides CRUD operations without being tied to specific tables.
    """
    
    def __init__(self, db: DatabaseInterface, table_name: str):
        self.db = db
        self.table_name = table_name
    
    def find_by_column(
        self,
        column: str,
        value: Any,
        limit: int = 1
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Find record(s) by column value
        
        Args:
            column: Column name to search
            value: Value to search for
            limit: Number of results (1 returns dict, >1 returns list)
        
        Returns:
            Single dict if limit=1, list of dicts otherwise, or None if not found
        """
        try:
            query = f'SELECT * FROM {self.table_name} WHERE "{column}" = %s'
            
            if limit == 1:
                query += " LIMIT 1"
                results = self.db.execute_query(query, (value,))
                return results[0] if results else None
            else:
                if limit > 1:
                    query += f" LIMIT {limit}"
                return self.db.execute_query(query, (value,))
        
        except Exception as e:
            logger.error(f"Error finding by {column}: {e}")
            return None
    
    def find_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all records from table"""
        try:
            query = f"SELECT * FROM {self.table_name}"
            if limit:
                query += f" LIMIT {limit}"
            return self.db.execute_query(query)
        except Exception as e:
            logger.error(f"Error finding all: {e}")
            return []
    
    def find_where(
        self,
        conditions: Dict[str, Any],
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find records matching multiple conditions
        
        Args:
            conditions: Dict of column:value pairs (AND logic)
            limit: Optional result limit
        """
        try:
            where_clauses = [f'"{col}" = %s' for col in conditions.keys()]
            where_str = " AND ".join(where_clauses)
            
            query = f"SELECT * FROM {self.table_name} WHERE {where_str}"
            if limit:
                query += f" LIMIT {limit}"
            
            params = tuple(conditions.values())
            return self.db.execute_query(query, params)
        
        except Exception as e:
            logger.error(f"Error finding with conditions: {e}")
            return []
    
    def insert(self, data: Dict[str, Any]) -> bool:
        """
        Insert new record
        
        Args:
            data: Dictionary of column:value pairs
        
        Returns:
            True if successful
        """
        try:
            columns = list(data.keys())
            placeholders = ["%s"] * len(columns)
            
            query = f"""
                INSERT INTO {self.table_name} ({', '.join(f'"{col}"' for col in columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            params = tuple(data.values())
            affected = self.db.execute_update(query, params)
            
            return affected > 0
        
        except Exception as e:
            logger.error(f"Error inserting: {e}")
            return False
    
    def update(
        self,
        data: Dict[str, Any],
        conditions: Dict[str, Any]
    ) -> int:
        """
        Update records
        
        Args:
            data: Dictionary of column:value pairs to update
            conditions: Dictionary of column:value pairs for WHERE clause
        
        Returns:
            Number of rows affected
        """
        try:
            set_clauses = [f'"{col}" = %s' for col in data.keys()]
            where_clauses = [f'"{col}" = %s' for col in conditions.keys()]
            
            query = f"""
                UPDATE {self.table_name}
                SET {', '.join(set_clauses)}
                WHERE {' AND '.join(where_clauses)}
            """
            
            params = tuple(list(data.values()) + list(conditions.values()))
            return self.db.execute_update(query, params)
        
        except Exception as e:
            logger.error(f"Error updating: {e}")
            return 0
    
    def delete(self, conditions: Dict[str, Any]) -> int:
        """
        Delete records
        
        Args:
            conditions: Dictionary of column:value pairs for WHERE clause
        
        Returns:
            Number of rows affected
        """
        try:
            where_clauses = [f'"{col}" = %s' for col in conditions.keys()]
            
            query = f"""
                DELETE FROM {self.table_name}
                WHERE {' AND '.join(where_clauses)}
            """
            
            params = tuple(conditions.values())
            return self.db.execute_update(query, params)
        
        except Exception as e:
            logger.error(f"Error deleting: {e}")
            return 0
    
    def count(self, conditions: Optional[Dict[str, Any]] = None) -> int:
        """Count records, optionally with conditions"""
        try:
            query = f"SELECT COUNT(*) as count FROM {self.table_name}"
            
            if conditions:
                where_clauses = [f'"{col}" = %s' for col in conditions.keys()]
                query += f" WHERE {' AND '.join(where_clauses)}"
                params = tuple(conditions.values())
            else:
                params = None
            
            result = self.db.execute_query(query, params)
            return result[0]["count"] if result else 0
        
        except Exception as e:
            logger.error(f"Error counting: {e}")
            return 0
    
    def exists(self, conditions: Dict[str, Any]) -> bool:
        """Check if record exists"""
        return self.count(conditions) > 0


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    # ==========================================
    # PostgreSQL Example
    # ==========================================
    
    print("=== PostgreSQL Example ===")
    
    pg_config = DatabaseConfig(
        db_type=DatabaseType.POSTGRESQL,
        host="172.18.0.16",
        port=5432,
        database="postgres",
        user="postgres",
        password="password"
    )
    
    pg_db = DatabaseFactory.create(pg_config)
    pg_db.connect()
    
    # Create repositories for different tables
    part_repo = Repository(pg_db, "part_weight_db")
    bin_repo = Repository(pg_db, "rfid_bin_db")
    
    # Find part by part number
    part = part_repo.find_by_column("PART NUMBER", "64303-K0L-D000")
    if part:
        print(f"Found part: {part['PART NAME']}")
        print(f"Weight: {part['PART WEIGHT']} kg")
    
    # Find bin by EPC
    bin_data = bin_repo.find_by_column("epc", "E7 76 09 89 49 00 37 33 90 00 00 01")
    if bin_data:
        print(f"Bin weight: {bin_data['empty_bin_weight']} kg")
    
    # Find all parts with specific model
    parts = part_repo.find_where({"MODEL": "CBR650R"}, limit=10)
    print(f"Found {len(parts)} parts for model CBR650R")
    
    # Count all parts
    total_parts = part_repo.count()
    print(f"Total parts in database: {total_parts}")
    
    # Check if part exists
    exists = part_repo.exists({"PART NUMBER": "64303-K0L-D000"})
    print(f"Part exists: {exists}")
    
    pg_db.disconnect()
    
    # ==========================================
    # SQLite Example
    # ==========================================
    
    print("\n=== SQLite Example ===")
    
    sqlite_config = DatabaseConfig(
        db_type=DatabaseType.SQLITE,
        db_file="factory.db"
    )
    
    sqlite_db = DatabaseFactory.create(sqlite_config)
    sqlite_db.connect()
    
    # Same repository pattern works with SQLite
    parts_sqlite = Repository(sqlite_db, "part_weight_db")
    
    # Insert new part
    new_part = {
        "PART NUMBER": "TEST-001",
        "PART NAME": "Test Part",
        "MODEL": "Test Model",
        "PART WEIGHT": 0.5
    }
    
    success = parts_sqlite.insert(new_part)
    print(f"Insert successful: {success}")
    
    # Update part
    updated = parts_sqlite.update(
        data={"PART WEIGHT": 0.6},
        conditions={"PART NUMBER": "TEST-001"}
    )
    print(f"Rows updated: {updated}")
    
    # Delete part
    deleted = parts_sqlite.delete({"PART NUMBER": "TEST-001"})
    print(f"Rows deleted: {deleted}")
    
    sqlite_db.disconnect()


# ==========================================
# INTEGRATION WITH EXISTING CODE
# ==========================================

"""
To use with your existing workflow orchestrator:

# Create database config
config = DatabaseConfig(
    db_type=DatabaseType.POSTGRESQL,
    host="172.18.0.16",
    port=5432,
    database="postgres",
    user="postgres",
    password="password"
)

# Create database instance
db = DatabaseFactory.create(config)
db.connect()

# Create repositories
part_repo = Repository(db, "part_weight_db")
bin_repo = Repository(db, "rfid_bin_db")

# Use in workflow orchestrator
class WorkflowOrchestrator:
    def __init__(self, db: DatabaseInterface):
        self.part_repo = Repository(db, "part_weight_db")
        self.bin_repo = Repository(db, "rfid_bin_db")
    
    def _handle_qr_scan(self, payload):
        qr_code = payload.get("qr_code")
        
        # Find part details
        part_details = self.part_repo.find_by_column("PART NUMBER", qr_code)
        
        if part_details:
            self.current_job.part_details = part_details
            ...
"""