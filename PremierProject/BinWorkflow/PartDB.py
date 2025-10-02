# part_db.py
import psycopg2
# -----------------------------
# CONFIG
# -----------------------------
POSTGRES_HOST = "172.18.0.4"   # your Postgres container or host IP
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your-super-secret-and-long-postgres-password"
TABLE_NAME = "public.bin_part_weight_db"

class PartDatabase:
    def __init__(self, host, port, dbname, user, password, tablename):
        """
        Initialize PostgreSQL connection parameters.
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.tablename=tablename

    def get_part_details(self, part_number: str):
        """
        Fetch part details from table by PART NUMBER.
        Returns a dictionary of column -> value, or None if not found.
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            cur = conn.cursor()

            query = f"""
                SELECT * FROM {self.tablename}
                WHERE "PART NUMBER" = %s
                LIMIT 1;
            """
            cur.execute(query, (part_number,))
            row = cur.fetchone()

            if row:
                colnames = [desc[0] for desc in cur.description]
                return dict(zip(colnames, row))
            else:
                return None

        except Exception as e:
            print("Error fetching part details:", e)
            return None
        finally:
            if conn:
                conn.close()


# Example usage when run directly
if __name__ == "__main__":
    db = PartDatabase(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    tablename=TABLE_NAME
    )

    part_number = "64303-K0L-D000"
    details = db.get_part_details(part_number)

    if details:
        print("Part Details:")
        for k, v in details.items():
            print(f"{k}: {v}")
    else:
        print(f"No details found for part number: {part_number}")
