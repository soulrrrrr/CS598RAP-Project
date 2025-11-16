import duckdb
import sys

class Querier:
    def __init__(self):
        """Initializes the connection and sets up Iceberg."""
        print("Connecting to DuckDB...")
        self.con = duckdb.connect()
        self.init_iceberg()

    def init_iceberg(self):
        """Loads extensions and attaches the Iceberg catalog."""
        print("Initializing Iceberg catalog configuration...")
        try:
            self.con.sql("""
    INSTALL httpfs;
    LOAD httpfs;
    INSTALL iceberg;
    LOAD iceberg;
    
    /* Attach the catalog. 
    NOTE: 'demo' is the catalog name in DuckDB.
    */
    ATTACH 'demo' AS demo (
        TYPE ICEBERG,
        ENDPOINT 'http://localhost:8181',
        AUTHORIZATION_TYPE 'none'
    );
    
    /* Create the S3 secret for MinIO */
    CREATE OR REPLACE SECRET minio_s3 (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'password',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
    
    /* Set the default schema. 
    This means queries like 'SELECT * FROM customer' will work.
    */
    SET schema 'demo.tpcds';
            """)
            print("✅ Catalog 'demo' attached and schema set to 'demo.tpcds'.")
        except Exception as e:
            print(f"❌ Error during Iceberg initialization: {e}", file=sys.stderr)
            raise

    def query(self, query_type: str, sql_path: str):
        """
        Reads an SQL query from a file and executes it.
        """
        if query_type.lower() != "iceberg":
            print(f"Query type '{query_type}' not supported.")
            return None

        try:
            # Correct way to run SQL from a file:
            # 1. Read the file content into a string.
            print(f"Reading SQL from: {sql_path}")
            with open(sql_path, 'r') as f:
                sql_query = f.read()

            if not sql_query.strip():
                print("Warning: SQL file is empty.")
                return None

            # 2. Execute the SQL string.
            print("Executing query...")
            # .pl() formats the result as a Polars-like table
            result = self.con.sql(sql_query).pl() 
            return result

        except FileNotFoundError:
            print(f"❌ Error: SQL file not found at {sql_path}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"❌ An error occurred during query execution: {e}", file=sys.stderr)
            return None

def main_interactive():
    """
    Runs the interactive query shell.
    """
    print("--- Starting Interactive Iceberg Query Shell ---")
    try:
        # Initialize the querier once
        querier = Querier()
    except Exception as e:
        print(f"Failed to initialize Querier. Exiting. Error: {e}", file=sys.stderr)
        return

    print("\nType 'exit' or press Ctrl+C to quit.")
    
    while True:
        try:
            print("-------------------------------------------------")
            query_type = input("Enter query type (e.g., 'iceberg')> ")
            if query_type.lower() == 'exit':
                break
            
            sql_path = input("Enter path to SQL file> ")
            if sql_path.lower() == 'exit':
                break

            if not query_type or not sql_path:
                print("Both fields are required. Please try again.")
                continue

            # Run the query
            result = querier.query(query_type, sql_path)
            
            if result is not None:
                print("\n--- Query Result ---")
                print(result)
                print("----------------------\n")

        except (KeyboardInterrupt, EOFError):
            break # Handle Ctrl+C and Ctrl+D
        except Exception as e:
            print(f"An unknown error occurred: {e}", file=sys.stderr)

    print("\nExiting query shell. Goodbye!")

if __name__ == "__main__":
    main_interactive()