import duckdb
import sys
from transform import wrap_with_iceberg_cte

class Querier:
    def __init__(self, ducklake_metadata_path: str, ducklake_data_folder_path: str, iceberg_path: str):
        """Initializes the connection and sets up Iceberg."""
        print("Connecting to DuckDB...")
        self.con = duckdb.connect()
        
        self.iceberg_path = iceberg_path
        
        self.init_ducklake(ducklake_metadata_path, ducklake_data_folder_path)
        self.init_iceberg()

    def init_ducklake(self, metadata_path: str, data_folder_path: str):
        """Loads extensions in Ducklake."""
        try:
            self.con.sql(f"""
    INSTALL ducklake;
    LOAD ducklake;
    ATTACH 'ducklake:{metadata_path}' AS my_ducklake (DATA_PATH '{data_folder_path}', OVERRIDE_DATA_PATH true);
            """)
            print("✅ Ducklake: my_ducklake attached.")
        except Exception as e:
            print(f"❌ Error during Ducklake initialization: {e}", file=sys.stderr)
            raise

    def init_iceberg(self):
        """Loads extensions and attaches the Iceberg catalog."""
        print("Initializing Iceberg catalog configuration...")
        try:
            self.con.sql("""
    INSTALL iceberg;
    LOAD iceberg;
    SET unsafe_enable_version_guessing = true;
            """)
            print("✅ Iceberg: Init.")
        except Exception as e:
            print(f"❌ Error during Iceberg initialization: {e}", file=sys.stderr)
            raise

    def query(self, query_type: str, sql_path: str):
        """
        Reads an SQL query from a file and executes it.
        """
        try:
            # Correct way to run SQL from a file:
            # 1. Read the file content into a string.
            print(f"Reading SQL from: {sql_path}")
            with open(sql_path, 'r') as f:
                sql_query = f.read()

            if not sql_query.strip():
                print("Warning: SQL file is empty.")
                return None
            
            if query_type.lower() == "iceberg":
                try:
                    sql_query = wrap_with_iceberg_cte(sql_query, base_path=self.iceberg_path)
                    print("--- Generated Iceberg Query ---")
                    print(sql_query)
                    print("-----------------------------")
                except Exception as e:
                    print(f"❌ Error during switching to Iceberg: {e}", file=sys.stderr)
                    raise
            elif query_type.lower() == "ducklake":
                try:
                    self.con.sql("""
                USE my_ducklake;
                    """)
                    print("Ducklake: schema set to my_ducklake")
                except Exception as e:
                    print(f"❌ Error during switching to Ducklake: {e}", file=sys.stderr)
                    raise
            else:
                print(f"Query type '{query_type}' not supported.")
                return None

            # 3. Execute the SQL string.
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

def main_interactive(querier):
    """
    Runs the interactive query shell.
    """
    print("\n--- Starting Interactive Query Shell ---")
    print("Type 'exit' or press Ctrl+C to quit.")
    
    while True:
        try:
            print("-------------------------------------------------")
            query_type = input("Enter query type (iceberg/ducklake)> ")
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

def main_single_query(querier, query_type: str, sql_path: str):
    """
    Runs a single query based on command line arguments and exits.
    """
    print(f"--- Running Single Query for '{query_type}' ---")
    
    result = querier.query(query_type, sql_path)
    
    if result is not None:
        print("\n--- Final Query Result ---")
        print(result)
        print("--------------------------\n")
    else:
        print("Query execution failed or returned no result.")


if __name__ == "__main__":
    
    # 1. Initialize Querier instance (done once)
    try:
        # Define default paths for the Ducklake database
        querier = Querier(
            ducklake_metadata_path = 'data/ducklake/metadata.ducklake',
            ducklake_data_folder_path = 'data/ducklake/data_files',
            iceberg_path = 'data/iceberg/tpcds'
        )
    except Exception as e:
        print(f"Failed to initialize Querier. Exiting. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Handle command line arguments
    num_args = len(sys.argv)

    if num_args == 2 and sys.argv[1] == '-i':
        # Interactive mode: python query.py -i
        main_interactive(querier)
    elif num_args == 3:
        # Single query mode: python query.py <query_type> <sql_path>
        query_type = sys.argv[1]
        sql_path = sys.argv[2]
        main_single_query(querier, query_type, sql_path)
    else:
        # Print usage instructions
        print("--- Usage ---")
        print("Interactive mode:")
        print("  python query.py -i")
        print("\nSingle query mode:")
        print("  python query.py <query_type> <sql_path>")
        print("\n<query_type> must be 'iceberg' or 'ducklake'.")
        print("Example: python query.py iceberg tpcds_q1.sql")
        sys.exit(1)