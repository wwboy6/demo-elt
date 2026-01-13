import subprocess
import time

def wait_for_postgres(host, interval=5, timeout=60):
    """Wait for PostgreSQL to be available."""
    start_time = time.time()
    while True:
        try:
            subprocess.run(
                [
                    "pg_isready",
                    "-h",
                    host,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            print("PostgreSQL is available.")
            return True
        except subprocess.CalledProcessError as e:
            if time.time() - start_time > timeout:
                print(f"Timed out waiting for PostgreSQL to be available: {e}")
                return False
            print("Waiting for PostgreSQL to be available...")
            time.sleep(interval)

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres',
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres',
}

def main():
    if not wait_for_postgres(source_config['host']):
        print("Exiting due to PostgreSQL unavailability.")
        return
    print("Starting ELT script...")
    subprocess.run(
        [
            'pg_dump',
            '-h', source_config['host'],
            '-U', source_config['user'],
            '-d', source_config['dbname'],
            '-f', 'dump.sql',
            '-w'
        ],
        env=dict(PGPASSWORD=source_config['password']),
        check=True
    )
    subprocess.run(
        [
            'psql',
            '-h', destination_config['host'],
            '-U', destination_config['user'],
            '-d', destination_config['dbname'],
            '-a',
            '-f', 'dump.sql',
        ],
        env=dict(PGPASSWORD=destination_config['password']),
        check=True
    )
    print("Ending ELT script...")

main()
