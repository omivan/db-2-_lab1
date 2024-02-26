import threading
import time
import psycopg2


db_params = {
    'database': 'lab1',
    'user': 'simon',
    'host': 'localhost',
    'port': '5432'
}


def create_connection():
    conn = psycopg2.connect(**db_params)
    return conn


def initialize_db():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS user_counter CASCADE;")

            cursor.execute("""
                CREATE TABLE user_counter (
                    user_id INTEGER PRIMARY KEY,
                    counter INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0
                );
            """)

            cursor.execute("""
                INSERT INTO user_counter (user_id, counter, version) 
                VALUES (1, 0, 0);
            """)


def get_final_counter():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cursor.fetchone()[0]
            return counter


def lost_update():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                counter = cursor.fetchone()[0]
                counter += 1
                cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                conn.commit()


def in_place_update():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (1,))
                conn.commit()


def update_counter_with_row_lock():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
                counter = cursor.fetchone()[0]
                counter += 1
                cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                conn.commit()


def update_counter_with_optimistic_locking():
    conn = create_connection()
    with conn:
        with conn.cursor() as cursor:
            for i in range(10000):
                while True:
                    cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                    counter, version = cursor.fetchone()
                    counter += 1
                    cursor.execute(
                        "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                        (counter, version + 1, 1, version))
                    conn.commit()
                    if cursor.rowcount > 0:
                        break


def test_method(func):
    initialize_db()
    threads = [threading.Thread(target=func) for _ in range(10)]
    start_time = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    end_time = time.time()
    print(f"Total execution time for {func.__name__}: {round(end_time - start_time, 2)} seconds")
    print(f"Counter is {get_final_counter()}")


if __name__ == "__main__":
    print("Started execution")
    test_method(lost_update)
    # Total execution time for lost_update: 12.74 seconds
    # Counter is 10736


    test_method(in_place_update)
    # Total execution time for in_place_update: 11.08 seconds
    # Counter is 100000


    test_method(update_counter_with_row_lock)
    # Total execution time for update_counter_with_row_lock: 18.16 seconds
    # Counter is 100000


    test_method(update_counter_with_optimistic_locking)
    # Total execution time for update_counter_with_optimistic_locking: 83.57 seconds
    # Counter is 100000


