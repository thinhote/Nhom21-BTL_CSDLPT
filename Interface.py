import psycopg2

DATABASE_NAME = 'csdl_pt'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getopenconnection(user='postgres', password='14102004', dbname='csdl_pt'):
    return psycopg2.connect(f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'")

def loadratings(tablename, filepath, conn):
    try:
        cur = conn.cursor()
    # Tạo bảng nếu chưa tồn tại
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tablename} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)

        batch_size = 50000
        batch = []

        with open(filepath, 'r') as file:
            for line in file:
                tokens = line.strip().split("::")
                if len(tokens) >= 3:
                    user = int(tokens[0])
                    movie = int(tokens[1])
                    rate = float(tokens[2])
                    batch.append((user, movie, rate))

                    # Khi đủ batch_size thì insert một lần
                    if len(batch) >= batch_size:
                        cur.executemany(
                            f"INSERT INTO {tablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
                            batch
                        )
                    batch = []

                # Chèn phần còn lại chưa đủ batch_size
                if batch:
                    cur.executemany(
                        f"INSERT INTO {tablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
                        batch
                    )

        conn.commit()
        cur.close()

    except Exception as e:
        print(f'Error in loadratings:', e)

def rangepartition(tablename, n, conn):
    try:
        cur = conn.cursor()
        step = 5.0 / n

        # Tạo các bảng phân mảnh trước
        for i in range(n):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {RANGE_TABLE_PREFIX}{i} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)

        # Duyệt toàn bộ bảng Ratings
        cur.execute(f"SELECT * FROM {tablename};")
        rows = cur.fetchall()

        # Gom dữ liệu theo phân mảnh
        partitions = [[] for _ in range(n)]
        for row in rows:
            rating = row[2]
            index = int(rating / step)
            if rating % step == 0 and index != 0:
                index -= 1
            partitions[index].append(row)

        # Chèn theo từng phân mảnh (executemany)
        for i in range(n):
            if partitions[i]:
                cur.executemany(
                    f"INSERT INTO {RANGE_TABLE_PREFIX}{i} VALUES (%s, %s, %s);",
                    partitions[i]
            )

        conn.commit()
        cur.close()

    except Exception as e:
        print(f'Error in rangepartition:', e)

def roundrobinpartition(tablename, n, conn):
    try:
        cur = conn.cursor()
        # Tạo các bảng phân mảnh
        for i in range(n):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {RROBIN_TABLE_PREFIX}{i} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)

        # Lấy toàn bộ dữ liệu
        cur.execute(f"SELECT * FROM {tablename};")
        rows = cur.fetchall()

        # Phân phối vào danh sách các phân mảnh
        partitions = [[] for _ in range(n)]
        for idx, row in enumerate(rows):
            part_index = idx % n
            partitions[part_index].append(row)

        # Chèn theo từng phân mảnh
        for i in range(n):
            if partitions[i]:
                cur.executemany(
                    f"INSERT INTO {RROBIN_TABLE_PREFIX}{i} VALUES (%s, %s, %s);",
                    partitions[i]
                )

        conn.commit()
        cur.close()

    except Exception as e:
        print(f'Error in roundrobinpartition:', e)

def rangeinsert(tablename, userid, movieid, rating, conn):
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {tablename} VALUES (%s, %s, %s);", (userid, movieid, rating))

        cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'range_part%';")
        n = cur.fetchone()[0]
        step = 5.0 / n

        index = int(rating / step)
        if rating % step == 0 and index != 0:
            index -= 1

        target_table = f"{RANGE_TABLE_PREFIX}{index}"
        cur.execute(f"INSERT INTO {target_table} VALUES (%s, %s, %s);", (userid, movieid, rating))

        conn.commit()
        cur.close()


    except Exception as e:
        print(f'Error in rangeinsert:', e)
def roundrobininsert(tablename, userid, movieid, rating, conn):
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {tablename} VALUES (%s, %s, %s);", (userid, movieid, rating))

        cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'rrobin_part%';")
        n = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {tablename};")
        count = cur.fetchone()[0]

        part_index = (count - 1) % n
        target_table = f"{RROBIN_TABLE_PREFIX}{part_index}"
        cur.execute(f"INSERT INTO {target_table} VALUES (%s, %s, %s);", (userid, movieid, rating))

        conn.commit()
        cur.close()


    except Exception as e:
        print(f'Error in roundrobininsert:', e)
def createdb(dbname):
    try:
        con = getopenconnection(dbname='csdl_pt')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
        else:
            print(f'Database "{dbname}" already exists')
        cur.close()
        con.close()


    except Exception as e:
        print(f'Error in createdb:', e)
def deleteAllPublicTables(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        for row in cur.fetchall():
            cur.execute(f"DROP TABLE IF EXISTS {row[0]} CASCADE;")
        cur.close()
        conn.commit()
    except Exception as e:
        print(f'Error in deleteAllPublicTables:', e)
