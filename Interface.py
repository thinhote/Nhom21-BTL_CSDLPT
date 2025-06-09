import psycopg2

DATABASE_NAME = 'dds_assgn1'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
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

def rangepartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()

    # Xóa các phân mảnh cũ nếu tồn tại (tùy chọn an toàn khi chạy nhiều lần)
    for i in range(numberofpartitions):
        cursor.execute(f"DROP TABLE IF EXISTS range_part{i};")

    # Tính độ rộng mỗi phân mảnh
    min_rating = 0.0
    max_rating = 5.0
    partition_width = (max_rating - min_rating) / numberofpartitions

    # Tạo bảng và chèn dữ liệu cho từng phân mảnh
    for i in range(numberofpartitions):
        lower_bound = min_rating + i * partition_width
        upper_bound = lower_bound + partition_width

        # Tạo bảng phân mảnh mới
        cursor.execute(f"""
            CREATE TABLE range_part{i} (
                UserID INT,
                MovieID INT,
                Rating FLOAT
            );
        """)

        # Tùy điều kiện WHERE theo phân mảnh đầu tiên và các phân mảnh còn lại
        if i == 0:
            condition = f"Rating >= {lower_bound} AND Rating <= {upper_bound}"
        else:
            condition = f"Rating > {lower_bound} AND Rating <= {upper_bound}"

        # Chèn dữ liệu từ bảng gốc vào phân mảnh
        cursor.execute(f"""
            INSERT INTO range_part{i} (UserID, MovieID, Rating)
            SELECT UserID, MovieID, Rating
            FROM {ratingstablename}
            WHERE {condition};
        """)

    # Commit thay đổi và đóng cursor (không đóng connection theo yêu cầu đề bài)
    openconnection.commit()
    cursor.close()

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

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    # Xóa các bảng partition cũ nếu có
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i};")
    for i in range(numberofpartitions):
        cur.execute(f"CREATE TABLE rrobin_part{i} (userid INT, movieid INT, rating FLOAT);")
    # Lấy tất cả dữ liệu và phân phối theo thứ tự
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
    rows = cur.fetchall()
    for idx, row in enumerate(rows):
        part_idx = idx % numberofpartitions
        cur.execute(f"INSERT INTO rrobin_part{part_idx} (userid, movieid, rating) VALUES (%s, %s, %s);", row)
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    # Thêm vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    # Xác định số partition
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%';")
    numberofpartitions = cur.fetchone()[0]
    # Đếm tổng số bản ghi đã có trong bảng ratings
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    index = (total_rows - 1) % numberofpartitions
    cur.execute(f"INSERT INTO rrobin_part{index} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    openconnection.commit()
    cur.close()

def createdb(dbname):
    try:
        con = getopenconnection(dbname='postgres')
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

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
