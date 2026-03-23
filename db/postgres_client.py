import os
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool 

load_dotenv()
host = os.environ.get("POSTGRES_HOST", "localhost")
port = os.environ.get("POSTGRES_PORT", "5432")

user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
db = os.environ.get("POSTGRES_DB")

_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host=host,
    port=port,
    user=user,
    password=password,
    dbname=db
)

def init_db():
    conn = _pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS global_rankings(
                        query TEXT NOT NULL,
                        result_id TEXT NOT NULL,
                        total_clicks INTEGER DEFAULT 0,
                        total_views INTEGER DEFAULT 0,
                        total_purchases INTEGER DEFAULT 0,
                        score FLOAT DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (query, result_id)
                    )""")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS user_rankings(
                query TEXT NOT NULL,
                result_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                total_clicks INTEGER DEFAULT 0,
                total_views INTEGER DEFAULT 0,
                total_purchases INTEGER DEFAULT 0,
                score FLOAT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (query, result_id, user_id)
            )"""
        )
        conn.commit()
    finally:
        _pool.putconn(conn)

def upsert_ranking(query, result_id, clicks=0, views=0, purchases=0, user_id=None):
    conn = _pool.getconn()
    try:
        cur = conn.cursor()
        score = (clicks * 3) + (views * 1) + (purchases * 10)
        if user_id is None:
            cur.execute("""
                INSERT INTO global_rankings (query, result_id, total_clicks, total_views,
                total_purchases, score, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (query, result_id)
                DO UPDATE SET
                    total_clicks = global_rankings.total_clicks + EXCLUDED.total_clicks,
                    total_views = global_rankings.total_views + EXCLUDED.total_views,
                    total_purchases = global_rankings.total_purchases + EXCLUDED.total_purchases,
                    score = global_rankings.score + EXCLUDED.score,
                    updated_at = NOW()
            """, (query, result_id, clicks, views, purchases, score))
        else:
            cur.execute("""
                INSERT INTO user_rankings (query, result_id, user_id, total_clicks, total_views,
                total_purchases, score, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (query, result_id, user_id)
                DO UPDATE SET
                    total_clicks = user_rankings.total_clicks + EXCLUDED.total_clicks,
                    total_views = user_rankings.total_views + EXCLUDED.total_views,
                    total_purchases = user_rankings.total_purchases + EXCLUDED.total_purchases,
                    score = user_rankings.score + EXCLUDED.score,
                    updated_at = NOW()
            """, (query, result_id, user_id, clicks, views, purchases, score))
        conn.commit()
    finally:
        _pool.putconn(conn)

def get_global_rankings(query, top_n=10):
    conn = _pool.getconn()
    res = []
    try:
        cur = conn.cursor()
        cur.execute(
                    """
                    SELECT result_id, total_clicks, total_views, total_purchases, score
                    FROM global_rankings
                    WHERE query = %s
                    ORDER BY score DESC
                    LIMIT %s
                    """
                    , (query, top_n))
        rows = cur.fetchall()
        res = [
            {
                "result_id": row[0],
                "total_clicks": row[1],
                "total_views": row[2],
                "total_purchases": row[3],
                "score": row[4],
            }
            for row in rows
        ]
    finally:
        _pool.putconn(conn)
    return res

def get_user_rankings(query, user_id, top_n=10):
    conn = _pool.getconn()
    res = []
    try:
        cur = conn.cursor()
        cur.execute(
                    """
                    SELECT result_id, total_clicks, total_views, total_purchases, score
                    FROM user_rankings
                    WHERE query = %s AND user_id = %s
                    ORDER BY score DESC
                    LIMIT %s
                    """
                    , (query, user_id, top_n))
        rows = cur.fetchall()
        res = [
            {
                "result_id": row[0],
                "total_clicks": row[1],
                "total_views": row[2],
                "total_purchases": row[3],
                "score": row[4],
            }
            for row in rows
        ]
    finally:
        _pool.putconn(conn)
    return res