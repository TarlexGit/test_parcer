import psycopg2


def setup_database(conn):
    """Настраивает структуру базы данных."""
    cursor = conn.cursor()

    cursor.execute(
        """CREATE SEQUENCE IF NOT EXISTS message_int_id_seq
           START 0 INCREMENT BY 1 MINVALUE 0;"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS message (
           created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
           id VARCHAR NOT NULL,
           int_id CHAR(16) NOT NULL DEFAULT nextval('message_int_id_seq'),
           str VARCHAR NOT NULL,
           status BOOL,
           CONSTRAINT message_id_pk PRIMARY KEY(id)
           );"""
    )
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS message_int_id_idx ON message (int_id);"""
    )
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS message_created_idx ON message (created);"""
    )

    # for log table
    cursor.execute(
        """CREATE SEQUENCE IF NOT EXISTS log_int_id_seq
           START 0 INCREMENT BY 1 MINVALUE 0;"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS log (
           created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
           int_id CHAR(16) NOT NULL DEFAULT nextval('log_int_id_seq'),
           str VARCHAR,
           address VARCHAR
           );"""
    )
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS log_address_idx ON log USING hash (address);"""
    )

    conn.commit()
    cursor.close()


def connect_to_db():
    """Устанавливает соединение с базой данных PostgreSQL."""
    return psycopg2.connect(
        dbname="test",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )
