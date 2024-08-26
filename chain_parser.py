import re
from datetime import datetime
import psycopg2


class LogHandler:
    """Базовый класс для обработки логов с поддержкой цепочки обязанностей."""

    def __init__(self, successor=None):
        self._successor = successor

    def handle(self, log_entry):
        """Обрабатывает запись лога или передает ее следующему обработчику."""
        if self._successor:
            self._successor.handle(log_entry)


class MessageLogHandler(LogHandler):
    """Обработчик для сохранения сообщений с определенным форматом.

    Args:
        conn (psycopg2.connect): Подключение к базе данных.
        successor (LogHandler, optional): Следующий обработчик в цепочке. По умолчанию None.
    """

    def __init__(self, conn, successor=None):
        super().__init__(successor)
        self.conn = conn

    def handle(self, log_entry):
        """Проверяет, соответствует ли запись лога формату сообщения, и сохраняет его."""
        if "<=" in log_entry["str"] and "id=" in log_entry["str"]:
            self._save_message(log_entry)
        else:
            super().handle(log_entry)

    def _extract_id(self, log_str):
        """Извлекает идентификатор из строки лога."""
        match = re.search(r"id=([^\s]+)", log_str)
        if match:
            return match.group(1).split(".")[0]
        return None

    def _save_message(self, log_entry):
        """Сохраняет сообщение в базе данных."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO message (created, id, str) 
                   VALUES (%s, %s, %s)""",
                (log_entry["created"], log_entry["id"], log_entry["str"]),
            )
            self.conn.commit()
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            if "duplicate key value violates unique constraint" in str(e):
                print("Duplicate key error")
            else:
                raise e
        finally:
            cursor.close()


class GeneralLogHandler(LogHandler):
    """Обработчик для сохранения логов, не относящихся к сообщениям.
    
    Args:
        conn (psycopg2.connect): Подключение к базе данных.
        successor (LogHandler, optional): Следующий обработчик в цепочке. По умолчанию None.
    """

    def __init__(self, conn, successor=None):
        super().__init__(successor)
        self.conn = conn

    def handle(self, log_entry):
        """Сохраняет лог в базе данных."""
        self._save_log(log_entry)

    def _save_log(self, log_entry):
        """Сохраняет лог в базе данных."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO log (created, str, address) 
                   VALUES (%s, %s, %s)""",
                (log_entry["created"], log_entry["str"], log_entry["address"]),
            )
            self.conn.commit()
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            if "duplicate key value violates unique constraint" in str(e):
                print("Duplicate key error")
            else:
                raise e
        finally:
            cursor.close()


def find_emails(text):
    """Находит все email-адреса в тексте."""
    email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
    return re.findall(email_pattern, text)


def parse_log_line(line):
    """Парсит строку лога и возвращает словарь с данными для записи в базу."""
    objects = line.split(" ")
    timestamp = f"{objects[0]} {objects[1]}"
    date_time_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    if "<=" in line and "id=" in line:
        log_id = objects[9].split("=")[1].replace("\n", "")
        return {
            "created": date_time_obj,
            "str": " ".join(objects[2:]),
            "id": log_id,
        }

    elif any(x in line for x in ["=>", "==", "->", "**", "=="]):
        emails = find_emails(line)
        if emails:
            return {
                "created": date_time_obj,
                "str": " ".join(objects[2:]),
                "address": emails[0],
            }

    return None


def process_log_file(file_path, message_handler):
    """Читает файл с логами и передает строки на обработку."""
    with open(file_path, "r") as file:
        for line in file:
            log_entry = parse_log_line(line)
            if log_entry:
                message_handler.handle(log_entry)


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
    
def create_sequences(conn):
    """
    Создает последовательности в базе данных. Обращение к БД с использованием контекстного менеджера. 
    * коннект будет закрыт под капотом

    Args:
        conn (psycopg2.connect): Подключение к базе данных.
    """
    with conn.cursor() as cursor:
        # Создаем последовательность для таблицы MESSAGE
        cursor.execute("""
        CREATE SEQUENCE message_int_id_seq
        START WITH 0
        INCREMENT BY 1
        MINVALUE 0
        NO MAXVALUE
        CACHE 1;
        """)

        # Создаем функцию для установки значения int_id в таблице MESSAGE
        cursor.execute("""
        CREATE OR REPLACE FUNCTION set_message_int_id()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.int_id := nextval('message_int_id_seq')::text;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)

        # Создаем триггер для таблицы MESSAGE
        cursor.execute("""
        CREATE TRIGGER before_insert_message
            BEFORE INSERT ON message
            FOR EACH ROW
            EXECUTE FUNCTION set_message_int_id();
        """)

        # Создаем последовательность для таблицы LOG
        cursor.execute("""
        CREATE SEQUENCE log_int_id_seq
        START 1
        INCREMENT BY 1;
        """)

        # Устанавливаем значение int_id по умолчанию в таблице LOG
        cursor.execute("""
        ALTER TABLE log ALTER COLUMN int_id SET DEFAULT nextval('log_int_id_seq');
        """)



def connect_to_db():
    """Устанавливает соединение с базой данных PostgreSQL."""
    return psycopg2.connect(
        dbname="test",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


def parse_logs():
    """Основной процесс парсинга логов."""
    conn = connect_to_db()
    setup_database(conn)
    # create_sequences(conn)

    general_log_handler = GeneralLogHandler(conn)
    message_handler = MessageLogHandler(conn, successor=general_log_handler)

    process_log_file("out", message_handler)
    conn.close()


if __name__ == "__main__":
    parse_logs()
