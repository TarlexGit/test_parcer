import re
from datetime import datetime

import psycopg2


class LogHandler:
    def __init__(self, successor=None):
        self._successor = successor

    def handle(self, log_entry):
        if self._successor:
            self._successor.handle(log_entry)


class MessageLogHandler(LogHandler):
    def __init__(self, conn, successor=None):
        super().__init__(successor)
        self.conn = conn

    def handle(self, log_entry):
        fields_exsists = all(["<=" in log_entry["str"] and "id=" in log_entry["str"]])
        if fields_exsists:
            # log_entry["id"] = self._extract_id(log_entry["str"])
            self._save_message(log_entry)
        else:
            super().handle(log_entry)

    def _extract_id(self, log_str):
        match = re.search(r"id=([^\s]+)", log_str)
        match_id = match.group(1) if match else None
        if match_id is not None:
            return match_id.split(".")[0]
        return match_id

    def _save_message(self, log_entry):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO message (created, id, str) 
                            VALUES (%s, %s, %s)""",
                (
                    log_entry["created"],
                    log_entry["id"],
                    log_entry["str"],
                ),
            )
            self.conn.commit()
        except psycopg2.IntegrityError as e:
            self.conn.rollback()  #
            if "duplicate key value violates unique constraint" in str(e):
                print("Duplicate key error")
            else:
                raise e
        finally:
            cursor.close()


class GeneralLogHandler(LogHandler):
    def __init__(self, conn, successor=None):
        super().__init__(successor)
        self.conn = conn

    def handle(self, log_entry):
        self._save_log(log_entry)

    def _save_log(self, log_entry):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO log (created, str, address) 
                            VALUES (%s, %s, %s)""",
                (
                    log_entry["created"],
                    log_entry["str"],
                    log_entry["address"],
                ),
            )
            self.conn.commit()
        except psycopg2.IntegrityError as e:
            self.conn.rollback()  #
            if "duplicate key value violates unique constraint" in str(e):
                print("Duplicate key error")
            else:
                raise e
        finally:
            cursor.close()
        # cursor.execute(
        #     """INSERT INTO log (created, int_id, str, address)
        #                   VALUES (%s, %s, %s, %s)""",
        #     (
        #         log_entry["created"],
        #         log_entry["int_id"],
        #         log_entry["str"],
        #         log_entry["address"],
        #     ),
        # )
        # self.conn.commit()
        # cursor.close()


def find_emails(text):
    # Регулярное выражение для поиска email-адресов
    email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

    # Найти все email-адреса в тексте
    emails = re.findall(email_pattern, text)

    return emails


def parse_log_line(line):
    objects = line.split(" ")
    if "<=" in line and "id=" in line:
        timestamp = objects[0] + " " + objects[1]
        date_time_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        log_id = objects[9].split("=")[1].replace("\n", "")
        return {
            "created": date_time_obj,
            "str": " ".join(objects[2:]),
            "id": log_id,
        }
    elif any(x in ["=>", "==", "->", "**", "=="] for x in objects):
        emails = find_emails(line)
        timestamp = objects[0] + " " + objects[1]
        date_time_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        if len(emails) > 0:
            return {
                "created": date_time_obj,
                "str": " ".join(objects[2:]),
                "address": emails[0],
            }

    else:
        return None


def process_log_file(file_path, message_handler):
    with open(file_path, "r") as file:
        for line in file:
            log_entry = parse_log_line(line)
            if log_entry:
                message_handler.handle(log_entry)


def setup_database(conn):

    def create_count_id_field():
        cursor = conn.cursor()
        cursor.execute(
            """CREATE SEQUENCE IF NOT EXISTS message_int_id_seq
            START 0
            INCREMENT BY 1
            MINVALUE 0;"""
        )

        conn.commit()
        cursor.close()

    # create_count_id_field()

    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS message (
        created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
        id VARCHAR NOT NULL,
        int_id CHAR(16) NOT NULL,
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

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS log (
        created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
        int_id CHAR(16) NOT NULL,
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
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )
    return conn


if __name__ == "__main__":
    # Подключение к базе данных PostgreSQL
    conn = connect_to_db()

    # Настройка базы данных
    setup_database(conn)

    # Инициализация цепочки обработчиков
    general_log_handler = GeneralLogHandler(conn)
    message_handler = MessageLogHandler(conn, successor=general_log_handler)

    # Обработка файла с логами
    process_log_file("out", message_handler)

    # Закрытие соединения с базой данных
    conn.close()
