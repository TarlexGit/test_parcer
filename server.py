import itertools
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

from parse.chain_parser import connect_to_db, find_emails
from my_html import PAGE


class AsyncHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    Класс обработчика HTTP-запросов.
    """

    def do_GET(self):
        # Обработка GET запроса
        if self.path == "/":
            # Возвращаем простую HTML-страницу с приветствием
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            html_content = PAGE
            self.wfile.write(html_content.encode("utf-8"))
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            response = "Hello, this is a GET response!"
            self.wfile.write(response.encode("utf-8"))

    def do_POST(self):
        try:
            response_data = []
            more = False

            post_data = self._get_post_data()
            email = self._extract_email(post_data)

            if email:
                print("Emails found:", email)
                gen_data = self._get_data_by_email(email)
                response_data, more = self._process_generator_data(gen_data)

            self._send_response(200, {"data": response_data, "more": more})

            print(f"POST request processed successfully with data: {post_data}")

        except Exception as e:
            self._send_response(500, {"error": str(e)})
            print(f"Error processing POST request: {e}")

    def _get_post_data(self):
        """Получение данных из POST запроса."""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        return json.loads(post_data)

    def _extract_email(self, data):
        """Извлечение email из данных."""
        emails = find_emails(data.get("email", ""))
        return emails[0] if emails else None

    def _process_generator_data(self, gen_data):
        """Обработка данных из генератора, возвращающего message и log данные."""
        response_data = []
        more = False

        for i in range(100):  # Максимум 100 итераций
            message_item = next(gen_data, None)
            log_item = next(gen_data, None)

            if message_item:
                response_data.append(self._format_data(message_item))
            if log_item:
                response_data.append(self._format_data(log_item))

            if not message_item and not log_item:
                break
        else:
            more = True

        return response_data, more

    def _format_data(self, item):
        """Форматирование данных для ответа."""
        human_readable = item[0].strftime("%Y-%m-%d %H:%M:%S")
        return [human_readable, *item[1:]]

    def _send_response(self, status_code, response_data):
        """Отправка HTTP ответа."""
        self.send_response(status_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response_data).encode("utf-8"))

    @staticmethod
    def _get_data_by_email(email):
        """Получение данных message и log из базы данных."""
        conn = connect_to_db()  # TODO: Вынести в отдельный модуль
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM message WHERE str LIKE %s ORDER BY created ASC",
                ("%" + email + "%",),
            )
            message_data = cursor.fetchall()

            cursor.execute(
                "SELECT * FROM log WHERE address = %s ORDER BY created ASC", (email,)
            )
            log_data = cursor.fetchall()

        for message_item, log_item in itertools.zip_longest(message_data, log_data):
            yield message_item
            yield log_item


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Многопоточный HTTP сервер."""

    daemon_threads = True
