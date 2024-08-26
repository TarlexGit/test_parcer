import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn


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

            html_content = """
            <html>
            <head>
                <title>Welcome</title>
            </head>
            <body>
                <h1>Hello, Welcome to our server!</h1>
                <p>This is a simple HTML response from our async server.</p>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode("utf-8"))

            # Печать текста в консоль
            print("GET request for HTML page received")
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            response = "Hello, this is a GET response!"
            self.wfile.write(response.encode("utf-8"))

            # Печать текста в консоль
            print("GET request received")

    def do_POST(self):
        # Обработка POST запроса
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        response = {"message": "POST received"}
        self.wfile.write(json.dumps(response).encode("utf-8"))

        # Печать текста в консоль
        print(f"POST request received with data: {post_data}")


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Многопоточный HTTP сервер."""

    daemon_threads = True
