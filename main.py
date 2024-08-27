import asyncio

from server import AsyncHTTPRequestHandler, ThreadedHTTPServer
from parse.chain_parser import parse_logs


async def run_server(loop, server):
    await loop.run_in_executor(None, server.serve_forever)


def start_server():
    server_address = ("", 8080)
    httpd = ThreadedHTTPServer(server_address, AsyncHTTPRequestHandler)
    print(f"\n\n{'-'*30}\nStarting HTTP server on port 8080")
    print(f"GO TO: http://localhost:8080/\n{'-'*30}\n\n")

    loop = asyncio.get_event_loop()
    loop.create_task(run_server(loop, httpd))
    loop.run_forever()


if __name__ == "__main__":
    # Заполнение данных в БД. Часть 1 - парсинг
    # parse_logs()
    # Часть 2 - сервер с поиском
    start_server()
