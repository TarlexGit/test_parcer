import asyncio

from server import AsyncHTTPRequestHandler, ThreadedHTTPServer


async def run_server(loop, server):
    await loop.run_in_executor(None, server.serve_forever)


def start_server():
    server_address = ("", 8080)
    httpd = ThreadedHTTPServer(server_address, AsyncHTTPRequestHandler)
    print("Starting HTTP server on port 8080")

    loop = asyncio.get_event_loop()
    loop.create_task(run_server(loop, httpd))
    loop.run_forever()


if __name__ == "__main__":
    start_server()
