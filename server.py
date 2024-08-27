import itertools
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

from chain_parser import connect_to_db, find_emails


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
            <html lang="ru">
                <head>
                    <meta charset="UTF-8">
                    <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
                    <title>Welcome</title>
                </head>
                <body>
                    <h1>Hello, Welcome to our server!</h1>
                    <p>This is a simple HTML response from our async server.</p>
                    
                    <div id="app">
                        <input v-model="inputValue" type="text" placeholder="Введите текст">
                        <button @click="handleClick">Нажмите меня</button>
                        <div class="messageFlag centered" v-if="messageFlag">Показаны первые 100 записей!</div>
                        <div v-for="i in logsData.data" :key="i">
                            <div>{{i[0]}} {{i[2]}}</div>
                        </div>
                    </div>
                </body>
                <style>
                    .messageFlag {
                        color: red;
                        font-weight: bold;
                    }
                    
                    .centered {
                        text-align: center;
                    }
                </style>
                <script>
                const { createApp, ref } = Vue

                createApp({
                    setup() {
                        const inputValue = ref('')
                        const logsData = ref([])
                        const messageFlag = ref(false)
                    
                        const handleClick = () => {
                            console.log('Кнопка была нажата. Введенное значение: ', inputValue.value)
                            fetchDataByEmail(inputValue.value)
                        }

                        function fetchDataByEmail(email) {
                            fetch("http://localhost:8080/", {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json'
                                },
                                body: JSON.stringify({"email": email})
                            })
                            .then(response => {
                                if (!response.ok) {
                                    throw new Error('HTTP error ' + response.status);
                                }
                                return response.json();
                            })
                            .then(data => {
                                logsData.value = data;  // Обновляем реактивные данные
                                messageFlag.value = true; // Сообщений больше 100 штук
                            })
                            .catch(error => {
                                console.log('There was a problem with your fetch operation: ' + error);
                            });
                        }

                        return {
                            inputValue,
                            handleClick,
                            logsData,
                            messageFlag
                        }
                    }
                }).mount('#app')
                </script>
            </html>

            """
            # вёрстку делать лень, я извиняюсь...
            self.wfile.write(html_content.encode("utf-8"))

            # Печать текста в консоль
            print("GET request for HTML page received")
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            response = "Hello, this is a GET response!"
            self.wfile.write(response.encode("utf-8"))

            print("GET request received")

    def do_POST(self):
        response_data = "POST received"
        more = None
        # Обработка POST запроса
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        
        data = json.loads(post_data)
        
        if len(emails:=find_emails(data["email"]))>0:
            print(emails)
            response_data = list()
            gen_data = self._get_data_by_email(emails[0])
            
            for i in list(range(101)):
                if i >= 100:
                    more=True    
                    break
                message_item = next(gen_data)
                log_item = next(gen_data)
                if message_item:
                    human_readable = message_item[0].strftime("%Y-%m-%d %H:%M:%S")
                    response_data.append([human_readable, *message_item[1:]])
                    print("message_item:", message_item)
                if log_item:
                    human_readable = log_item[0].strftime("%Y-%m-%d %H:%M:%S")
                    response_data.append([human_readable, *log_item[1:]])
                    print("log_item:", log_item)
                if not message_item and not log_item:
                    break
                

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        response = {"data": response_data, "more": more}
        self.wfile.write(json.dumps(response).encode("utf-8"))

        # Печать текста в консоль
        print(f"POST request received with data: {post_data}")

    @staticmethod
    def _get_data_by_email(email):
        conn = connect_to_db() # переиспользуем. Вынести бы в функции с логикой работы с БД в отдельный модуль... TODO
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM message WHERE str like %s ORDER BY created ASC", ('%'+email+'%',))
            message_data = cursor.fetchall()

            cursor.execute("SELECT * FROM log WHERE address = %s ORDER BY created ASC", (email,))
            log_data = cursor.fetchall()
            
        for message_item, log_item in itertools.zip_longest(message_data, log_data):
            yield message_item
            yield log_item
        # return message_data, log_data



class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Многопоточный HTTP сервер."""

    daemon_threads = True
