PAGE = """
<html lang="ru">
    <head>
        <meta charset="UTF-8">
        <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
        <title>Welcome</title>
    </head>
    <body>
        <div class="centered" id="app">
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
            padding-top: 60px;
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
