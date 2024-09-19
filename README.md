[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/zixaop7v)

## Bibliotecas Utilizadas
As seguintes bibliotecas Python foram utilizadas no projeto:

psycopg2: Utilizada para se conectar e interagir com o banco de dados PostgreSQL.
dash: Framework utilizado para criar a interface web e visualizações interativas.
pandas: Utilizada para manipulação de dados.
plotly.express: Biblioteca de visualização de dados usada para gerar gráficos interativos.
datetime: Usada para manipular datas.
re: Biblioteca para expressões regulares, usada no tratamento e extração de dados.

## Executando o código

Para criar o banco de dados, basta alterar as configurações de user, password e definir um nome para o banco de dados.

```
user = "postgres"
password = ""
dbname = "postgres"
```

No dashboard, de forma semelhante, basta alterar as configurações citadas anteriormente.

```
conn = psycopg2.connect(
    dbname="nome_do_banco",
    user="seu_usuario",
    password="sua_senha"
)
```

Após o banco de dados estar configurado, execute o código Dash para criar as visualizações gráficas.
O servidor Dash estará disponível localmente em http://127.0.0.1:8050/ (pode variar conforme a configuração).

