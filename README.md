## 🗄️ Banco de Dados

Neste projeto foi utilizado o **PostgreSQL 16**, escolhido por sua robustez, alta escalabilidade e ampla adoção no mercado corporativo.

Foi criado o banco de dados denominado **`SiCooperative`**, onde:

- Foram implementadas as tabelas conforme o modelo relacional definido;
- Foram inseridos dados fictícios para simulação de cenários reais;
- A base foi utilizada como origem do processo de ETL (Bronze → Silver → Gold).

O banco atuou como camada transacional (**OLTP**), servindo como fonte para ingestão, transformação e validação dos dados no pipeline.

---

### 🔧 Ajustes Realizados na Modelagem

Durante a evolução do projeto, foram realizados ajustes estruturais no modelo:

- Remoção do relacionamento direto entre as tabelas **cartao** e **associado**, evitando redundância e possíveis inconsistências na ingestão;
- Manutenção da normalização, garantindo que o vínculo entre cartão e associado ocorra exclusivamente pela tabela **conta** (coluna `id_associado`);
- Inclusão da coluna `data_criacao_cartao` na tabela **cartao**, aprimorando o controle temporal e a rastreabilidade dos registros.

<p align="center">
  <img width="900" alt="Modelagem Banco de Dados" src="https://github.com/user-attachments/assets/87832e75-5dc0-4950-8fbb-49522461c7a7" />
</p>

---

## ⚙️ Projeto Local — Tecnologias Utilizadas

- PostgreSQL 16  
- Apache Spark  
- Hadoop (camada de filesystem utilizada pelo Spark)  
- VS Code  
- DBeaver  
- Git  

Para execução do projeto, foi necessário configurar e integrar as tecnologias acima, estruturando um ambiente local.

---

### 📁 Estrutura do Projeto

A organização do projeto no VS Code foi estruturada da seguinte forma:

<p align="center">
  <img width="280" alt="Estrutura do Projeto" src="https://github.com/user-attachments/assets/1e24f20f-4608-47e3-b0d9-6d723630ce3c" />
</p>

O arquivo **`config`** centraliza:

- Variáveis de ambiente  
- Configurações de conexão com o banco  
- Caminhos de saída dos dados  
- Configurações do Spark  

Essa separação garante:

- Maior organização  
- Segurança (evitando exposição de credenciais no arquivo principal)  
- Melhor manutenibilidade do código  

O arquivo **`main`** é responsável por:

- Leitura das bases do PostgreSQL via JDBC  
- Aplicação de transformações e validações  
- Mescla das tabelas para geração do arquivo flat  
- Salvamento do arquivo `movimento_flat` final em CSV  

---

## 🔐 Segurança e Gerenciamento de Dependências

O projeto foi estruturado utilizando arquivo **`.env`** para armazenamento de variáveis sensíveis, seguindo boas práticas de segurança.

Os arquivos que não devem ser versionados (como `.env`) foram adicionados ao **`.gitignore`**, garantindo que credenciais e informações sensíveis não sejam expostas no repositório.

Também foi incluído o arquivo **`requirements.txt`**, contendo todas as dependências utilizadas no projeto, permitindo a fácil reprodução do ambiente e garantindo padronização das bibliotecas necessárias para execução.

---

#  Versão Bônus — Pipeline Automatizada com Databricks

### ☁️ Tecnologias Utilizadas

- Neon (PostgreSQL serverless na nuvem — AWS)
- Databricks
- Apache Spark
- GitHub

Para provisionar o projeto no **Databricks**, foi realizada a exportação do banco PostgreSQL via **PowerShell**, gerando o arquivo `backup.dump`.

Esse arquivo foi restaurado na plataforma **Neon (AWS)**, permitindo disponibilizar o banco de dados em ambiente cloud.

<p align="center">
  <img width="900" alt="Exportação PowerShell" src="https://github.com/user-attachments/assets/23f4da69-e2c1-48e3-a33e-f767bd6df687" />
</p>

Posteriormente, após a criação do banco na plataforma Neon, foram utilizadas as credenciais de autenticação para conexão via Databricks e execução automatizada do pipeline.

<p align="center">
  <img width="900" alt="Credenciais Neon" src="https://github.com/user-attachments/assets/04fe12a4-b004-4eb1-9fa0-51b303edf016" />
</p>

Em seguida, foi validado que as tabelas foram corretamente restauradas no ambiente cloud:

<p align="center">
  <img width="300" alt="Tabelas no Neon" src="https://github.com/user-attachments/assets/bd4bec2f-eba5-4585-9e0c-8769fcc827f7" />
</p>

Com isso validado, foi criada a estrutura no Databricks, organizando a pasta **`src_databricks`** no GitHub para versionamento do código baseado na arquitetura **Medallion** (Bronze, Silver e Gold).

<p align="center">
  <img width="550" alt="Estrutura Medallion" src="https://github.com/user-attachments/assets/8a0a7433-efef-4aa6-9f70-34f78db32e36" />
</p>

Foi criado o arquivo **`secrets_config`** para armazenar credenciais, pois a versão gratuita do Databricks não permite criação nativa de Secret Scope.

###  Bronze
Leitura das bases via JDBC utilizando Spark, garantindo que credenciais não fossem expostas.

###  Silver
Processo de ETL, padronização de colunas, nomenclatura e validações para garantir qualidade dos dados.

###  Gold
Cruzamento das bases para geração da saída final, incluindo validações adicionais para assegurar integridade dos dados.

---

##  Testes Unitários — Camada Gold

Foi desenvolvido o script **`tests_gold_validation`**, contendo quatro validações principais:

- Schema da Gold conforme esperado;
- Garantia de que a base não está vazia;
- Colunas obrigatórias sem valores nulos;
- Formato correto do número do cartão (4 dígitos + 8 asteriscos + 4 dígitos).

Caso algum critério não seja atendido, o processo é interrompido via **`assert`**, impedindo atualização incorreta da camada Gold.

---

## ⚙️ Jobs no Databricks

Após o desenvolvimento da estrutura, foram criados três Jobs principais (Bronze, Silver e Gold), integrados ao GitHub para versionamento e automação.

<p align="center">
  <img width="1000" alt="Jobs Databricks" src="https://github.com/user-attachments/assets/c2009465-70b5-4dd3-9fa1-986fd0bc20a8" />
</p>

Dentro do Job da camada **Gold**, o script de testes unitários está configurado como dependência obrigatória. Caso os testes falhem, a camada Gold não é atualizada, evitando impactos no ambiente de produção.

<p align="center">
  <img width="1000" alt="Dependência Job Gold" src="https://github.com/user-attachments/assets/f7b06d06-95c8-48bd-a44a-6c38efdb46ba" />
</p>

---

## ⏱️ Agendamento Automático

Os Jobs estão configurados para execução automática de segunda a sexta-feira, utilizando a expressão cron:

## 📊 Resultado Final

Resultado final disponível no catálogo, na camada **Gold**, da tabela `movimento_flat`, desenvolvido nesta primeira POC:

<p align="center">
  <img width="1000" alt="Resultado Final Gold" src="https://github.com/user-attachments/assets/f73d7b8a-f6ff-41a6-8973-3f9df60aac63" />
</p>

---

## 🧩 Considerações Finais

Durante o desenvolvimento do projeto, a principal dificuldade encontrada foi a configuração do ambiente.

Como o ambiente local não possuía previamente as dependências instaladas, foi necessário realizar:

- Instalação e configuração do PostgreSQL;
- Configuração do Spark e Hadoop;
- Integração via JDBC;
- Ajustes de variáveis de ambiente.

Esse processo demanda tempo até que todas as ferramentas estejam funcionando de forma integrada e em sincronia com o projeto.

Caso houvesse mais tempo para evolução da POC, poderiam ser implementadas melhorias adicionais, como:

- Criação de um novo critério de teste unitário na camada **Gold**, validando se todos os *joins* entre as tabelas foram concluídos corretamente;
- Implementação de testes unitários também na camada **Bronze**, dependendo da criticidade e da qualidade da origem dos dados;
- Expansão das regras de Data Quality conforme a necessidade do negócio.

Cada projeto possui um nível diferente de criticidade e exigência de qualidade de dados, sendo fundamental adaptar as validações conforme o contexto e o impacto no ambiente de produção.

