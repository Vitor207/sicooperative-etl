import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variaveis de ambiente do arquivo .env.
load_dotenv()

# Configuracoes de conexao com o banco de dados.
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Destinos de saida para os artefatos finais de dados.
OUTPUT_PATH = os.getenv(
    "OUTPUT_PATH", r"C:\Users\User\OneDrive\Documentos\sicooperative-etl\base_final"
)
MOVIMENTO_FLAT_TABLE = os.getenv("MOVIMENTO_FLAT_TABLE", "movimento_flat")

# Configuracoes de execucao do Spark.
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "movimento-flat-etl")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_JARS = os.getenv("SPARK_JARS", r"C:\spark\jars\postgresql-42.7.2.jar")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "ERROR")

# URI absoluta para o arquivo local de configuracao do Log4j2.
LOG4J2_FILE = (Path(__file__).resolve().parent / "log4j2.properties").as_uri()

# Configuracoes extras do Spark aplicadas no builder da SparkSession.
SPARK_CONFIGS = {
    "spark.jars": SPARK_JARS,
    "spark.ui.showConsoleProgress": "false",
    "spark.log.level": SPARK_LOG_LEVEL,
    "spark.driver.extraJavaOptions": f"-Dlog4j.configurationFile={LOG4J2_FILE}",
    "spark.executor.extraJavaOptions": f"-Dlog4j.configurationFile={LOG4J2_FILE}",
    "spark.hadoop.io.native.lib.available": "false",
    "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
    "spark.hadoop.fs.file.impl.disable.cache": "true",
}

# URL JDBC e propriedades de conexao.
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}
