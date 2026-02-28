import os
import shutil
from pathlib import Path

# Sessao Spark e funcoes SQL auxiliares.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuracoes centralizadas de execucao e conexao.
from config import (
    JDBC_URL,
    JDBC_PROPERTIES,
    MOVIMENTO_FLAT_TABLE,
    OUTPUT_PATH,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_CONFIGS,
)

# Configura Hadoop nativo no Windows para escrita local via Spark.
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

# Monta a sessao Spark com configuracoes basicas da aplicacao.
builder = SparkSession.builder.appName(SPARK_APP_NAME).master(SPARK_MASTER)

# Aplica configuracoes extras do Spark (jar JDBC, logs etc.).
for key, value in SPARK_CONFIGS.items():
    builder = builder.config(key, value)

# Inicia a sessao Spark.
spark = builder.getOrCreate()

# Garantia adicional para reduzir verbosidade dos logs do Spark.
spark.sparkContext.setLogLevel("ERROR")

# Extrai as tabelas de origem do PostgreSQL.
df_associado = spark.read.jdbc(url=JDBC_URL, table="associado", properties=JDBC_PROPERTIES)
df_conta = spark.read.jdbc(url=JDBC_URL, table="conta", properties=JDBC_PROPERTIES)
df_cartao = spark.read.jdbc(url=JDBC_URL, table="cartao", properties=JDBC_PROPERTIES)
df_movimento = spark.read.jdbc(url=JDBC_URL, table="movimento", properties=JDBC_PROPERTIES)

# Transforma para o modelo flat exigido na entrega.
movimento_flat = (
    df_movimento.alias("m")
    .join(df_cartao.alias("c"), F.col("m.id_cartao") == F.col("c.id"), "inner")
    .join(df_conta.alias("co"), F.col("c.id_conta") == F.col("co.id"), "inner")
    .join(df_associado.alias("a"), F.col("co.id_associado") == F.col("a.id"), "inner")
    .select(
        F.col("a.nome").alias("nome_associado"),
        F.col("a.sobrenome").alias("sobrenome_associado"),
        F.col("a.idade").alias("idade_associado"),
        F.col("m.vlr_transacao").alias("vlr_transacao_movimento"),
        F.col("m.des_transacao").alias("des_transacao_movimento"),
        F.col("m.data_movimento").alias("data_movimento"),
        F.col("c.num_cartao").alias("numero_cartao"),
        F.col("c.nom_impresso").alias("nome_impresso_cartao"),
        F.col("c.data_criacao_cartao").alias("data_criacao_cartao"),
        F.col("co.tipo").alias("tipo_conta"),
        F.col("co.data_criacao_conta").alias("data_criacao_conta"),
    )
)

# Carrega no PostgreSQL na tabela final.
movimento_flat.write.jdbc(
    url=JDBC_URL,
    table=MOVIMENTO_FLAT_TABLE,
    mode="overwrite",
    properties=JDBC_PROPERTIES,
)

# Prepara o caminho local de saida para exportacao CSV.
output_dir = Path(OUTPUT_PATH)
output_dir.mkdir(parents=True, exist_ok=True)
for stale_file in output_dir.glob("part-*.csv"):
    stale_file.unlink(missing_ok=True)
(output_dir / "_SUCCESS").unlink(missing_ok=True)

# Exporta via Spark para pasta temporaria.
spark_output_dir = output_dir / "_spark_movimento_flat"
movimento_flat.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(str(spark_output_dir))

# Renomeia o arquivo part-*.csv para movimento_flat.csv.
part_files = list(spark_output_dir.glob("part-*.csv"))
if not part_files:
    raise FileNotFoundError("Arquivo CSV gerado pelo Spark nao foi encontrado.")

final_csv = output_dir / "movimento_flat.csv"
if final_csv.exists():
    final_csv.unlink()
shutil.move(str(part_files[0]), str(final_csv))
shutil.rmtree(spark_output_dir, ignore_errors=True)

# Exibe uma amostra para validacao rapida.
movimento_flat.show(20, truncate=False)

# Encerra o Spark e libera recursos.
spark.stop()
