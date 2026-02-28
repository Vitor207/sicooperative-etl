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


def validar_qualidade_df(
    df,
    nome_tabela,
    pk_col,
    colunas_valor=None,
    coluna_idade=None,
    coluna_email=None,
    emails_bloqueados=None,
    colunas_tamanho_minimo=None,
):
    """Valida regras basicas de qualidade de dados por tabela."""
    erros = []

    # Valida base nao vazia para evitar saida em branco.
    qtd_registros = df.count()
    if qtd_registros == 0:
        erros.append("Base vazia (0 registros)")

    # Valida PK duplicada.
    qtd_pk_duplicada = df.groupBy(pk_col).count().filter(F.col("count") > 1).count()
    if qtd_pk_duplicada > 0:
        erros.append(f"PK duplicada: {qtd_pk_duplicada} chave(s)")

    # Valida nulos por coluna.
    agregacoes_nulos = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
    ]
    nulos = df.agg(*agregacoes_nulos).collect()[0].asDict()
    nulos_detectados = {col: qtd for col, qtd in nulos.items() if qtd > 0}
    if nulos_detectados:
        erros.append(f"Nulos por coluna: {nulos_detectados}")

    # Valida valores zerados para colunas de valor.
    colunas_valor = colunas_valor or []
    zeros_detectados = {}
    for col in colunas_valor:
        qtd_zero = df.filter(F.col(col) == 0).count()
        if qtd_zero > 0:
            zeros_detectados[col] = qtd_zero
    if zeros_detectados:
        erros.append(f"Valores zerados: {zeros_detectados}")

    # Valida faixa de idade entre 0 e 120.
    if coluna_idade:
        qtd_idade_invalida = df.filter(
            (F.col(coluna_idade) < 0) | (F.col(coluna_idade) > 120)
        ).count()
        if qtd_idade_invalida > 0:
            erros.append(f"Idade fora da faixa 0-120: {qtd_idade_invalida} registro(s)")

    # Valida email no formato basico e bloqueia emails placeholder conhecidos.
    if coluna_email:
        emails_bloqueados = set(e.lower() for e in (emails_bloqueados or []))
        regex_email = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        df_email = df.withColumn("_email_norm", F.lower(F.trim(F.col(coluna_email))))
        qtd_email_invalido = df_email.filter(
            (~F.col("_email_norm").rlike(regex_email))
            | (F.col("_email_norm").isin(list(emails_bloqueados)))
        ).count()
        if qtd_email_invalido > 0:
            erros.append(f"Email invalido/bloqueado: {qtd_email_invalido} registro(s)")

    # Valida tamanho minimo de texto por coluna (ex.: num_cartao >= 16).
    colunas_tamanho_minimo = colunas_tamanho_minimo or {}
    tamanho_invalido = {}
    for col, tamanho_minimo in colunas_tamanho_minimo.items():
        qtd_tamanho_invalido = df.filter(F.length(F.col(col).cast("string")) < tamanho_minimo).count()
        if qtd_tamanho_invalido > 0:
            tamanho_invalido[col] = qtd_tamanho_invalido
    if tamanho_invalido:
        erros.append(f"Tamanho minimo invalido: {tamanho_invalido}")

    if erros:
        raise ValueError(
            f"Falha na qualidade de dados da tabela {nome_tabela}: " + " | ".join(erros)
        )

    print(f"[OK] Qualidade validada para {nome_tabela}")


# Extrai as tabelas de origem do PostgreSQL.
df_associado = spark.read.jdbc(url=JDBC_URL, table="associado", properties=JDBC_PROPERTIES)
df_conta = spark.read.jdbc(url=JDBC_URL, table="conta", properties=JDBC_PROPERTIES)
df_cartao = spark.read.jdbc(url=JDBC_URL, table="cartao", properties=JDBC_PROPERTIES)
df_movimento = spark.read.jdbc(url=JDBC_URL, table="movimento", properties=JDBC_PROPERTIES)

# Valida qualidade nas tabelas de origem.
validar_qualidade_df(
    df_associado,
    "associado",
    pk_col="id",
    coluna_idade="idade",
    coluna_email="email",
    emails_bloqueados=["sememail@email.com"],
)
validar_qualidade_df(df_conta, "conta", pk_col="id")
validar_qualidade_df(
    df_cartao,
    "cartao",
    pk_col="id",
    colunas_tamanho_minimo={"num_cartao": 16},
)
validar_qualidade_df(df_movimento, "movimento", pk_col="id", colunas_valor=["vlr_transacao"])

# Transforma para o modelo flat a tabela final.
movimento_flat = (
    df_movimento.alias("m")
    .join(df_cartao.alias("c"), F.col("m.id_cartao") == F.col("c.id"), "inner")
    .join(df_conta.alias("co"), F.col("c.id_conta") == F.col("co.id"), "inner")
    .join(df_associado.alias("a"), F.col("co.id_associado") == F.col("a.id"), "inner")
    .select(
        F.col("a.nome").alias("Nome_associado"),
        F.col("a.sobrenome").alias("Sobrenome_associado"),
        F.col("a.idade").alias("idade_associado"),
        F.col("m.vlr_transacao").alias("Vlr_transacao_movimento"),
        F.col("m.des_transacao").alias("Des_transacao_movimento"),
        F.col("m.data_movimento").alias("Data_movimento"),
        F.col("c.num_cartao").alias("Numero_cartao"),
        F.col("c.nom_impresso").alias("Nome_impresso_cartao"),
        F.col("c.data_criacao_cartao").alias("Data_criacao_cartao"),
        F.col("co.tipo").alias("Tipo_conta"),
        F.col("co.data_criacao_conta").alias("Data_criacao_conta"),
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

# Encerra o Spark.
spark.stop()
