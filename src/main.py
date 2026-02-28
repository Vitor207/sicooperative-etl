import os
import shutil
import time
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
    colunas_obrigatorias=None,
    colunas_numericas_obrigatorias=None,
    colunas_valor=None,
    coluna_idade=None,
    coluna_email=None,
    emails_bloqueados=None,
    colunas_tamanho_minimo=None,
):
    """Valida regras basicas de qualidade de dados por tabela."""
    erros = []
    colunas_obrigatorias = colunas_obrigatorias or []
    colunas_numericas_obrigatorias = colunas_numericas_obrigatorias or []
    colunas_valor = colunas_valor or []
    colunas_tamanho_minimo = colunas_tamanho_minimo or {}
    emails_bloqueados = set(e.lower() for e in (emails_bloqueados or []))

    df_val = df.cache()
    try:
        # Consolidacao de contagens para reduzir numero de actions no Spark.
        agg_exprs = [
            F.count(F.lit(1)).alias("_row_count"),
            F.countDistinct(F.col(pk_col)).alias("_pk_distinct"),
        ]

        for col in colunas_obrigatorias:
            agg_exprs.append(
                F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"_null_{col}")
            )

        for col in colunas_numericas_obrigatorias:
            agg_exprs.append(
                F.sum(
                    F.when(
                        F.col(col).isNull()
                        | F.col(col).cast("long").isNull()
                        | (F.col(col).cast("long") <= 0),
                        1,
                    ).otherwise(0)
                ).alias(f"_num_invalid_{col}")
            )

        for col in colunas_valor:
            agg_exprs.append(
                F.sum(F.when(F.col(col) == 0, 1).otherwise(0)).alias(f"_zero_{col}")
            )

        for col, tamanho_minimo in colunas_tamanho_minimo.items():
            agg_exprs.append(
                F.sum(
                    F.when(F.length(F.col(col).cast("string")) < tamanho_minimo, 1).otherwise(0)
                ).alias(f"_minlen_{col}")
            )

        if coluna_idade:
            agg_exprs.append(
                F.sum(
                    F.when((F.col(coluna_idade) < 0) | (F.col(coluna_idade) > 120), 1).otherwise(0)
                ).alias("_idade_invalida")
            )

        if coluna_email:
            regex_email = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            email_norm = F.lower(F.trim(F.col(coluna_email)))
            agg_exprs.append(
                F.sum(
                    F.when((~email_norm.rlike(regex_email)) | email_norm.isin(list(emails_bloqueados)), 1).otherwise(0)
                ).alias("_email_invalido")
            )

        metricas = df_val.agg(*agg_exprs).collect()[0].asDict()

        # Base nao vazia.
        qtd_registros = metricas["_row_count"]
        if qtd_registros == 0:
            erros.append("Base vazia (0 registros)")

        # PK duplicada: calcula detalhamento apenas se necessario.
        if qtd_registros > metricas["_pk_distinct"]:
            qtd_pk_duplicada = df_val.groupBy(pk_col).count().filter(F.col("count") > 1).count()
            erros.append(f"PK duplicada: {qtd_pk_duplicada} chave(s)")

        nulos_detectados = {
            col: metricas[f"_null_{col}"] for col in colunas_obrigatorias if metricas[f"_null_{col}"] > 0
        }
        if nulos_detectados:
            erros.append(f"Nulos em colunas obrigatorias: {nulos_detectados}")

        numeros_invalidos = {
            col: metricas[f"_num_invalid_{col}"]
            for col in colunas_numericas_obrigatorias
            if metricas[f"_num_invalid_{col}"] > 0
        }
        if numeros_invalidos:
            erros.append(f"Numeros invalidos em colunas obrigatorias: {numeros_invalidos}")

        zeros_detectados = {
            col: metricas[f"_zero_{col}"] for col in colunas_valor if metricas[f"_zero_{col}"] > 0
        }
        if zeros_detectados:
            erros.append(f"Valores zerados: {zeros_detectados}")

        tamanho_invalido = {
            col: metricas[f"_minlen_{col}"]
            for col in colunas_tamanho_minimo
            if metricas[f"_minlen_{col}"] > 0
        }
        if tamanho_invalido:
            erros.append(f"Tamanho minimo invalido: {tamanho_invalido}")

        if coluna_idade and metricas.get("_idade_invalida", 0) > 0:
            erros.append(f"Idade fora da faixa 0-120: {metricas['_idade_invalida']} registro(s)")

        if coluna_email and metricas.get("_email_invalido", 0) > 0:
            erros.append(f"Email invalido/bloqueado: {metricas['_email_invalido']} registro(s)")

        if erros:
            raise ValueError(
                f"Falha na qualidade de dados da tabela {nome_tabela}: " + " | ".join(erros)
            )

        print(f"[OK] Qualidade validada para {nome_tabela}")
    finally:
        df_val.unpersist()


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
    colunas_obrigatorias=["id", "nome", "sobrenome", "idade", "email"],
    colunas_numericas_obrigatorias=["id"],
    coluna_idade="idade",
    coluna_email="email",
    emails_bloqueados=["sememail@email.com"],
)
validar_qualidade_df(
    df_conta,
    "conta",
    pk_col="id",
    colunas_obrigatorias=["id", "tipo", "id_associado"],
    colunas_numericas_obrigatorias=["id", "id_associado"],
)
validar_qualidade_df(
    df_cartao,
    "cartao",
    pk_col="id",
    colunas_obrigatorias=["id", "num_cartao", "id_conta"],
    colunas_numericas_obrigatorias=["id", "id_conta"],
    colunas_tamanho_minimo={"num_cartao": 16},
)
validar_qualidade_df(
    df_movimento,
    "movimento",
    pk_col="id",
    colunas_obrigatorias=["id", "vlr_transacao", "id_cartao"],
    colunas_numericas_obrigatorias=["id", "id_cartao"],
    colunas_valor=["vlr_transacao"],
)

# Transforma para o modelo flat a tabela final.
movimento_flat = (
    df_movimento.alias("m")
    .join(df_cartao.alias("c"), F.col("m.id_cartao") == F.col("c.id"), "left")
    .join(df_conta.alias("co"), F.col("c.id_conta") == F.col("co.id"), "left")
    .join(df_associado.alias("a"), F.col("co.id_associado") == F.col("a.id"), "left")
    .select(
        F.initcap(F.trim(F.col("a.nome"))).cast("string").alias("Nome_associado"),
        F.initcap(F.trim(F.col("a.sobrenome"))).cast("string").alias("Sobrenome_associado"),
        F.col("a.idade").cast("int").alias("Idade_associado"),
        F.col("m.vlr_transacao").cast("decimal(18,2)").alias("Vlr_transacao_movimento"),
        F.initcap(F.trim(F.col("m.des_transacao"))).cast("string").alias("Des_transacao_movimento"),
        F.date_format(F.col("m.data_movimento"), "dd/MM/yyyy").alias("Data_movimento"),
        F.concat(
            F.substring(F.col("c.num_cartao").cast("string"), 1, 4),
            F.lit("********"),
            F.expr("right(cast(c.num_cartao as string), 4)"),
        ).cast("string").alias("Numero_cartao"),
        F.initcap(F.trim(F.col("c.nom_impresso"))).cast("string").alias("Nome_impresso_cartao"),
        F.date_format(F.col("c.data_criacao_cartao"), "dd/MM/yyyy").alias("Data_criacao_cartao"),
        F.initcap(F.trim(F.col("co.tipo"))).cast("string").alias("Tipo_conta"),
        F.date_format(F.col("co.data_criacao_conta"), "dd/MM/yyyy").alias("Data_criacao_conta"),
        F.when(
            F.col("c.id").isNull() | F.col("co.id").isNull() | F.col("a.id").isNull(),
            F.lit(1),
        ).otherwise(F.lit(0)).cast("int").alias("flag_orfao_referencia"),
    )
)

# Carrega no PostgreSQL na tabela final.
# movimento_flat.write.jdbc(
#     url=JDBC_URL,
#     table=MOVIMENTO_FLAT_TABLE,
#     mode="overwrite",
#     properties=JDBC_PROPERTIES,
# )

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
destino_csv = final_csv
if final_csv.exists():
    for tentativa in range(5):
        try:
            final_csv.unlink()
            break
        except PermissionError:
            if tentativa == 4:
                destino_csv = output_dir / f"movimento_flat_{int(time.time())}.csv"
                print(
                    f"Arquivo bloqueado. Salvando com nome alternativo: {destino_csv.name}"
                )
                break
            time.sleep(1)
shutil.move(str(part_files[0]), str(destino_csv))
shutil.rmtree(spark_output_dir, ignore_errors=True)

# Exibe uma amostra para validacao rapida.
movimento_flat.show(20, truncate=False)

# Encerra o Spark.
spark.stop()
