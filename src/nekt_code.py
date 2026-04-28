## IMPORTS
import nekt
from typing         import Optional 
from pyspark.sql    import DataFrame, Column 
from pyspark.sql    import functions as F

## HELPER FUNCTIONS
def extract_nekt_table(layer_name: str, table_name: str) -> DataFrame:
    """Simplify nekt table extraction syntax."""
    return nekt.load_table(layer_name=layer_name, table_name=table_name)

def save_nekt_table(
    df: DataFrame, 
    layer_name: str, 
    table_name: str,
    folder_name: Optional[str] = None 
):
    """Simplify nekt table saving syntax."""
    nekt.save_table(
        df=df,
        layer_name=layer_name,
        table_name=table_name,
        folder_name=folder_name
    )

def ms_to_timestamp(col_name: str) -> Column:
    """Converts a Unix millisecond epoch column to a TimestampType."""
    return F.to_timestamp(F.col(col_name).cast("long") / 1000)

## EXTRACTING TABLES
# clickup - bronze tables
df_bronze_clickup_users                 = extract_nekt_table("Bronze", "studio61_clickup_bronze_users")
df_bronze_clickup_spaces                = extract_nekt_table("Bronze", "studio61_clickup_bronze_spaces" )
df_bronze_clickup_time_entries          = extract_nekt_table("Bronze", "studio61_clickup_bronze_time_entries")

# conta azul - bronze tables
df_bronze_contaazul_accounts_payable    = extract_nekt_table("Bronze", "studio61_contaazul_bronze_accounts_payable")
df_bronze_contaazul_accounts_receivable = extract_nekt_table("Bronze", "studio61_contaazul_bronze_accounts_receivable")
df_bronze_contaazul_categories          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_categories")
df_bronze_contaazul_financial_accounts  = extract_nekt_table("Bronze", "studio61_contaazul_bronze_financial_accounts")
df_bronze_contaazul_installments        = extract_nekt_table("Bronze", "studio61_contaazul_bronze_installments")
df_bronze_contaazul_sales_list          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_sales_list")
df_bronze_contaazul_sales_details       = extract_nekt_table("Bronze", "studio61_contaazul_bronze_sales_details")

## TRANSFORMING TABLES
# clickup - users
df_silver_clickup_users = (
    df_bronze_clickup_users
    .select(
        F.col("id")         .cast("integer").alias("user_id"),
        F.col("username")   .cast("string").alias("user_name")
    )
    .filter(
        F.col("user_name").isNotNull()
    )
    .dropDuplicates(
        ["user_id"]
    )
)

# clickup - spaces
df_silver_clickup_spaces = (
    df_bronze_clickup_spaces
    .select(
        F.col("id")     .cast("long").alias("space_id"),
        F.col("name")   .cast("string").alias("space_name")
    )
    .filter(
        F.col("space_name").isNotNull()
    )
    .dropDuplicates(
        ["space_id"]
    )
)

# clickup - time entries
df_silver_clickup_time_entries = (
    df_bronze_clickup_time_entries
    .select(
        F.col("id")                     .cast("long").alias("interval_id"),
        F.col("wid")                    .cast("integer").alias("team_id"),
        F.col("user.id")                .cast("integer").alias("user_id"),
        F.col("user.username")          .cast("string").alias("user_name"),
        F.col("task_location.space_id") .cast("long").alias("space_id"),
        F.col("task.id")                .cast("string").alias("task_id"),
        F.col("start")                  .cast("long").alias("interval_date_start_ms"),
        ms_to_timestamp("start")        .cast("string").alias("interval_date_start_iso"),
        F.col("end")                    .cast("long").alias("interval_date_end_ms"),
        ms_to_timestamp("end")          .cast("string").alias("interval_date_end_iso"),
        F.col("at")                     .cast("long").alias("interval_date_added_ms"),
        ms_to_timestamp("at")           .cast("string").alias("interval_date_added_iso"),
    )
    .filter(
        F.col("interval_id").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("interval_date_start_ms").isNotNull() &
        F.col("interval_date_end_ms").isNotNull() &
        (F.col("interval_date_end_ms").cast("long") > F.col("interval_date_start_ms").cast("long"))
    )
    .dropDuplicates(
        ["interval_id"]
    )
)

# conta azul - accounts payable (expenses)
df_silver_contaazul_accounts_payable = (
    df_bronze_contaazul_accounts_payable
    .filter(
        F.col("id").isNotNull() &
        F.col("data_criacao").isNotNull()
    )
    .withColumn(
        "categoria", F.explode("categorias")
    )
    .select(
        F.col("id")                 .cast("string").alias("id"),
        F.col("descricao")          .cast("string").alias("descricao"),
        F.col("data_vencimento")    .cast("string").alias("data_vencimento"),
        F.col("status")             .cast("string").alias("status"),
        F.col("total")              .cast("float") .alias("total"),
        F.col("nao_pago")           .cast("float") .alias("nao_pago"),
        F.col("pago")               .cast("float") .alias("pago"),
        F.col("data_criacao")       .cast("string").alias("data_criacao"),
        F.col("data_alteracao")     .cast("string").alias("data_alteracao"),
        F.col("categoria.id")       .cast("string").alias("categoria_principal_id"),
        F.col("categoria.nome")     .cast("string").alias("categoria_principal_nome"),
        F.col("fornecedor.id")      .cast("string").alias("fornecedor_id"),
        F.col("fornecedor.nome")    .cast("string").alias("fornecedor_nome"),
        F.current_timestamp()       .cast("string").alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - accounts receivable (revenues)
df_silver_contaazul_accounts_receivable = (
    df_bronze_contaazul_accounts_receivable
    .filter(
        F.col("id").isNotNull() &
        F.col("data_criacao").isNotNull()
    )
    .withColumn(
        "categoria", F.explode("categorias")
    )
    .select(
        F.col("id")                 .cast("string").alias("id"),
        F.col("descricao")          .cast("string").alias("descricao"),
        F.col("data_vencimento")    .cast("string").alias("data_vencimento"),
        F.col("status")             .cast("string").alias("status"),
        F.col("total")              .cast("float") .alias("total"),
        F.col("nao_pago")           .cast("float") .alias("nao_pago"),
        F.col("pago")               .cast("float") .alias("pago"),
        F.col("data_criacao")       .cast("string").alias("data_criacao"),
        F.col("data_alteracao")     .cast("string").alias("data_alteracao"),
        F.col("categoria.id")       .cast("string").alias("categoria_principal_id"),
        F.col("categoria.nome")     .cast("string").alias("categoria_principal_nome"),
        F.col("cliente.id")         .cast("string").alias("cliente_id"),
        F.col("cliente.nome")       .cast("string").alias("cliente_nome"),
        F.current_timestamp()       .cast("string").alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - categories   
df_silver_contaazul_categories = (
    df_bronze_contaazul_categories
    .select(
        F.col("id")                     .cast("string") .alias("id"),
        F.col("nome")                   .cast("string") .alias("nome"),
        F.col("versao")                 .cast("integer").alias("versao"),
        F.col("categoria_pai")          .cast("string") .alias("categoria_pai"),
        F.col("tipo")                   .cast("string") .alias("tipo"),
        F.col("entrada_dre")            .cast("string") .alias("entrada_dre"),
        F.col("considera_custo_dre")    .cast("boolean").alias("considera_custo_dre"),
        F.current_timestamp()           .cast("string").alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - customers
df_silver_contaazul_customers = (
    df_bronze_contaazul_sales_details.alias("sd")
    .join(
        df_bronze_contaazul_sales_list.alias("sl"),
        on=F.col("sd.cliente.uuid") == F.col("sl.cliente.id"),
        how="inner",
    )
    .select(
        F.col("sd.cliente.uuid")        .alias("id"),
        F.col("sd.cliente.nome")        .alias("name"),
        F.col("sd.cliente.tipo_pessoa") .alias("person_type"),
        F.col("sl.cliente.email")       .alias("email"),
        F.col("sl.cliente.telefone")    .alias("phone"),
        F.col("sd.cliente.documento")   .alias("document"),
        F.col("sl.cliente.cep")         .alias("zip_code"),
        F.col("sl.cliente.cidade")      .alias("city"),
        F.col("sl.cliente.estado")      .alias("state"),
        F.col("sl.cliente.endereco")    .alias("address"),
        F.col("sl.cliente.pais")        .alias("country"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - parent categories
df_parent_categories = (
    df_bronze_contaazul_categories
    .join(
        df_bronze_contaazul_categories
        .filter(
            F.col("categoria_pai").isNotNull()
        )
        .select(
            F.col("categoria_pai").alias("id")
        )
        .distinct(),
        on="id",
        how="inner",
    )
)

# conta azul - combined accounts
df_silver_contaazul_combined_accounts = (
    df_silver_contaazul_accounts_payable
    .withColumn(
        "tipo", F.lit("D").cast("string")
    )
    .unionByName(
        df_silver_contaazul_accounts_receivable
        .withColumn(
            "tipo", F.lit("R").cast("string")
        ),
        allowMissingColumns=True,
    )
    .join(
        # first join — resolve categoria_pai_id from child category
        df_silver_contaazul_categories.select(
            F.col("id")             .cast("string").alias("categoria_principal_id"),
            F.col("categoria_pai")  .cast("string").alias("categoria_pai_id"),
        ),
        on="categoria_principal_id",
        how="left",
    )
    .join(
        # second join — resolve categoria_pai_nome from parent category
        df_parent_categories.select(
            F.col("categoria_pai_id")   .cast("string").alias("categoria_pai_id"),
            F.col("categoria_pai_nome") .cast("string").alias("categoria_pai_nome"),
        ),
        on="categoria_pai_id",
        how="left",
    )
)

# conta azul - dre_financial_categories - to develop
    
# conta azul - dre_items                - to develop

# conta azul - dre_subitems             - to develop

# conta azul - financial_accounts
df_silver_contaazul_financial_accounts = (
    df_bronze_contaazul_financial_accounts
    .filter(
        F.col("conta_financeira.id").isNotNull()
    )
    .select(
        F.col("id")                             .cast("string") .alias("id"),
        F.col("banco")                          .cast("string") .alias("banco"),
        F.col("codigo_banco")                   .cast("integer").alias("codigo_banco"),
        F.col("nome")                           .cast("string") .alias("nome"),
        F.col("ativo")                          .cast("boolean").alias("ativo"),
        F.col("tipo")                           .cast("string") .alias("tipo"),
        F.col("conta_padrao")                   .cast("boolean").alias("conta_padrao"),
        F.col("possui_config_boleto_bancario")  .cast("boolean").alias("possui_config_boleto_bancario"),
        F.col("agencia")                        .cast("string") .alias("agencia"),
        F.col("numero")                         .cast("string") .alias("numero"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - installments
df_silver_contaazul_installments = (
    df_bronze_contaazul_installments
    .select(
        F.col("id")                        .alias("parcela_id"),
        F.col("status")                    .alias("parcela_status"),
        F.col("evento.condicao_pagamento") .alias("condicao_pagamento"),
        F.col("referencia"),
        F.col("evento.agendado")           .alias("agendado"),
        F.col("evento.tipo")               .alias("tipo_evento"),
        F.current_timestamp()              .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["parcela_id"]
    )
)

## LOADING TABLES
# clickup - silver tables
# save_nekt_table(df_silver_clickup_users,                    "Silver", "studio61_clickup_silver_users",                  folder_name="studio61_clickup_silver")
# save_nekt_table(df_silver_clickup_spaces,                   "Silver", "studio61_clickup_silver_spaces",                 folder_name="studio61_clickup_silver")
# save_nekt_table(df_silver_clickup_time_entries,             "Silver", "studio61_clickup_silver_time_entries",           folder_name="studio61_clickup_silver")

# conta azul - silver tables
# save_nekt_table(df_silver_contaazul_accounts_payable,       "Silver", "studio61_contaazul_silver_accounts_payable",     folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_accounts_receivable,    "Silver", "studio61_contaazul_silver_accounts_receivable",  folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_categories,             "Silver", "studio61_contaazul_silver_categories",           folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_customers,              "Silver", "studio61_contaazul_silver_customers",            folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_combined_accounts,      "Silver", "studio61_contaazul_silver_combined_accounts",    folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_financial_accounts,     "Silver", "studio61_contaazul_silver_financial_accounts",   folder_name="studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_installments,           "Silver", "studio61_contaazul_silver_installments",         folder_name="studio61_contaazul_silver")