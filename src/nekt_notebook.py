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

def last_element(array_col: str, field: str) -> F.Column:
    """Safely retrieves a field from the last element of an array column."""
    return (
        F.when(
            F.size(F.col(array_col)) > 0,
            F.element_at(F.col(array_col), F.size(F.col(array_col)))[field]
        ).otherwise(F.lit(None))
    )

## EXTRACTING TABLES
# clickup - bronze tables
df_bronze_clickup_users                 = extract_nekt_table("Bronze", "studio61_clickup_bronze_users")
df_bronze_clickup_spaces                = extract_nekt_table("Bronze", "studio61_clickup_bronze_spaces" )
df_bronze_clickup_time_entries          = extract_nekt_table("Bronze", "studio61_clickup_bronze_time_entries")

# conta azul - bronze tables
df_bronze_contaazul_accounts_payable    = extract_nekt_table("Bronze", "studio61_contaazul_bronze_despesas")
df_bronze_contaazul_accounts_receivable = extract_nekt_table("Bronze", "studio61_contaazul_bronze_receitas")
df_bronze_contaazul_categories          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_categorias")
df_bronze_contaazul_dre_categories      = extract_nekt_table("Bronze", "studio61_contaazul_bronze_categorias_dre")
# df_bronze_contaazul_financial_accounts  = extract_nekt_table("Bronze", "studio61_contaazul_bronze_contas_financeiras")
df_bronze_contaazul_installments        = extract_nekt_table("Bronze", "studio61_contaazul_bronze_parcelas")
df_bronze_contaazul_sales_list          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_vendas_lista")
df_bronze_contaazul_sales_details       = extract_nekt_table("Bronze", "studio61_contaazul_bronze_vendas_detalhes")

## TRANSFORMING TABLES
# clickup - users
df_silver_clickup_users = (
    df_bronze_clickup_users
    .filter(
        F.col("username").isNotNull()
    )
    .select(
        F.col("id")         .cast("integer").alias("user_id"),
        F.col("username")   .cast("string") .alias("user_name")
    )
    
    .dropDuplicates(
        ["user_id"]
    )
)

# clickup - spaces
df_silver_clickup_spaces = (
    df_bronze_clickup_spaces
    .filter(
        F.col("name").isNotNull()
    )
    .select(
        F.col("id")     .cast("long")   .alias("space_id"),
        F.col("name")   .cast("string") .alias("space_name")
    )
    .dropDuplicates(
        ["space_id"]
    )
)

# clickup - time entries
df_silver_clickup_time_entries = (
    df_bronze_clickup_time_entries
    .filter(
        F.col("id").isNotNull() &
        F.col("user.id").isNotNull() &
        F.col("start").isNotNull() &
        F.col("end").isNotNull() &
        (F.col("end") > F.col("start"))
    )
    .select(
        F.col("id")                     .cast("long")   .alias("interval_id"),
        F.col("wid")                    .cast("integer").alias("team_id"),
        F.col("user.id")                .cast("integer").alias("user_id"),
        F.col("user.username")          .cast("string") .alias("user_name"),
        F.col("task_location.space_id") .cast("long")   .alias("space_id"),
        F.col("task.id")                .cast("string") .alias("task_id"),
        F.col("start")                  .cast("long")   .alias("interval_date_start_ms"),
        ms_to_timestamp("start")        .cast("string") .alias("interval_date_start_iso"),
        F.col("end")                    .cast("long")   .alias("interval_date_end_ms"),
        ms_to_timestamp("end")          .cast("string") .alias("interval_date_end_iso"),
        F.col("at")                     .cast("long")   .alias("interval_date_added_ms"),
        ms_to_timestamp("at")           .cast("string") .alias("interval_date_added_iso"),
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
        F.col("id")             .cast("string") .alias("id"),
        F.col("descricao")      .cast("string") .alias("descricao"),
        F.col("data_vencimento").cast("string") .alias("data_vencimento"),
        F.col("status")         .cast("string") .alias("status"),
        F.col("total")          .cast("float")  .alias("total"),
        F.col("nao_pago")       .cast("float")  .alias("nao_pago"),
        F.col("pago")           .cast("float")  .alias("pago"),
        F.col("data_criacao")   .cast("string") .alias("data_criacao"),
        F.col("data_alteracao") .cast("string") .alias("data_alteracao"),
        F.col("categoria.id")   .cast("string") .alias("categoria_principal_id"),
        F.col("categoria.nome") .cast("string") .alias("categoria_principal_nome"),
        F.col("fornecedor.id")  .cast("string") .alias("fornecedor_id"),
        F.col("fornecedor.nome").cast("string") .alias("fornecedor_nome"),
        F.current_timestamp()   .cast("string") .alias("_loaded_at"),
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
        F.col("id")             .cast("string") .alias("id"),
        F.col("descricao")      .cast("string") .alias("descricao"),
        F.col("data_vencimento").cast("string") .alias("data_vencimento"),
        F.col("status")         .cast("string") .alias("status"),
        F.col("total")          .cast("float")  .alias("total"),
        F.col("nao_pago")       .cast("float")  .alias("nao_pago"),
        F.col("pago")           .cast("float")  .alias("pago"),
        F.col("data_criacao")   .cast("string") .alias("data_criacao"),
        F.col("data_alteracao") .cast("string") .alias("data_alteracao"),
        F.col("categoria.id")   .cast("string") .alias("categoria_principal_id"),
        F.col("categoria.nome") .cast("string") .alias("categoria_principal_nome"),
        F.col("cliente.id")     .cast("string") .alias("cliente_id"),
        F.col("cliente.nome")   .cast("string") .alias("cliente_nome"),
        F.current_timestamp()   .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - categories   
df_silver_contaazul_categories = (
    df_bronze_contaazul_categories
    .select(
        F.col("id")                 .cast("string") .alias("id"),
        F.col("nome")               .cast("string") .alias("nome"),
        F.col("versao")             .cast("integer").alias("versao"),
        F.col("categoria_pai")      .cast("string") .alias("categoria_pai"),
        F.col("tipo")               .cast("string") .alias("tipo"),
        F.col("entrada_dre")        .cast("string") .alias("entrada_dre"),
        F.col("considera_custo_dre").cast("boolean").alias("considera_custo_dre"),
        F.current_timestamp()       .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - customers - TO DO
# df_silver_contaazul_customers = (
#     df_bronze_contaazul_sales_details.alias("sd")
#     .join(
#         df_bronze_contaazul_sales_list.alias("sl"),
#         on=F.col("sd.cliente.uuid") == F.col("sl.cliente.id"),
#         how="inner",
#     )
#     .select(
#         F.col("sd.cliente.uuid")                        .alias("id"),
#         F.col("sd.cliente.nome")                        .alias("name"),
#         F.col("sd.cliente.tipo_pessoa")                 .alias("person_type"),
#         F.col("sl.cliente.email")                       .alias("email"),
#         F.col("sl.cliente.telefone")                    .alias("phone"),
#         F.col("sd.cliente.documento")                   .alias("document"),
#         F.col("sl.cliente.cep")                         .alias("zip_code"),
#         F.col("sl.cliente.cidade")                      .alias("city"),
#         F.col("sl.cliente.estado")                      .alias("state"),
#         F.col("sl.cliente.endereco")                    .alias("address"),
#         F.col("sl.cliente.pais")                        .alias("country"),
#         F.current_timestamp()           .cast("string") .alias("_loaded_at"),
#     )
#     .dropDuplicates(
#         ["id"]
#     )
# )

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
        df_silver_contaazul_categories
        .select(
            F.col("id")             .cast("string").alias("categoria_principal_id"),
            F.col("categoria_pai")  .cast("string").alias("categoria_pai_id"),
        ),
        on="categoria_principal_id",
        how="left",
    )
    .join(
        # second join — resolve categoria_pai_nome from parent category
        df_parent_categories
        .select(
            F.col("id")             .cast("string").alias("categoria_pai_id"),
            F.col("nome")           .cast("string").alias("categoria_pai_nome"),
        ),
        on="categoria_pai_id",
        how="left",
    )
)

# conta azul - dre_items
df_silver_contaazul_dre_items = (
    df_bronze_contaazul_dre_categories
    .filter(
        F.col("id").isNotNull()
    )
    .select(                      
        F.col("id")                                                         .cast("string") .alias("id"),
        F.col("descricao")                                                  .cast("string") .alias("descricao"),
        F.coalesce(F.col("codigo"), F.lit("0"))                             .cast("string") .alias("codigo"),
        F.col("posicao")                                                    .cast("integer").alias("posicao"),
        F.col("indica_totalizador")                                         .cast("boolean").alias("indica_totalizador"),
        F.col("representa_soma_custo_medio")                                .cast("boolean").alias("representa_soma_custo_medio"), 
        F.when(F.size("subitens")               > 0, True).otherwise(False) .cast("boolean").alias("tem_subitens"),
        F.when(F.size("categorias_financeiras") > 0, True).otherwise(False) .cast("boolean").alias("tem_categorias_financeiras"),
        F.col("categorias_financeiras")                                                     .alias("categorias_financeiras"),
        F.lit("item")                                                       .cast("string") .alias("tipo"),
        F.current_timestamp()                                               .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - dre_subitems
df_silver_contaazul_dre_subitems = (
    df_bronze_contaazul_dre_categories
    .filter(
        F.col("subitens").isNotNull() &
        (F.size("subitens") > 0)
    )
    .select(
        F.col("id"),                                 
        F.explode("subitens").alias("subitem")
    )
    .select(                      
        F.col("subitem.id")                                                         .cast("string") .alias("id"),
        F.col("subitem.descricao")                                                  .cast("string") .alias("descricao"),
        F.col("subitem.codigo")                                                     .cast("string") .alias("codigo"),
        F.col("subitem.posicao")                                                    .cast("integer").alias("posicao"),
        F.col("subitem.indica_totalizador")                                         .cast("boolean").alias("indica_totalizador"),
        F.col("subitem.representa_soma_custo_medio")                                .cast("boolean").alias("representa_soma_custo_medio"),
        F.col("id")                                                                 .cast("string") .alias("parent_item_id"),   
        F.when(F.size("subitem.categorias_financeiras") > 0, True).otherwise(False) .cast("boolean").alias("tem_categorias_financeiras"),
        F.col("subitem.categorias_financeiras")                                                     .alias("categorias_financeiras"),
        F.lit("subitem")                                                            .cast("string") .alias("tipo"),
        F.current_timestamp()                                                       .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - combined dre items/subitems 
df_dre_combined_items = (
    df_silver_contaazul_dre_items
    .select(
        F.col("id"),
        F.col("id")                             .alias("parent_item_id"),
        F.col("descricao"),
        F.col("codigo"),
        F.col("posicao"),
        F.col("indica_totalizador"),
        F.col("representa_soma_custo_medio"),
        F.col("categorias_financeiras"),
        F.col("tipo" ),
    )
    .unionByName(
        df_silver_contaazul_dre_subitems
        .select(
            F.col("id"),
            F.col("parent_item_id"),
            F.col("descricao"),     
            F.col("codigo"),
            F.col("posicao"),
            F.col("indica_totalizador"),
            F.col("representa_soma_custo_medio"),
            F.col("categorias_financeiras"),
            F.col("tipo" ),
        ),
        allowMissingColumns=False
    )
)

# conta azul - dre_financial_categories - TO REVIEW
df_silver_contaazul_dre_financial_categories = (
    df_dre_combined_items
    .select(
        F.col("id"),   
        F.col("parent_item_id"),
        F.col("tipo"),                           
        F.explode("categorias_financeiras").alias("categoria_financeria")
    )
    .select(
        # record_id
        F.col("categoria_financeria.id")    .cast("string") .alias("categoria_id"),
        F.col("categoria_financeria.codigo").cast("string") .alias("codigo"),
        F.col("categoria_financeria.nome")  .cast("string") .alias("nome"),
        F.col("categoria_financeria.ativo") .cast("boolean").alias("ativo"),
        F.col("tipo")                       .cast("string") .alias("origem_tipo"),
        F.col("id")                         .cast("string") .alias("origem_id"), 
        F.col("parent_item_id")             .cast("string") .alias("origem_item_id"),
        F.current_timestamp()               .cast("string") .alias("_loaded_at"),
    )
)

# conta azul - financial_accounts - TO DO
# df_silver_contaazul_financial_accounts = (
#     df_bronze_contaazul_financial_accounts
#     .filter(
#         F.col("id").isNotNull()
#     )
#     .select(
#         F.col("id")                             .cast("string") .alias("id"),
#         F.col("banco")                          .cast("string") .alias("banco"),
#         F.col("codigo_banco")                   .cast("integer").alias("codigo_banco"),
#         F.col("nome")                           .cast("string") .alias("nome"),
#         F.col("ativo")                          .cast("boolean").alias("ativo"),
#         F.col("tipo")                           .cast("string") .alias("tipo"),
#         F.col("conta_padrao")                   .cast("boolean").alias("conta_padrao"),
#         F.col("possui_config_boleto_bancario")  .cast("boolean").alias("possui_config_boleto_bancario"),
#         F.col("agencia")                        .cast("string") .alias("agencia"),
#         F.col("numero")                         .cast("string") .alias("numero"),
#         # total_recebido
#         # total_a_receber
#         # total_pago
#         # total_a_pagar
#         # saldo_atual
#         F.current_timestamp()                                   .alias("_loaded_at"),
#     )
#     .dropDuplicates(
#         ["id"]
#     )
# )

# conta azul - installments
df_silver_contaazul_installments = (
    df_bronze_contaazul_installments
    .withColumn(
        "last_rateio",
        F.when(
            F.size(F.col("evento.rateio")) > 0,
            F.element_at(
                F.col("evento.rateio"), 
                F.size(F.col("evento.rateio"))
            )
        ).otherwise(F.lit(None))
    )
    .select(
        F.col("id")                                                         .cast("string") .alias("parcela_id"),
        F.col("status")                                                     .cast("string") .alias("parcela_status"),
        F.col("evento.condicao_pagamento")                                  .cast("string") .alias("condicao_pagamento"),
        F.col("referencia")                                                 .cast("string") .alias("referencia"),
        F.col("evento.agendado")                                            .cast("boolean").alias("agendado"),
        F.col("evento.tipo")                                                .cast("string") .alias("tipo_evento"),
        F.col("conciliado")                                                 .cast("boolean").alias("conciliado"),
        F.col("valor_pago")                                                 .cast("float")  .alias("valor_pago"),
        F.col("perda")                                                      .cast("string") .alias("perda"),
        F.col("nao_pago")                                                   .cast("float")  .alias("nao_pago"),
        F.col("data_vencimento")                                            .cast("string") .alias("data_vencimento"),
        F.col("data_pagamento_previsto")                                    .cast("string") .alias("data_pagamento_previsto"),
        F.col("descricao")                                                  .cast("string") .alias("descricao"),
        F.col("conta_financeira.id")                                        .cast("string") .alias("id_conta_financeira"),
        F.col("metodo_pagamento")                                           .cast("string") .alias("metodo_pagamento"),
        F.col("evento.id")                                                  .cast("string") .alias("parent_evento_id"),
        F.col("last_rateio.id_categoria")                                   .cast("string") .alias("rateio_id_categoria"),
        F.col("last_rateio.nome_categoria")                                 .cast("string") .alias("rateio_nome_categoria"),
        F.col("last_rateio.valor")                                          .cast("float")  .alias("rateio_valor"),
        last_element("last_rateio.rateio_centro_custo", "id_centro_custo")  .cast("string") .alias("rateio_centro_custo_id"),
        last_element("last_rateio.rateio_centro_custo", "nome_centro_custo").cast("string") .alias("rateio_centro_custo_nome"),
        last_element("last_rateio.rateio_centro_custo", "valor")            .cast("float")  .alias("rateio_centro_custo_valor"),
        F.current_timestamp()                                               .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(["parcela_id"])
)

# conta azul - installments write-off
df_silver_contaazul_installment_payments = (
    df_bronze_contaazul_installments
    .filter(
        F.col("baixas").isNotNull() &
        (F.size("baixas") > 0)
    )
    .select(
        F.col("id"),
        F.explode("baixas").alias("baixa"),
    )
    .select(
        F.col("id")                                     .cast("string") .alias("parcela_id"),
        F.col("baixa.id")                               .cast("string") .alias("baixa_id"),
        F.col("baixa.versao")                           .cast("integer").alias("baixa_versao"),
        F.col("baixa.data_pagamento")                   .cast("string") .alias("baixa_data_pagamento"),
        F.col("baixa.id_reconciliacao")                 .cast("string") .alias("baixa_id_reconciliacao"),
        F.col("baixa.id_parcela")                       .cast("string") .alias("baixa_id_parcela"),
        F.col("baixa.id_solicitacao_cobranca")          .cast("string") .alias("baixa_id_solicitacao_cobranca"),
        F.col("baixa.observacao")                       .cast("string") .alias("baixa_observacao"),
        F.col("baixa.metodo_pagamento")                 .cast("string") .alias("baixa_metodo_pagamento"),
        F.col("baixa.origem")                           .cast("string") .alias("baixa_origem"),
        F.col("baixa.id_recibo_digital")                .cast("string") .alias("baixa_id_recibo_digital"),
        F.col("baixa.tipo_evento_financeiro")           .cast("string") .alias("baixa_tipo_evento_financeiro"),
        F.col("baixa.nsu")                              .cast("string") .alias("baixa_nsu"),
        F.col("baixa.id_referencia")                    .cast("string") .alias("baixa_id_referencia"),
        F.col("baixa.atualizado_em")                    .cast("string") .alias("baixa_atualizado_em"),
        F.col("baixa.valor_composicao.desconto")        .cast("float")  .alias("baixa_desconto"),
        F.col("baixa.valor_composicao.juros")           .cast("float")  .alias("baixa_juros"),
        F.col("baixa.valor_composicao.multa")           .cast("float")  .alias("baixa_multa"),
        F.col("baixa.valor_composicao.taxa")            .cast("float")  .alias("baixa_taxa"),
        F.col("baixa.valor_composicao.valor_bruto")     .cast("float")  .alias("baixa_valor_bruto"),
        F.col("baixa.valor_composicao.valor_liquido")   .cast("float")  .alias("baixa_valor_liquido"),
        F.current_timestamp()                           .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["baixa_id"]
    )
)

## LOADING TABLES
# clickup - silver tables
save_nekt_table(df_silver_clickup_users,                        "Silver", "studio61_clickup_silver_users",                      "studio61_clickup_silver")
save_nekt_table(df_silver_clickup_spaces,                       "Silver", "studio61_clickup_silver_spaces",                     "studio61_clickup_silver")
save_nekt_table(df_silver_clickup_time_entries,                 "Silver", "studio61_clickup_silver_time_entries",               "studio61_clickup_silver")

# conta azul - silver tables
save_nekt_table(df_silver_contaazul_accounts_payable,           "Silver", "studio61_contaazul_silver_accounts_payable",         "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_accounts_receivable,        "Silver", "studio61_contaazul_silver_accounts_receivable",      "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_categories,                 "Silver", "studio61_contaazul_silver_categories",               "studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_customers,                  "Silver", "studio61_contaazul_silver_customers",                "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_combined_accounts,          "Silver", "studio61_contaazul_silver_combined_accounts",        "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_items,                  "Silver", "studio61_contaazul_silver_dre_items",                "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_subitems,               "Silver", "studio61_contaazul_silver_dre_subitems",             "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_financial_categories,   "Silver", "studio61_contaazul_silver_dre_financial_categories", "studio61_contaazul_silver")
# save_nekt_table(df_silver_contaazul_financial_accounts,         "Silver", "studio61_contaazul_silver_financial_accounts",       "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_installments,               "Silver", "studio61_contaazul_silver_installments",             "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_installment_payments,       "Silver", "studio61_contaazul_silver_installment_payments",     "studio61_contaazul_silver")