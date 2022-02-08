def get_deter_1(ano):
    import pandas as pd
    import numpy as np

    df = pd.read_csv(f'./intermunicipal_sc/raw/{ano}.csv', delimiter = ';', encoding = 'cp1252', header = 1)

    df.replace("  ", "", regex = True, inplace = True)

    df.rename(columns={
        "DESTINO" : "des_localidade_nome",
        "MUNICIPIO DESTINO" : "des_municipio_nome",
        "TRANSPORTADORA" : "empresa",
        "KM" : "km",
        "ORIGEM" : "ori_localidade_nome",
        "MUNICIPIO ORIGEM" : "ori_municipio_nome",
        "PASSAG.ESTUDANTE" : "pax_jovem_gratis",
        "PASSAG.COMUM" : "pax_pagantes",
        "PASSAG.TOTAL" : "pax_total",
        "LINHA/RAMAL" : "prefixo",
        "SECAO" : "sequencial",
        "OPERACAO" : "servico_ambito"}, inplace=True)

    df.drop(["ITINERARIO", "TEMPO"], axis = 1, inplace = True)

    # cria colunas para uso futuro
    df["ano"] = ano
    df["codigo"] = np.nan
    df["des_localidade_id"] = np.nan
    df["des_localidade_uf"] = np.nan
    df["des_municipio_id"] = np.nan
    df["empresa_cnpj"] = np.nan
    df["empresa_situacao"] = np.nan
    df["empresa_tipo"] = np.nan
    df["ida_idoso_desconto"] = np.nan
    df["ida_idoso_gratis"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["ida_pagantes"] = np.nan
    df["ida_passelivre"] = np.nan
    df["km_total"] = np.nan
    df["linha"] = np.nan
    df["linha_id"] = np.nan
    df["lugares_idas"] = np.nan
    df["lugares_voltas"] = np.nan
    df["mes"] = np.nan
    df["ori_localidade_id"] = np.nan
    df["ori_localidade_uf"] = np.nan
    df["ori_municipio_id"] = np.nan
    df["pax_gratis_descontos"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["secao_id"] = np.nan
    df["servico_tipo"] = np.nan
    df["sisdap_fim"] = np.nan
    df["sisdap_inicio"] = np.nan
    df["viagem_idas"] = np.nan
    df["viagem_voltas"] = np.nan
    df["volta_idoso_desconto"] = np.nan
    df["volta_idoso_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["volta_pagantes"] = np.nan
    df["volta_passelivre"] = np.nan

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["des_localidade_nome"])

    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis = 1)

    # É obrigatória a existência de um total de 53 colunas no dataset!!!

    df.to_csv(f'./intermunicipal_sc/ref/csv/{ano}.csv', sep = ',', encoding = 'utf-8', index = False)
    # printa msg dataset OK
    print(f"Gravação no formato CSV efetuada para o dataset de {ano}")

    df.to_parquet(f'./intermunicipal_sc/ref/parquet/{ano}.parquet', compression = 'snappy', index = False)
    # printa msg dataset OK
    print(f"Gravação no formato Parquet efetuada para o dataset de {ano}")
    return print(f"Sucesso para o dataset de {ano}\n")

def get_deter_2(ano):
    import pandas as pd
    import numpy as np

    df = pd.read_csv(f'./intermunicipal_sc/raw/{ano}.csv', delimiter = ';', encoding = 'cp1252', header = 1)

    df.replace("  ", "", regex = True, inplace = True)

    df.rename(columns={
        "Destino Seção" : "des_localidade_nome",
        "Municipio Destino" : "des_municipio_nome",
        "Transportadora" : "empresa",
        "Km" : "km",
        "Origem Seção" : "ori_localidade_nome",
        "Municipio Origem" : "ori_municipio_nome",
        "Passag.Estudante" : "pax_jovem_gratis",
        "Passag.Comum" : "pax_pagantes",
        "Passag.Total" : "pax_total",
        "Linha/Ramal" : "prefixo",
        "Seção" : "sequencial",
        "Operacao" : "servico_ambito"}, inplace=True)

    df.drop(["Itinerario", "Tempo", "Vl.Tarifa"], axis = 1, inplace = True)

    # cria colunas para uso futuro
    df["ano"] = ano
    df["codigo"] = np.nan
    df["des_localidade_id"] = np.nan
    df["des_localidade_uf"] = np.nan
    df["des_municipio_id"] = np.nan
    df["empresa_cnpj"] = np.nan
    df["empresa_situacao"] = np.nan
    df["empresa_tipo"] = np.nan
    df["ida_idoso_desconto"] = np.nan
    df["ida_idoso_gratis"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["ida_pagantes"] = np.nan
    df["ida_passelivre"] = np.nan
    df["km_total"] = np.nan
    df["linha"] = np.nan
    df["linha_id"] = np.nan
    df["lugares_idas"] = np.nan
    df["lugares_voltas"] = np.nan
    df["mes"] = np.nan
    df["ori_localidade_id"] = np.nan
    df["ori_localidade_uf"] = np.nan
    df["ori_municipio_id"] = np.nan
    df["pax_gratis_descontos"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["secao_id"] = np.nan
    df["servico_tipo"] = np.nan
    df["sisdap_fim"] = np.nan
    df["sisdap_inicio"] = np.nan
    df["viagem_idas"] = np.nan
    df["viagem_voltas"] = np.nan
    df["volta_idoso_desconto"] = np.nan
    df["volta_idoso_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["volta_pagantes"] = np.nan
    df["volta_passelivre"] = np.nan

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["des_localidade_nome"])

    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis = 1)

    # É obrigatória a existência de um total de 53 colunas no dataset!!!

    df.to_csv(f'./intermunicipal_sc/ref/csv/{ano}.csv', sep = ',', encoding = 'utf-8', index = False)
    # printa msg dataset OK
    print(f"Gravação no formato CSV efetuada para o dataset de {ano}")

    df.to_parquet(f'./intermunicipal_sc/ref/parquet/{ano}.parquet', compression = 'snappy', index = False)
    # printa msg dataset OK
    print(f"Gravação no formato Parquet efetuada para o dataset de {ano}")
    return print(f"Sucesso para o dataset de {ano}\n")

def ler_sc():
    import pandas as pd
    ano_input = input("Digite o ano desejado")
    ano = int(ano_input)
    print(f"Você escolheu o ano: {ano}")
    print("Agora, escolha o formato para leitura.")
    print("Formatos disponíveis:\n1 - CSV | 2 - Parquet")
    formato_input = input("Digite o formato desejado")
    formato = int(formato_input)
    print(f"Você escolheu o formato: {formato}")
    if formato == 1:
        df = pd.read_csv(f'./intermunicipal_sc/ref/csv/{ano}.csv', sep = ',', encoding = 'utf-8')
    if formato == 2:
        df = pd.read_parquet(f'./intermunicipal_sc/ref/parquet/{ano}.parquet')
    return df