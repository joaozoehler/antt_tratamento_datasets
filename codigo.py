def get_antt_padrao_2005(ano):
    # importa libs
    import pandas as pd
    import numpy as np

    if ano == 2014:
        df = pd.read_csv(f'raw/{ano}.csv', sep = ';', encoding = 'cp1252')
    else:
        df = pd.read_csv(f'raw/{ano}.csv', sep = ';', encoding = 'utf-8')

    # renomeia colunas ARQUIVOS ANTT 2005 ATÉ 2014
    df.rename(columns={"CODIGO" : "codigo",
                    "EMPRESA" : "empresa",
                    "ANO" : "ano",
                    "MES" : "mes",
                    "PREFIXO" : "prefixo",
                    "LINHA" : "linha",
                    "NUMEROVIAGEMIDA" : "viagem_idas",
                    "NUMEROVIAGEMVOLTA" : "viagem_voltas",
                    "NUMEROLUGAROFERTADOIDA" : "lugares_idas",
                    "NUMEROLUGAROFERTADOVOLTA" : "lugares_voltas",
                    "SEQUENCIALORDENACAO" : "sequencial",
                    "IDLOCALIDADE" : "ori_localidade_id",
                    "ORIGEM" : "ori_localidade_nome",
                    "UF" : "ori_localidade_uf",
                    "IDLOCALIDADE.1" : "des_localidade_id",
                    "DESTINO" : "des_localidade_nome",
                    "UF.1" : "des_localidade_uf",
                    "NUMEROPAGANTESIDA" : "ida_pagantes",
                    "NUMEROPAGANTESVOLTA" : "volta_pagantes",
                    "NUMEROGRATUIDADEPASSELIVREIDA" : "ida_passelivre",
                    "NUMEROGRATUIDADEPASSELIVREVOLTA" : "volta_passelivre",
                    "NUMEROGRATUIDADEIDOSOIDA" : "ida_idoso_gratis",
                    "NUMEROGRATUIDADEIDOSOVOLTA" : "volta_idoso_gratis",
                    "NUMERODESCONTOIDOSOIDA" : "ida_idoso_desconto",
                    "NUMERODESCONTOIDOSOVOLTA" : "volta_idoso_desconto"}, inplace=True)

    # substitui os valores NULL
    df['ida_pagantes'] = df['ida_pagantes'].fillna(0)
    df['volta_pagantes'] = df['volta_pagantes'].fillna(0)
    df['ida_passelivre'] = df['ida_passelivre'].fillna(0)
    df['volta_passelivre'] = df['volta_passelivre'].fillna(0)
    df['ida_idoso_gratis'] = df['ida_idoso_gratis'].fillna(0)
    df['volta_idoso_gratis'] = df['volta_idoso_gratis'].fillna(0)
    df['ida_idoso_desconto'] = df['ida_idoso_desconto'].fillna(0)
    df['volta_idoso_desconto'] = df['volta_idoso_desconto'].fillna(0)

    # cria coluna pax_total com soma
    df["pax_total"] = (df["ida_pagantes"] +
                    df["volta_pagantes"] +
                    df["ida_passelivre"] +
                    df["volta_passelivre"] +
                    df["ida_idoso_gratis"] +
                    df["volta_idoso_gratis"] +
                    df["ida_idoso_desconto"] +
                    df["volta_idoso_desconto"])

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["ori_municipio_nome"] = np.nan
    df["des_municipio_nome"] = np.nan
    df["empresa_situacao"] = np.nan
    df["empresa_tipo"] = np.nan
    df["empresa_cnpj"] = np.nan
    df["linha_id"] = np.nan
    df["linha_id"] = np.nan
    df["secao_id"] = np.nan
    df["servico_tipo"] = np.nan
    df["servico_ambito"] = np.nan
    df["ori_municipio_id"] = np.nan
    df["des_municipio_id"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["sisdap_inicio"] = np.nan
    df["sisdap_fim"] = np.nan
    df["km"] = np.nan
    df["km_total"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_jovem_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["pax_pagantes"] = np.nan
    df["pax_gratis_descontos"] = np.nan

    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int}) 

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)

def get_antt_padrao_2015(ano):
    # importa lib
    import pandas as pd
    import numpy as np

    # lê arquivo origem
    df = pd.read_excel(f'raw/{ano}.xlsx')

    # renomeia colunas ARQUIVOS ANTT 2015
    df.rename(columns={ 'ano' : 'ano',
                        'mes' : 'mes',
                        'situacaoEmpresa' : 'empresa_situacao',
                        'tipoEmpresa' : 'empresa_tipo',
                        'cnpjFonteInformacao' : 'empresa_cnpj',
                        'razaoSocialFonteInformacao' : 'empresa',
                        'idLinha' : 'linha_id',
                        'prefixo' : 'prefixo',
                        'servico' : 'servico_tipo',
                        'ambitoSecao' : 'servico_ambito',
                        'municipioOrigemId' : 'ori_municipio_id',
                        'municipioOrigemNome' : 'ori_municipio_nome',
                        'municipioOrigemUF' : 'ori_localidade_uf',
                        'localidadeOrigemId' : 'ori_localidade_id',
                        'localidadeOrigemNome' : 'ori_localidade_nome',
                        'municipioDestinoId' : 'des_municipio_id',
                        'municipioDestinoNome' : 'des_municipio_nome',
                        'municipioDestinoUF' : 'des_localidade_uf',
                        'localidadeDestinoId' : 'des_localidade_id',
                        'localidadeDestinoNome' : 'des_localidade_nome',
                        'NumeroLugarOfertadoIda' : 'lugares_idas',
                        'NumeroLugarOfertadoVolta' : 'lugares_voltas',
                        'NumeroViagemIda' : 'viagem_idas',
                        'NumeroViagemVolta' : 'viagem_voltas',
                        'NumeroGratuidadeIdosoIda' : 'ida_idoso_gratis',
                        'NumeroGratuidadePasseLivreIda' : 'ida_passelivre',
                        'NumeroIdosoDescontoIda' : 'ida_idoso_desconto',
                        'NumeroPagantesIda' : 'ida_pagantes',
                        'NumeroGratuidadeIdosoVolta' : 'volta_idoso_gratis',
                        'NumeroGratuidadePasseLivreVolta' : 'volta_passelivre',
                        'NumeroIdosoDescontoVolta' : 'volta_idoso_desconto',
                        'NumeroPagantesVolta' : 'volta_pagantes',
                        'dataInicioReferencia' : 'sisdap_inicio',
                        'dataFimReferencia' : 'sisdap_fim'}, inplace=True)
    
    # cria coluna pax_total com soma
    df["pax_total"] = (df["ida_pagantes"] +
                    df["volta_pagantes"] +
                    df["ida_passelivre"] +
                    df["volta_passelivre"] +
                    df["ida_idoso_gratis"] +
                    df["volta_idoso_gratis"] +
                    df["ida_idoso_desconto"]+
                    df["volta_idoso_desconto"])

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["secao_id"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["km"] = np.nan
    df["km_total"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_jovem_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["pax_pagantes"] = np.nan
    df["pax_gratis_descontos"] = np.nan
    df["codigo"] = np.nan
    df["sequencial"] = np.nan
    df["linha"] = np.nan
    
    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int}) 

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)

def get_antt_padrao_2016(ano):
    # importa lib
    import pandas as pd
    import numpy as np

    # lê arquivo origem
    df = pd.read_excel(f'raw/{ano}.xlsx')

    # renomeia colunas ARQUIVOS ANTT 2016
    df.rename(columns={ 'ano' : 'ano',
                        'mes' : 'mes',
                        'situacaoEmpresa' : 'empresa_situacao',
                        'tipoEmpresa' : 'empresa_tipo',
                        'cnpjFonteInformacao' : 'empresa_cnpj',
                        'razaoSocialFonteInformacao' : 'empresa',
                        'idLinha' : 'linha_id',
                        'prefixo' : 'prefixo',
                        'servico' : 'servico_tipo',
                        'ambitoSecao' : 'servico_ambito',
                        'municipioOrigemId' : 'ori_municipio_id',
                        'municipioOrigemNome' : 'ori_municipio_nome',
                        'municipioOrigemUF' : 'ori_localidade_uf',
                        'localidadeOrigemId' : 'ori_localidade_id',
                        'localidadeOrigemNome' : 'ori_localidade_nome',
                        'municipioDestinoId' : 'des_municipio_id',
                        'municipioDestinoNome' : 'des_municipio_nome',
                        'municipioDestinoUF' : 'des_localidade_uf',
                        'localidadeDestinoId' : 'des_localidade_id',
                        'localidadeDestinoNome' : 'des_localidade_nome',
                        'NumeroLugarOfertadoIda' : 'lugares_idas',
                        'NumeroLugarOfertadoVolta' : 'lugares_voltas',
                        'NumeroViagemIda' : 'viagem_idas',
                        'NumeroViagemVolta' : 'viagem_voltas',
                        'NumeroGratuidadeIdosoIda' : 'ida_idoso_gratis',
                        'NumeroGratuidadePasseLivreIda' : 'ida_passelivre',
                        'NumeroIdosoDescontoIda' : 'ida_idoso_desconto',
                        'NumeroPagantesIda' : 'ida_pagantes',
                        'NumeroGratuidadeIdosoVolta' : 'volta_idoso_gratis',
                        'NumeroGratuidadePasseLivreVolta' : 'volta_passelivre',
                        'NumeroIdosoDescontoVolta' : 'volta_idoso_desconto',
                        'NumeroPagantesVolta' : 'volta_pagantes',
                        'Total Passageiros' : 'pax_total'}, inplace=True)

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["secao_id"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["sisdap_inicio"] = np.nan
    df["sisdap_fim"] = np.nan
    df["km"] = np.nan
    df["km_total"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_jovem_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["pax_pagantes"] = np.nan
    df["pax_gratis_descontos"] = np.nan
    df["codigo"] = np.nan
    df["sequencial"] = np.nan
    df["linha"] = np.nan
    
    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int}) 

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)

def get_antt_padrao_2017(ano):
    # importa lib
    import pandas as pd
    import numpy as np

    # lê arquivo origem
    df = pd.read_excel(f'raw/{ano}.xlsx')

    # renomeia colunas ARQUIVOS ANTT 2017
    df.rename(columns={'ano' : 'ano',
                    'mes' : 'mes',
                    'situacaoEmpresa' : 'empresa_situacao',
                    'tipoEmpresa' : 'empresa_tipo',
                    'cnpj' : 'empresa_cnpj',
                    'razaoSocialFonteInformacao' : 'empresa',
                    'idLinha' : 'linha_id',
                    'prefixo' : 'prefixo',
                    'tipoServico' : 'servico_tipo',
                    'ambitoServico' : 'servico_ambito',
                    'municipioOrigemId' : 'ori_municipio_id',
                    'municipioOrigemNome' : 'ori_municipio_nome',
                    'municipioOrigemUF' : 'ori_localidade_uf',
                    'localidadeOrigemId' : 'ori_localidade_id',
                    'localidadeOrigemNome' : 'ori_localidade_nome',
                    'municipioDestinoId' : 'des_municipio_id',
                    'municipioDestinoNome' : 'des_municipio_nome',
                    'municipioDestinoUF' : 'des_localidade_uf',
                    'localidadeDestinoId' : 'des_localidade_id',
                    'localidadeDestinoNome' : 'des_localidade_nome',
                    'numeroLugarOfertadoIda' : 'lugares_idas',
                    'numeroLugarOfertadoVolta' : 'lugares_voltas',
                    'numeroViagemIda' : 'viagem_idas',
                    'numeroViagemVolta' : 'viagem_voltas',
                    'numeroGratuidadeJovemDescontoIda' : 'ida_jovem_desconto',
                    'numeroGratuidadeJovemIda' : 'ida_jovem_gratis',
                    'numeroGratuidadeIdosoIda' : 'ida_idoso_gratis',
                    'numeroGratuidadePasseLivreIda' : 'ida_passelivre',
                    'numeroIdosoDescontoIda' : 'ida_idoso_desconto',
                    'numeroPagantesIda' : 'ida_pagantes',
                    'numeroGratuidadeJovemDescontoVolta' : 'volta_jovem_desconto',
                    'numeroGratuidadeJovemVolta' : 'volta_jovem_gratis',
                    'numeroGratuidadeIdosoVolta' : 'volta_idoso_gratis',
                    'numeroGratuidadePasseLivreVolta' : 'volta_passelivre',
                    'numeroIdosoDescontoVolta' : 'volta_idoso_desconto',
                    'numeroPagantesVolta' : 'volta_pagantes',
                    'dataInicioSISDAP' : 'sisdap_inicio',
                    'dataFimSISDAP' : 'sisdap_fim',
                    'km' : 'km',
                    'Gratuidades' : 'pax_gratis_descontos',
                    'Passageiros' : 'pax_total'}, inplace=True)

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["secao_id"] = np.nan
    df["km_total"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_jovem_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["pax_pagantes"] = np.nan
    df["codigo"] = np.nan
    df["sequencial"] = np.nan
    df["linha"] = np.nan
    
    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int}) 

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)

def get_antt_padrao_2018(ano):
    # importa lib
    import pandas as pd
    import numpy as np

    # lê CSV origem
    df = pd.read_excel(f'raw/{ano}.xlsx')

    # renomeia colunas ARQUIVOS ANTT 2018
    df.rename(columns={'ano' : 'ano',
                    'mês' : 'mes',
                    'situacaoEmpresa' : 'empresa_situacao',
                    'tipoEmpresa' : 'empresa_tipo',
                    'cnpj' : 'empresa_cnpj',
                    'razaoSocialFonteInformacao' : 'empresa',
                    'idLinha' : 'linha_id',
                    'idSecao' : 'secao_id',
                    'prefixo' : 'prefixo',
                    'tipoServico' : 'servico_tipo',
                    'ambitoServico' : 'servico_ambito',
                    'municipioOrigemId' : 'ori_municipio_id',
                    'municipioOrigemNome' : 'ori_municipio_nome',
                    'municipioOrigemUF' : 'ori_localidade_uf',
                    'localidadeOrigemId' : 'ori_localidade_id',
                    'localidadeOrigemNome' : 'ori_localidade_nome',
                    'municipioDestinoId' : 'des_municipio_id',
                    'municipioDestinoNome' : 'des_municipio_nome',
                    'municipioDestinoUF' : 'des_localidade_uf',
                    'localidadeDestinoId' : 'des_localidade_id',
                    'localidadeDestinoNome' : 'des_localidade_nome',
                    'numeroLugarOfertadoIda' : 'lugares_idas',
                    'numeroLugarOfertadoVolta' : 'lugares_voltas',
                    'numeroViagemIda' : 'viagem_idas',
                    'numeroViagemVolta' : 'viagem_voltas',
                    'numeroGratuidadeJovemDescontoIda' : 'ida_jovem_desconto',
                    'numeroGratuidadeJovemIda' : 'ida_jovem_gratis',
                    'numeroGratuidadeIdosoIda' : 'ida_idoso_gratis',
                    'numeroGratuidadePasseLivreIda' : 'ida_passelivre',
                    'numeroIdosoDescontoIda' : 'ida_idoso_desconto',
                    'numeroPagantesIda' : 'ida_pagantes',
                    'numeroGratuidadeJovemDescontoVolta' : 'volta_jovem_desconto',
                    'numeroGratuidadeJovemVolta' : 'volta_jovem_gratis',
                    'numeroGratuidadeIdosoVolta' : 'volta_idoso_gratis',
                    'numeroGratuidadePasseLivreVolta' : 'volta_passelivre',
                    'numeroIdosoDescontoVolta' : 'volta_idoso_desconto',
                    'numeroPagantesVolta' : 'volta_pagantes',
                    'dataInicioSISDAP' : 'sisdap_inicio',
                    'dataFimSISDAP' : 'sisdap_fim',
                    'km' : 'km',
                    'Gratuidade Idoso' : 'pax_idoso_gratis',
                    'Desconto Idoso' : 'pax_idoso_desconto',
                    'Gratuidade Jovem' : 'pax_jovem_gratis',
                    'Desconto Jovem' : 'pax_jovem_desconto',
                    'Passe Livre' : 'pax_passelivre',
                    'Pagantes' : 'pax_pagantes',
                    'Gratuidades e Descontos Legais' : 'pax_gratis_descontos',
                    'Total de Passageiros' : 'pax_total'}, inplace=True)

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["codigo"] = np.nan
    df["sequencial"] = np.nan
    df["linha"] = np.nan
    df["km_total"] = np.nan

    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int}) 

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)        

def get_antt_padrao_2019(ano):
    # importa lib
    import pandas as pd
    import numpy as np

    # lê CSV origem
    df = pd.read_excel(f'raw/{ano}.xlsx')

    # renomeia colunas ARQUIVOS ANTT 2019 EM DIANTE
    df.rename(columns={'ano' : 'ano',
                    'mês' : 'mes',
                    'situacaoEmpresa' : 'empresa_situacao',
                    'tipoEmpresa' : 'empresa_tipo',
                    'cnpj' : 'empresa_cnpj',
                    'razaoSocialFonteInformacao' : 'empresa',
                    'idLinha' : 'linha_id',
                    'idSecao' : 'secao_id',
                    'prefixo' : 'prefixo',
                    'tipoServico' : 'servico_tipo',
                    'ambitoServico' : 'servico_ambito',
                    'municipioOrigemId' : 'ori_municipio_id',
                    'municipioOrigemNome' : 'ori_municipio_nome',
                    'municipioOrigemUF' : 'ori_localidade_uf',
                    'localidadeOrigemId' : 'ori_localidade_id',
                    'localidadeOrigemNome' : 'ori_localidade_nome',
                    'municipioDestinoId' : 'des_municipio_id',
                    'municipioDestinoNome' : 'des_municipio_nome',
                    'municipioDestinoUF' : 'des_localidade_uf',
                    'localidadeDestinoId' : 'des_localidade_id',
                    'localidadeDestinoNome' : 'des_localidade_nome',
                    'numeroLugarOfertadoIda' : 'lugares_idas',
                    'numeroLugarOfertadoVolta' : 'lugares_voltas',
                    'numeroViagemIda' : 'viagem_idas',
                    'numeroViagemVolta' : 'viagem_voltas',
                    'numeroGratuidadeJovemDescontoIda' : 'ida_jovem_desconto',
                    'numeroGratuidadeJovemIda' : 'ida_jovem_gratis',
                    'numeroGratuidadeIdosoIda' : 'ida_idoso_gratis',
                    'numeroGratuidadePasseLivreIda' : 'ida_passelivre',
                    'numeroIdosoDescontoIda' : 'ida_idoso_desconto',
                    'numeroPagantesIda' : 'ida_pagantes',
                    'numeroGratuidadeJovemDescontoVolta' : 'volta_jovem_desconto',
                    'numeroGratuidadeJovemVolta' : 'volta_jovem_gratis',
                    'numeroGratuidadeIdosoVolta' : 'volta_idoso_gratis',
                    'numeroGratuidadePasseLivreVolta' : 'volta_passelivre',
                    'numeroIdosoDescontoVolta' : 'volta_idoso_desconto',
                    'numeroPagantesVolta' : 'volta_pagantes',
                    'dataInicioSISDAP' : 'sisdap_inicio',
                    'dataFimSISDAP' : 'sisdap_fim',
                    'km' : 'km',
                    'kmTotal' : 'km_total',
                    'Gratuidade Idoso' : 'pax_idoso_gratis',
                    'Desconto Idoso' : 'pax_idoso_desconto',
                    'Gratuidade Jovem' : 'pax_jovem_gratis',
                    'Desconto Jovem' : 'pax_jovem_desconto',
                    'Passe Livre' : 'pax_passelivre',
                    'Pagantes' : 'pax_pagantes',
                    'Gratuidades e Descontos Legais' : 'pax_gratis_descontos',
                    'Total de Passageiros' : 'pax_total'}, inplace=True)

    # retira excesso de espaços
    df.replace("  ", "", regex = True, inplace = True)

    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["ori_localidade_uf"] + " - " + df["des_localidade_nome"]) + "-" + df["des_localidade_uf"]

    # cria colunas para uso futuro
    df["codigo"] = np.nan
    df["sequencial"] = np.nan
    df["linha"] = np.nan

    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis=1)

    # garante que a coluna PAX_TOTAL seja no formato correto
    df = df.astype({"pax_total": int})

    # printa msg dataset OK
    print(f"Sucesso para o dataset de {ano}")

    return df.to_csv(f'ref/{ano}.csv', sep = ';', quotechar = "'", encoding = 'utf-8', index = False)

def get_antt(ano):
    # importa lib
    import pandas as pd
    import numpy as np
    import os

    padrao_2005 = (2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014)
    padrao_2015 = (2015)
    padrao_2016 = (2016)
    padrao_2017 = (2017)
    padrao_2018 = (2018)
    padrao_2019 = (2019, 2020, 2021)
    
    os.system('mkdir ref')

    if ano in padrao_2005:
        get_antt_padrao_2005(ano)
    
    elif ano in padrao_2015:
        get_antt_padrao_2015(ano)

    elif ano in padrao_2016:
        get_antt_padrao_2016(ano)
    
    elif ano in padrao_2017:
        get_antt_padrao_2017(ano)

    elif ano in padrao_2018:
        get_antt_padrao_2018(ano)

    elif ano in padrao_2019:
        get_antt_padrao_2019(ano)
    