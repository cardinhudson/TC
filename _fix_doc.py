"""Fix apresentacao visual roteiro section."""
with open(r'pages/6 - Documentacao.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_line = None
end_line = None
for i, line in enumerate(lines):
    if 'Roteiro sugerido (objetivo' in line:
        start_line = i
    if start_line and 'denominador (volume)' in line and i > start_line:
        end_line = i + 2  # include the closing paren line
        break

print(f'Start: {start_line}, End: {end_line}')

if start_line and end_line:
    new_block = [
        '        st.subheader("\U0001f3a4 Roteiro sugerido (objetivo: clareza em 5 minutos)")\n',
        '        st.markdown(\n',
        '            """\n',
        '            **0:00\u20130:30 \u2014 Contexto**\n',
        '            - O que \u00e9 o Portal TC e seu objetivo: decis\u00e3o r\u00e1pida com dados de custo/volume.\n',
        '            - Dois m\u00f3dulos: **TC Estendido** (agregado) e **TC Ve\u00edculos** (rateado por modelo).\n',
        '\n',
        '            **0:30\u20131:15 \u2014 TC Ext (Home)**\n',
        '            - Mostrar filtros (Ano/Per\u00edodo/Oficina/Ve\u00edculo) e altern\u00e2ncia **Custo Total \u2194 CPU**.\n',
        '            - Refor\u00e7ar a regra: em CPU, o total \u00e9 **ponderado por volume** (`sum(Total)/sum(Volume)`).\n',
        '\n',
        '            **1:15\u20132:00 \u2014 TC Ve\u00edculos (Home)**\n',
        '            - Cadeia: Despesa Prim\u00e1ria \u2192 FA \u2192 FP \u2192 D&A \u2192 FP sem Dedicada.\n',
        '            - 6 tabs: TC Ve\u00edculos, An\u00e1lise Flex, Volume, Custos por Oficina, Tempo Produ\u00e7\u00e3o, Dados Detalhados.\n',
        '            - Sele\u00e7\u00e3o de ve\u00edculo espec\u00edfico aciona rateio por tempo de produ\u00e7\u00e3o.\n',
        '\n',
        '            **2:00\u20132:45 \u2014 Waterfall**\n',
        '            - Explicar "o que mudou" entre dois per\u00edodos e como o Flex Bud separa efeito volume/custo.\n',
        '            - Dispon\u00edvel nos dois m\u00f3dulos (TC Ext e TC Ve\u00edculos).\n',
        '\n',
        '            **2:45\u20134:00 \u2014 Best Estimate**\n',
        '            - **Simulador**: define premissas (sensibilidade/infla\u00e7\u00e3o/volume) e gera `Forecast/`.\n',
        '            - **An\u00e1lise BE**: layout da Home com Forecast. Cores: roxo escuro = Hist\u00f3rico, roxo claro = BE.\n',
        '            - Dispon\u00edvel para TC Ext e TC Ve\u00edculos.\n',
        '\n',
        '            **4:00\u20135:00 \u2014 Encerramento**\n',
        '            - Exporta\u00e7\u00e3o Excel com formata\u00e7\u00e3o profissional.\n',
        '            - Multi-moeda (BRL/USD/EUR) e fator de escala (K/M).\n',
        '            - Equipe: Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus.\n',
        '            """\n',
        '        )\n',
        '        st.info(\n',
        '            "Dica: quando algu\u00e9m questionar varia\u00e7\u00f5es de TOTAL em CPU por m\u00eas, abra o expander "\n',
        "            \"\u2018Volume por per\u00edodo\u2019 para mostrar que a diferen\u00e7a vem do denominador (volume).\"\n",
        '        )\n',
    ]
    lines[start_line:end_line] = new_block
    with open(r'pages/6 - Documentacao.py', 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print('Roteiro updated successfully')

