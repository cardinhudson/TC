"""Chart utility functions for home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tc_principal.shared import (
    ordenar_por_mes, calcular_cpu,
)

def create_periodo_chart(df_periodo, df_flex, tipo, label_valor,
                         simbolo, sufixo, ordem_per, tem_ano=False,
                         col_tipo=None, modo_be=False,
                         hover_payloads_bar=None,
                         hover_payloads_budget=None,
                         hover_payloads_delta=None):
    """
    Cria gráfico de Custo FP por Período usando Plotly (resolução definitiva
    do bug de renderização do Altair).

    Modo Real  → barras com degradê roxo contínuo, Flex Bud laranja pontilhada.
    Modo BE    → barras Histórico roxo escuro + BE roxo claro, Flex Bud laranja.
    Delta      → mini-barras verde (negativo = bom) / vermelho (positivo = ruim).
    """
    try:
        # Garantir ordem_per único (remover duplicados mantendo ordem)
        ordem_per = list(dict.fromkeys(ordem_per)) if ordem_per else []
        
        coluna = 'Custo FP'
        titulo_y = f'{label_valor} ({simbolo}{sufixo})'

        # ── Preparar dados ──
        df_p = df_periodo.copy()
        df_p[coluna] = pd.to_numeric(df_p[coluna], errors='coerce').fillna(0)
        df_p = df_p.replace([np.inf, -np.inf], 0)

        # Coluna do período para eixo X
        if tem_ano and 'Ano' in df_p.columns:
            df_p['_x_label'] = df_p['Período'].astype(str) + ' ' + df_p['Ano'].astype(str)
        else:
            df_p['_x_label'] = df_p['Período'].astype(str)

        x_col = '_x_label'
        _usar_tipo = bool(col_tipo and col_tipo in df_p.columns)

        # ── Decidir número de subplots ──
        tem_flex = False
        df_flex_p = _preparar_flex(df_flex, tem_ano, tipo, ordem_per)
        if df_flex_p is not None and len(df_flex_p) > 0:
            tem_flex = True

        n_rows = 2 if tem_flex else 1
        row_heights = [0.162, 0.838] if tem_flex else [1.0]

        fig = make_subplots(
            rows=n_rows, cols=1, shared_xaxes=True,
            vertical_spacing=0.17,
            row_heights=row_heights,
        )

        # ════════════════════════════════════════
        # BARRAS PRINCIPAIS (último subplot = embaixo)
        # ════════════════════════════════════════
        bar_row = n_rows  # última linha

        if _usar_tipo and modo_be:
            # ── Modo BE: empilhar Histórico (escuro) + BE (claro) por período ──
            _cores_tipo = {'Histórico': '#4C1D95', 'BE': '#C4B5FD'}
            for tipo_val in ['Histórico', 'BE']:
                mask = df_p[col_tipo] == tipo_val
                sub = df_p[mask].copy()
                if sub.empty:
                    continue
                # Agregar por x_label (um valor por período por tipo)
                sub_agg = sub.groupby(x_col, as_index=False, observed=False)[coluna].sum()
                # Ordenar
                sub_agg[x_col] = pd.Categorical(sub_agg[x_col], categories=ordem_per, ordered=True)
                sub_agg = sub_agg.sort_values(x_col)

                # ── Tooltip rico (se disponível) ──
                _be_hover = None
                if hover_payloads_bar:
                    _be_hover = [hover_payloads_bar.get(str(p).strip(), '') for p in sub_agg[x_col].astype(str)]
                    if not any(_be_hover):
                        _be_hover = None

                _bar_kw = (
                    dict(hovertext=_be_hover, hovertemplate='%{hovertext}<extra></extra>')
                    if _be_hover
                    else dict(hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>' + tipo_val + '</extra>')
                )
                fig.add_trace(go.Bar(
                    x=sub_agg[x_col].astype(str),
                    y=sub_agg[coluna],
                    name=tipo_val,
                    marker_color=_cores_tipo.get(tipo_val, '#C4B5FD'),
                    textposition='none',
                    **_bar_kw,
                ), row=bar_row, col=1)
                # Rótulos na base interna com caixa cinza
                for _i, _row_be in sub_agg.iterrows():
                    _val = _row_be[coluna]
                    if pd.notna(_val) and _val != 0:
                        fig.add_annotation(
                            x=str(_row_be[x_col]),
                            y=_val * 0.05,
                            text=f'{_val:,.2f}',
                            showarrow=False,
                            font=dict(size=10, color='#333333'),
                            bgcolor='rgba(220,220,220,0.75)',
                            borderpad=2,
                            xref=f'x{bar_row}' if bar_row > 1 else 'x',
                            yref=f'y{bar_row}' if bar_row > 1 else 'y',
                            yanchor='bottom',
                        )
        else:
            # ── Modo Real: degradê roxo contínuo ──
            # Agregar por período (sem Tipo)
            df_agg = df_p.groupby(x_col, as_index=False, observed=False)[coluna].sum()
            df_agg[x_col] = pd.Categorical(df_agg[x_col], categories=ordem_per, ordered=True)
            df_agg = df_agg.sort_values(x_col)

            vals = df_agg[coluna].values
            v_min = vals.min() if len(vals) > 0 else 0
            v_max = vals.max() if len(vals) > 0 else 1
            if v_max == v_min:
                v_max = v_min + 1

            # Gerar cores roxo degradê (mais claro → mais escuro proporcional ao valor)
            bar_colors = []
            for v in vals:
                t = (v - v_min) / (v_max - v_min) if v_max > v_min else 0.5
                # Interpolar de roxo claro (#D8B4FE) a roxo escuro (#4C1D95)
                r = int(216 + t * (76 - 216))
                g = int(180 + t * (29 - 180))
                b = int(254 + t * (149 - 254))
                bar_colors.append(f'rgb({r},{g},{b})')

            # ── Tooltip rico (se disponível) ──
            _real_hover = None
            if hover_payloads_bar:
                _real_hover = [hover_payloads_bar.get(str(p).strip(), '') for p in df_agg[x_col].astype(str)]
                if not any(_real_hover):
                    _real_hover = None

            _bar_kw_real = (
                dict(hovertext=_real_hover, hovertemplate='%{hovertext}<extra></extra>')
                if _real_hover
                else dict(hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>Real</extra>')
            )
            fig.add_trace(go.Bar(
                x=df_agg[x_col].astype(str),
                y=df_agg[coluna],
                name='Real',
                marker_color=bar_colors,
                textposition='none',
                showlegend=False,
                **_bar_kw_real,
            ), row=bar_row, col=1)
            # Rótulos na base interna com caixa cinza
            for _i, _row_r in df_agg.iterrows():
                _val = _row_r[coluna]
                if pd.notna(_val) and _val != 0:
                    fig.add_annotation(
                        x=str(_row_r[x_col]),
                        y=_val * 0.05,
                        text=f'{_val:,.2f}',
                        showarrow=False,
                        font=dict(size=10, color='#333333'),
                        bgcolor='rgba(220,220,220,0.75)',
                        borderpad=2,
                        xref=f'x{bar_row}' if bar_row > 1 else 'x',
                        yref=f'y{bar_row}' if bar_row > 1 else 'y',
                        yanchor='bottom',
                    )

        # ════════════════════════════════════════
        # LINHA FLEX BUD (laranja pontilhada)
        # ════════════════════════════════════════
        if tem_flex and df_flex_p is not None:
            # Garantir ordem
            if x_col == '_x_label':
                if tem_ano and 'Ano' in df_flex_p.columns:
                    df_flex_p['_x_label'] = df_flex_p['Período'].astype(str) + ' ' + df_flex_p['Ano'].astype(str)
                else:
                    df_flex_p['_x_label'] = df_flex_p['Período'].astype(str)
            df_flex_p[x_col] = pd.Categorical(df_flex_p[x_col], categories=ordem_per, ordered=True)
            df_flex_p = df_flex_p.sort_values(x_col)

            # ── Tooltip rico para Flex Bud (se disponível) ──
            _flex_hover = None
            if hover_payloads_budget:
                _flex_hover = [hover_payloads_budget.get(str(p).strip(), '') for p in df_flex_p[x_col].astype(str)]
                if not any(_flex_hover):
                    _flex_hover = None

            _flex_kw = (
                dict(hovertext=_flex_hover, hovertemplate='%{hovertext}<extra></extra>')
                if _flex_hover
                else dict(hovertemplate='%{x}<br>Flex Bud: %{y:,.2f}<extra>Flex Bud</extra>')
            )
            fig.add_trace(go.Scatter(
                x=df_flex_p[x_col].astype(str),
                y=df_flex_p['Flex_Bud'],
                name='Flex Bud',
                mode='lines+markers+text',
                line=dict(color='#FF6B35', width=2, dash='dot'),
                marker=dict(color='#FF6B35', size=7),
                text=df_flex_p['Flex_Bud'],
                texttemplate='%{y:,.2f}',
                textposition='top center',
                cliponaxis=False,
                textfont=dict(size=11, color='#FF6B35'),
                **_flex_kw,
            ), row=bar_row, col=1)

            # ════════════════════════════════════════
            # DELTA (mini-barras no topo)
            # ════════════════════════════════════════
            delta_label = 'BE' if modo_be else 'Real'
            delta_titulo = f'Delta ({delta_label} - Flex Bud)'

            # Agregar período sem Tipo para comparar com Flex
            delta_real = df_p.groupby(x_col, as_index=False, observed=False)[coluna].sum()
            delta_flex_agg = df_flex_p.groupby(x_col, as_index=False, observed=False)['Flex_Bud'].sum()
            delta_data = delta_real.merge(delta_flex_agg, on=x_col, how='left')
            delta_data['Flex_Bud'] = delta_data['Flex_Bud'].fillna(0)
            delta_data['Delta'] = delta_data[coluna] - delta_data['Flex_Bud']
            delta_data[x_col] = pd.Categorical(delta_data[x_col], categories=ordem_per, ordered=True)
            delta_data = delta_data.sort_values(x_col)

            delta_colors = ['#00AA00' if d < 0 else '#FF0000' for d in delta_data['Delta']]

            # ── Tooltip rico para Delta (se disponível) ──
            _delta_hover = None
            if hover_payloads_delta:
                _delta_hover = [hover_payloads_delta.get(str(p).strip(), '') for p in delta_data[x_col].astype(str)]
                if not any(_delta_hover):
                    _delta_hover = None

            _delta_kw = (
                dict(hovertext=_delta_hover, hovertemplate='%{hovertext}<extra></extra>')
                if _delta_hover
                else dict(hovertemplate='%{x}<br>Delta: %{y:,.2f}<extra>' + delta_titulo + '</extra>')
            )
            fig.add_trace(go.Bar(
                x=delta_data[x_col].astype(str),
                y=delta_data['Delta'],
                name=delta_titulo,
                marker_color=delta_colors,
                width=0.315,
                text=delta_data['Delta'],
                texttemplate='%{y:,.2f}',
                textposition='outside',
                cliponaxis=False,
                textfont=dict(size=11, color=delta_colors),
                showlegend=False,
                **_delta_kw,
            ), row=1, col=1)

            fig.update_yaxes(title_text=delta_titulo, row=1, col=1,
                             showgrid=False, zeroline=True,
                             zerolinecolor='rgba(160,160,160,0.35)', zerolinewidth=0.5,
                             tickfont=dict(size=11))
            fig.update_xaxes(
                row=1, col=1,
                showline=False,
                showgrid=False,
                linecolor='rgba(0,0,0,0)',
                linewidth=0,
                ticks='',
            )

        # ════════════════════════════════════════
        # LAYOUT FINAL
        # ════════════════════════════════════════
        n_periodos = len(ordem_per) if ordem_per else 1
        altura = min(620, max(350, 22 * n_periodos + 180))

        fig.update_yaxes(title_text=titulo_y, row=bar_row, col=1,
                 showgrid=False, automargin=True)
        fig.update_xaxes(title_text='Período', row=bar_row, col=1,
                         categoryorder='array', categoryarray=ordem_per,
                         automargin=True, title_standoff=20)
        if tem_flex:
            fig.update_xaxes(showticklabels=False, row=1, col=1,
                             categoryorder='array', categoryarray=ordem_per)
            fig.update_xaxes(showline=False, row=1, col=1)
            fig.update_xaxes(showline=False, row=bar_row, col=1)
        else:
            fig.update_xaxes(showline=False, row=bar_row, col=1)

        _altura_base = altura + (100 if tem_flex else 0)
        _altura_final = int(_altura_base * 1.24) if tem_flex else _altura_base

        fig.update_layout(
            height=_altura_final,
            barmode='stack' if (_usar_tipo and modo_be) else 'group',
            legend=dict(
                orientation='h', yanchor='top', y=-0.24,
                xanchor='center', x=0.5, font=dict(size=10),
            ),
            margin=dict(l=60, r=30, t=130, b=130),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )

        return fig

    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None


def _preparar_flex(df_flex, tem_ano, tipo, ordem_per):
    """Prepara dados do Flex Budget para o gráfico."""
    if df_flex is None or len(df_flex) == 0:
        return None

    colunas_flex = ['Período', 'Flex_Bud']
    if tem_ano and 'Ano' in df_flex.columns:
        colunas_flex.insert(0, 'Ano')

    df_flex_p = df_flex[colunas_flex].copy()
    df_flex_p['Período'] = df_flex_p['Período'].astype(str)

    if tem_ano and 'Ano' in df_flex_p.columns:
        df_flex_p['Ano'] = df_flex_p['Ano'].astype(str)

    df_flex_p = ordenar_por_mes(df_flex_p)

    if tipo == 'CPU (Custo por Unidade)' and 'Vol_Actual' in df_flex.columns:
        colunas_vol_merge = ['Período', 'Vol_Actual']
        if tem_ano and 'Ano' in df_flex.columns:
            colunas_vol_merge.insert(0, 'Ano')
            merge_on = ['Ano', 'Período']
        else:
            merge_on = 'Período'
        df_flex_vol = df_flex[colunas_vol_merge].copy()
        df_flex_vol['Período'] = df_flex_vol['Período'].astype(str)
        if tem_ano and 'Ano' in df_flex_vol.columns:
            df_flex_vol['Ano'] = df_flex_vol['Ano'].astype(str)
        df_flex_p = df_flex_p.merge(df_flex_vol, on=merge_on, how='left')
        df_flex_p['Vol_Actual'] = df_flex_p['Vol_Actual'].fillna(0)
        df_flex_p['Flex_Bud'] = calcular_cpu(df_flex_p['Flex_Bud'], df_flex_p['Vol_Actual'])

    df_flex_p = df_flex_p.replace([np.inf, -np.inf], 0)
    df_flex_p['Flex_Bud'] = df_flex_p['Flex_Bud'].fillna(0)
    df_flex_p = df_flex_p.reset_index(drop=True)

    # Nota: mesmo com Flex_Bud=0 em todos os períodos, o chart é válido
    # (mostra Delta = Real - 0, útil quando budget por veículo é zero)

    return df_flex_p
