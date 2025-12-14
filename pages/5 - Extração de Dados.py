import streamlit as st
import pandas as pd
import os
import shutil
import subprocess
from datetime import datetime
from versionamento import obter_versao_atual
import sys

# Adicionar o diretório raiz ao path para importar os módulos de processamento
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from processamento_dados import processar_completo as processar_dados_reais_completo
    from processamento_dados_BUD import processar_completo_bud as processar_dados_budget_completo
except ImportError as e:
    st.error(f"❌ Erro ao importar módulos de processamento: {e}")
    st.stop()

# Função para obter data e hora de atualização dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da última atualização dos arquivos de dados"""
    arquivos_dados = [
        os.path.join("dados", "historico_consolidado", "df_final_historico.parquet"),
        os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet"),
        os.path.join("dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
    ]
    
    data_atualizacao = None
    for arquivo in arquivos_dados:
        if os.path.exists(arquivo):
            data_modificacao = os.path.getmtime(arquivo)
            if data_atualizacao is None or data_modificacao > data_atualizacao:
                data_atualizacao = data_modificacao
    
    if data_atualizacao:
        dt = datetime.fromtimestamp(data_atualizacao)
        meses = {
            1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
            5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
            9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
        }
        return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
    return "Não disponível"

# Exibir data de atualização dos dados no topo
data_atualizacao = obter_data_atualizacao_dados()
st.markdown(f"""
<div style='text-align: right; color: #666; padding: 5px 10px; font-size: 0.85rem;'>
    📅 Dados atualizados em: {data_atualizacao}
</div>
""", unsafe_allow_html=True)

# CSS para reduzir fonte das configurações da sidebar
st.markdown("""
    <style>
        /* Reduzir fonte do header da sidebar */
        .css-1d391kg h3 {
            font-size: 0.9rem !important;
        }
        /* Reduzir fonte dos radio buttons da sidebar */
        .css-1d391kg div[data-testid="stRadio"] label {
            font-size: 0.75rem !important;
        }
        .css-1d391kg div[data-testid="stRadio"] label p {
            font-size: 0.75rem !important;
        }
        /* Reduzir fonte do number input da sidebar */
        .css-1d391kg div[data-testid="stNumberInput"] label {
            font-size: 0.75rem !important;
        }
        .css-1d391kg div[data-testid="stNumberInput"] label p {
            font-size: 0.75rem !important;
        }
        /* Reduzir fonte do info box da sidebar */
        .css-1d391kg .stAlert {
            font-size: 0.75rem !important;
        }
        .css-1d391kg .stAlert p {
            font-size: 0.75rem !important;
        }
        .css-1d391kg .stAlert strong {
            font-size: 0.8rem !important;
        }
    </style>
""", unsafe_allow_html=True)

# Configuração da página
st.set_page_config(
    page_title="Extração de Dados - TC",
    page_icon="📥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título
st.title("📥 Extração e Processamento de Dados")
st.markdown("---")

# Seleção de configurações na página principal
col_config1, col_config2 = st.columns(2)

with col_config1:
    tipo_extracao = st.radio(
        "📊 Selecione o tipo de extração:",
        ["📊 Dados REAIS (dados.ipynb)", "💰 Dados BUDGET (dados_BUD.ipynb)", "🔄 Ambos"],
        horizontal=True
    )

with col_config2:
    ano_padrao = datetime.now().year
    ano_selecionado = st.number_input(
        "📅 Ano para processar:",
        min_value=2020,
        max_value=2100,
        value=ano_padrao,
        step=1
    )

st.markdown("---")

# Sidebar - Informações
st.sidebar.header("ℹ️ Informações")
st.sidebar.info("""
**📋 Instruções:**
1. Selecione o tipo de extração
2. Informe o ano
3. Verifique os arquivos necessários
4. Execute o processamento

**🔄 Sincronização:**
Os módulos Python são convertidos dos notebooks `.ipynb` mantendo toda a lógica original.
Use o botão na página principal para verificar se estão atualizados.
""")

# ==========================================
# FUNÇÕES DE VALIDAÇÃO
# ==========================================

def verificar_arquivos_reais(ano):
    """Verifica arquivos necessários para dados REAIS"""
    pasta_ano = f'dados/{ano}'
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume e Sapiens'
    }
    
    arquivos_ok = []
    arquivos_faltando = []
    
    for arquivo, descricao in arquivos_necessarios.items():
        caminho_ano = os.path.join(pasta_ano, arquivo)
        caminho_raiz = os.path.join('.', arquivo)
        
        if os.path.exists(caminho_ano):
            arquivos_ok.append((arquivo, caminho_ano, 'pasta_ano'))
        elif os.path.exists(caminho_raiz):
            arquivos_ok.append((arquivo, caminho_raiz, 'raiz'))
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    return arquivos_ok, arquivos_faltando

def verificar_arquivos_budget(ano):
    """Verifica arquivos necessários para dados BUDGET"""
    pasta_ano = f'dados/{ano}'
    pasta_bud = f'dados/{ano}/BUD'
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume'
    }
    
    arquivos_ok = []
    arquivos_faltando = []
    
    for arquivo, descricao in arquivos_necessarios.items():
        caminho_ano = os.path.join(pasta_ano, arquivo)
        caminho_bud = os.path.join(pasta_bud, arquivo)
        caminho_raiz = os.path.join('.', arquivo)
        
        if os.path.exists(caminho_ano):
            arquivos_ok.append((arquivo, caminho_ano, 'pasta_ano'))
        elif os.path.exists(caminho_bud):
            arquivos_ok.append((arquivo, caminho_bud, 'pasta_bud'))
        elif os.path.exists(caminho_raiz):
            arquivos_ok.append((arquivo, caminho_raiz, 'raiz'))
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    return arquivos_ok, arquivos_faltando

# ==========================================
# INTERFACE PRINCIPAL
# ==========================================

# Verificação de sincronização (dentro da página)
st.markdown("---")
col_sync1, col_sync2, col_sync3 = st.columns([2, 1, 1])
with col_sync1:
    st.markdown("### 🔄 Verificação de Sincronização")
    st.caption("Verifica se os módulos Python estão atualizados com os notebooks `.ipynb`")
with col_sync2:
    st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento
    verificar_sync = st.button("🔄 Verificar Sincronização", use_container_width=True, type="secondary")
with col_sync3:
    st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento

if verificar_sync:
    try:
        with st.spinner("🔄 Verificando sincronização..."):
            resultado = subprocess.run(
                [sys.executable, "sincronizar_notebooks.py"],
                capture_output=True,
                text=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
        
        with st.expander("📊 Relatório de Sincronização", expanded=True):
            st.code(resultado.stdout, language="text")
        
        if resultado.returncode == 0 and "sincronizados" in resultado.stdout:
            st.success("✅ Todos os módulos estão sincronizados com os notebooks!")
        else:
            st.warning("⚠️ Verifique o relatório acima para detalhes")
    except Exception as e:
        st.error(f"❌ Erro ao verificar sincronização: {e}")
        st.exception(e)

st.markdown("---")

# Tabs para organização
tab1, tab2, tab3 = st.tabs(["📋 Validação de Arquivos", "⚙️ Executar Processamento", "📊 Status e Logs"])

# TAB 1: Validação de Arquivos
with tab1:
    st.header("📋 Validação de Arquivos Necessários")
    
    # ==========================================
    # SEÇÃO DE UPLOAD DE ARQUIVOS
    # ==========================================
    st.markdown("### 📤 Upload de Arquivos")
    st.info("""
    **💡 Dica:** Se os arquivos não estiverem na pasta `dados/{ano_selecionado}/` ou na raiz do projeto,
    você pode fazer upload diretamente aqui. Os arquivos serão salvos automaticamente na pasta do ano.
    """)
    
    if tipo_extracao in ["📊 Dados REAIS (dados.ipynb)", "🔄 Ambos"]:
        st.markdown("#### 📊 Upload para Dados REAIS")
        
        col_upload1, col_upload2 = st.columns(2)
        
        with col_upload1:
            pasta_ano = f'dados/{ano_selecionado}'
            caminho_destino = os.path.join(pasta_ano, 'Dados SAPIENS.xlsx')
            
            # Verificar se arquivo já existe ANTES do upload
            if os.path.exists(caminho_destino):
                st.warning(f"⚠️ O arquivo `Dados SAPIENS.xlsx` já existe em `{caminho_destino}`")
                st.info("💡 Se você fizer upload de um novo arquivo, o existente será sobrescrito.")
            
            arquivo_sapiens_upload = st.file_uploader(
                "📄 Upload: Dados SAPIENS.xlsx",
                type=['xlsx'],
                key="upload_sapiens_reais",
                help="Faça upload do arquivo Dados SAPIENS.xlsx para dados REAIS"
            )
            
            if arquivo_sapiens_upload is not None:
                os.makedirs(pasta_ano, exist_ok=True)
                
                # Se arquivo existe, pedir confirmação
                if os.path.exists(caminho_destino):
                    st.warning(f"⚠️ O arquivo `Dados SAPIENS.xlsx` já existe e será sobrescrito!")
                    
                    if st.button("🔄 Confirmar Sobrescrita", key="btn_confirmar_sapiens_reais"):
                        with open(caminho_destino, 'wb') as f:
                            f.write(arquivo_sapiens_upload.getbuffer())
                        st.success(f"✅ Arquivo sobrescrito em: `{caminho_destino}`")
                        st.rerun()
                else:
                    with open(caminho_destino, 'wb') as f:
                        f.write(arquivo_sapiens_upload.getbuffer())
                    st.success(f"✅ Arquivo salvo em: `{caminho_destino}`")
                    st.rerun()
        
        with col_upload2:
            pasta_ano = f'dados/{ano_selecionado}'
            caminho_destino = os.path.join(pasta_ano, 'Reporting fluxo anexo.xlsx')
            
            # Verificar se arquivo já existe ANTES do upload
            if os.path.exists(caminho_destino):
                st.warning(f"⚠️ O arquivo `Reporting fluxo anexo.xlsx` já existe em `{caminho_destino}`")
                st.info("💡 Se você fizer upload de um novo arquivo, o existente será sobrescrito.")
            
            arquivo_rateio_upload = st.file_uploader(
                "📄 Upload: Reporting fluxo anexo.xlsx",
                type=['xlsx'],
                key="upload_rateio_reais",
                help="Faça upload do arquivo Reporting fluxo anexo.xlsx para dados REAIS"
            )
            
            if arquivo_rateio_upload is not None:
                os.makedirs(pasta_ano, exist_ok=True)
                
                # Se arquivo existe, pedir confirmação
                if os.path.exists(caminho_destino):
                    st.warning(f"⚠️ O arquivo `Reporting fluxo anexo.xlsx` já existe e será sobrescrito!")
                    
                    if st.button("🔄 Confirmar Sobrescrita", key="btn_confirmar_rateio_reais"):
                        with open(caminho_destino, 'wb') as f:
                            f.write(arquivo_rateio_upload.getbuffer())
                        st.success(f"✅ Arquivo sobrescrito em: `{caminho_destino}`")
                        st.rerun()
                else:
                    with open(caminho_destino, 'wb') as f:
                        f.write(arquivo_rateio_upload.getbuffer())
                    st.success(f"✅ Arquivo salvo em: `{caminho_destino}`")
                    st.rerun()
        
        st.markdown("---")
    
    if tipo_extracao in ["💰 Dados BUDGET (dados_BUD.ipynb)", "🔄 Ambos"]:
        st.markdown("#### 💰 Upload para Dados BUDGET")
        
        col_upload_bud1, col_upload_bud2 = st.columns(2)
        
        with col_upload_bud1:
            pasta_ano = f'dados/{ano_selecionado}'
            caminho_destino = os.path.join(pasta_ano, 'Dados SAPIENS.xlsx')
            
            # Verificar se arquivo já existe ANTES do upload
            if os.path.exists(caminho_destino):
                st.warning(f"⚠️ O arquivo `Dados SAPIENS.xlsx` já existe em `{caminho_destino}`")
                st.info("💡 Se você fizer upload de um novo arquivo, o existente será sobrescrito.")
            
            arquivo_sapiens_bud_upload = st.file_uploader(
                "📄 Upload: Dados SAPIENS.xlsx (BUD)",
                type=['xlsx'],
                key="upload_sapiens_budget",
                help="Faça upload do arquivo Dados SAPIENS.xlsx para dados BUDGET"
            )
            
            if arquivo_sapiens_bud_upload is not None:
                os.makedirs(pasta_ano, exist_ok=True)
                
                # Se arquivo existe, pedir confirmação
                if os.path.exists(caminho_destino):
                    st.warning(f"⚠️ O arquivo `Dados SAPIENS.xlsx` já existe e será sobrescrito!")
                    
                    if st.button("🔄 Confirmar Sobrescrita", key="btn_confirmar_sapiens_budget"):
                        with open(caminho_destino, 'wb') as f:
                            f.write(arquivo_sapiens_bud_upload.getbuffer())
                        st.success(f"✅ Arquivo sobrescrito em: `{caminho_destino}`")
                        st.rerun()
                else:
                    with open(caminho_destino, 'wb') as f:
                        f.write(arquivo_sapiens_bud_upload.getbuffer())
                    st.success(f"✅ Arquivo salvo em: `{caminho_destino}`")
                    st.rerun()
        
        with col_upload_bud2:
            pasta_ano = f'dados/{ano_selecionado}'
            caminho_destino = os.path.join(pasta_ano, 'Reporting fluxo anexo.xlsx')
            
            # Verificar se arquivo já existe ANTES do upload
            if os.path.exists(caminho_destino):
                st.warning(f"⚠️ O arquivo `Reporting fluxo anexo.xlsx` já existe em `{caminho_destino}`")
                st.info("💡 Se você fizer upload de um novo arquivo, o existente será sobrescrito.")
            
            arquivo_rateio_bud_upload = st.file_uploader(
                "📄 Upload: Reporting fluxo anexo.xlsx (BUD)",
                type=['xlsx'],
                key="upload_rateio_budget",
                help="Faça upload do arquivo Reporting fluxo anexo.xlsx para dados BUDGET"
            )
            
            if arquivo_rateio_bud_upload is not None:
                os.makedirs(pasta_ano, exist_ok=True)
                
                # Se arquivo existe, pedir confirmação
                if os.path.exists(caminho_destino):
                    st.warning(f"⚠️ O arquivo `Reporting fluxo anexo.xlsx` já existe e será sobrescrito!")
                    
                    if st.button("🔄 Confirmar Sobrescrita", key="btn_confirmar_rateio_budget"):
                        with open(caminho_destino, 'wb') as f:
                            f.write(arquivo_rateio_bud_upload.getbuffer())
                        st.success(f"✅ Arquivo sobrescrito em: `{caminho_destino}`")
                        st.rerun()
                else:
                    with open(caminho_destino, 'wb') as f:
                        f.write(arquivo_rateio_bud_upload.getbuffer())
                    st.success(f"✅ Arquivo salvo em: `{caminho_destino}`")
                    st.rerun()
        
        st.markdown("---")

# TAB 2: Executar Processamento
with tab2:
    st.header("⚙️ Executar Processamento")
    
    st.info("""
    **⚠️ Importante:**
    - Certifique-se de que todos os arquivos necessários estão presentes
    - O processamento pode levar alguns minutos
    - Não feche a página durante a execução
    """)
    
    col1, col2, col3 = st.columns(3)
    
    executar_reais = False
    executar_budget = False
    executar_ambos = False
    
    with col1:
        if tipo_extracao in ["📊 Dados REAIS (dados.ipynb)", "🔄 Ambos"]:
            executar_reais = st.button(
                "🚀 Executar dados.ipynb",
                type="primary",
                use_container_width=True
            )
    
    with col2:
        if tipo_extracao in ["💰 Dados BUDGET (dados_BUD.ipynb)", "🔄 Ambos"]:
            executar_budget = st.button(
                "🚀 Executar dados_BUD.ipynb",
                type="primary",
                use_container_width=True
            )
    
    with col3:
        if tipo_extracao == "🔄 Ambos":
            executar_ambos = st.button(
                "🚀 Executar Ambos",
                type="primary",
                use_container_width=True
            )
    
    # Container para logs
    log_container = st.container()
    
    # Executar processamentos
    if executar_reais or (executar_ambos and tipo_extracao == "🔄 Ambos"):
        with log_container:
            st.subheader("📊 Processando Dados REAIS...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_messages = st.empty()
            
            mensagens_log = []
            
            def callback_progresso(mensagem):
                mensagens_log.append(mensagem)
                log_messages.text("\n".join(mensagens_log[-10:]))  # Mostrar últimas 10 mensagens
            
            try:
                with st.spinner("🔄 Processando dados REAIS..."):
                    resultado = processar_dados_reais_completo(
                        ano=ano_selecionado,
                        continuar_sem_arquivos=False,
                        progress_callback=callback_progresso
                    )
                    
                    progress_bar.progress(100)
                    status_text.success("✅ Processamento de dados REAIS concluído com sucesso!")
                    st.json(resultado)
            except Exception as e:
                progress_bar.progress(0)
                status_text.error(f"❌ Erro durante processamento: {str(e)}")
                st.exception(e)
    
    if executar_budget or (executar_ambos and tipo_extracao == "🔄 Ambos"):
        with log_container:
            st.subheader("💰 Processando Dados BUDGET...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_messages = st.empty()
            
            mensagens_log = []
            
            def callback_progresso(mensagem):
                mensagens_log.append(mensagem)
                log_messages.text("\n".join(mensagens_log[-10:]))
            
            try:
                with st.spinner("🔄 Processando dados BUDGET..."):
                    resultado = processar_dados_budget_completo(
                        ano=ano_selecionado,
                        continuar_sem_arquivos=False,
                        progress_callback=callback_progresso
                    )
                    
                    progress_bar.progress(100)
                    status_text.success("✅ Processamento de dados BUDGET concluído com sucesso!")
                    st.json(resultado)
            except Exception as e:
                progress_bar.progress(0)
                status_text.error(f"❌ Erro durante processamento: {str(e)}")
                st.exception(e)

# TAB 3: Status e Logs
with tab3:
    st.header("📊 Status e Logs")
    
    st.subheader("📁 Estrutura de Pastas")
    
    if os.path.exists(f'dados/{ano_selecionado}'):
        st.success(f"✅ Pasta `dados/{ano_selecionado}/` existe")
        
        # Listar arquivos na pasta do ano
        arquivos_ano = os.listdir(f'dados/{ano_selecionado}')
        if arquivos_ano:
            st.markdown("**Arquivos na pasta do ano:**")
            for arquivo in arquivos_ano:
                caminho_completo = os.path.join(f'dados/{ano_selecionado}', arquivo)
                if os.path.isfile(caminho_completo):
                    tamanho = os.path.getsize(caminho_completo) / (1024 * 1024)  # MB
                    data_mod = datetime.fromtimestamp(os.path.getmtime(caminho_completo))
                    st.text(f"  📄 {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
    else:
        st.warning(f"⚠️ Pasta `dados/{ano_selecionado}/` não existe ainda")
    
    st.markdown("---")
    
    st.subheader("📚 Histórico Consolidado")
    
    if os.path.exists('dados/historico_consolidado'):
        st.success("✅ Pasta `dados/historico_consolidado/` existe")
        
        # Verificar arquivos principais
        arquivos_historico = [
            'df_final_historico.parquet',
            'df_vol_historico.parquet'
        ]
        
        for arquivo in arquivos_historico:
            caminho = os.path.join('dados/historico_consolidado', arquivo)
            if os.path.exists(caminho):
                tamanho = os.path.getsize(caminho) / (1024 * 1024)  # MB
                data_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                st.success(f"  ✅ {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
            else:
                st.warning(f"  ⚠️ {arquivo} não encontrado")
        
        # Verificar histórico BUD
        if os.path.exists('dados/historico_consolidado/BUD'):
            st.markdown("**Histórico BUD:**")
            arquivos_historico_bud = [
                'df_final_historico_BUD.parquet',
                'df_vol_historico_BUD.parquet'
            ]
            
            for arquivo in arquivos_historico_bud:
                caminho = os.path.join('dados/historico_consolidado/BUD', arquivo)
                if os.path.exists(caminho):
                    tamanho = os.path.getsize(caminho) / (1024 * 1024)  # MB
                    data_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                    st.success(f"  ✅ {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
                else:
                    st.warning(f"  ⚠️ {arquivo} não encontrado")
    else:
        st.warning("⚠️ Pasta `dados/historico_consolidado/` não existe ainda")

# Rodapé
st.markdown("---")
mes_atual = datetime.now().strftime("%B")
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
meses = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
mes_atual_nome = meses[datetime.now().month]

st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual_nome} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)

