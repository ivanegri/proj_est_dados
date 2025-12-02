import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob
import os
from collections import Counter
import numpy as np

# ==============================================================================
# 1. CONFIGURAÇÃO GERAL E ESTILIZAÇAO
# ==============================================================================
st.set_page_config(
    page_title="Spotify Data Analytics | Fatec",
    page_icon="🎧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Paleta de Cores Personalizada (Spotifi Dark Theme)
COLOR_SPOTIFY_GREEN = "#1DB954"
COLOR_SPOTIFY_BLACK = "#191414"
COLOR_SPOTIFY_WHITE = "#FFFFFF"
COLOR_ACCENT_RED = "#E91429"
COLOR_ACCENT_BLUE = "#1E90FF"
COLOR_ACCENT_YELLOW = "#FFD700"

# CSS 
st.markdown("""
    <style>
        .block-container {padding-top: 1.5rem; padding-bottom: 3rem;}
        h1, h2, h3 {font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; font-weight: 700;}
        .stMetric {background-color: #0E1117; border: 1px solid #303030; padding: 10px; border-radius: 5px;}
        .stMetric label {color: #b0b0b0;}
        .stMetric [data-testid="stMetricValue"] {color: #1DB954;}
        .insight-box {
            background: linear-gradient(135deg, #1DB954 0%, #1ed760 100%);
            padding: 15px;
            border-radius: 10px;
            color: white;
            font-weight: bold;
            margin: 10px 0;
        }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. CAMADA DE DADOS (ETL) - MELHORADA
# ==============================================================================
@st.cache_data(ttl=3600, show_spinner="Processando base de dados...")
def load_and_process_data():
    """
    Carrega, consolida e limpa os dados brutos com tratamento robusto de erros.
    """
    path = "raw_data"
    
    # ✅ CORREÇÃO: Busca pelo nome correto dos arquivos
    all_files = glob.glob(os.path.join(path, "spotify_hits_brasil_*.csv"))
    
    # Se não encontrar, tenta o padrão antigo (retrocompatibilidade)
    if not all_files:
        all_files = glob.glob(os.path.join(path, "dados_brasil_*.csv"))
    
    if not all_files:
        return None

    df_list = []
    for filename in all_files:
        try:
            df_temp = pd.read_csv(filename, encoding='utf-8')
            df_list.append(df_temp)
        except Exception as e:
            st.warning(f"⚠️ Arquivo ignorado ({os.path.basename(filename)}): {e}")
            continue

    if not df_list:
        return None

    df = pd.concat(df_list, ignore_index=True)

    # ✅ VALIDAÇÃO: Verifica colunas essenciais
    required_cols = ['track_id', 'track_name', 'year']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        st.error(f"❌ Colunas obrigatórias ausentes: {missing_cols}")
        return None

    # --- Feature Engineering (Engenharia de Atributos) ---
    
    # 1. Tratamento Temporal
    if 'duration_ms' in df.columns:
        df['duration_min'] = pd.to_numeric(df['duration_ms'], errors='coerce') / 60000
        df['duration_min'] = df['duration_min'].fillna(df['duration_min'].median())
    else:
        df['duration_min'] = 3.0  # Valor padrão razoável
    
    # 2. Tratamento de Categóricas
    if 'explicit' in df.columns:
        df['content_type'] = df['explicit'].apply(
            lambda x: 'Explícito' if str(x).lower() in ['true', '1', 'yes'] else 'Livre'
        )
    else:
        df['content_type'] = 'Livre'
        
    # 3. Tratamento Numérico (com valores padrão seguros)
    numeric_cols = {
        'track_popularity': 0,
        'primary_artist_followers': 0,
        'primary_artist_popularity': 0
    }
    
    for col, default_val in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(default_val)
        else:
            df[col] = default_val

    # 4. Extração de Gêneros 
    if 'primary_artist_genres' in df.columns:
        df['main_genre'] = df['primary_artist_genres'].apply(extract_main_genre)
    else:
        df['main_genre'] = 'Outros'

    # 5. Classificação por Popularidade 
    df['popularity_tier'] = pd.cut(
        df['track_popularity'], 
        bins=[0, 30, 60, 100], 
        labels=['Nicho', 'Moderado', 'Hit']
    )

    # 6. Remoção de Duplicatas
    df = df.drop_duplicates(subset=['track_id'])
    
    # 7. Limpeza de valores inválidos
    df = df[df['year'] > 2000]  # Remove dados claramente errados
    df = df[df['duration_min'] > 0.5]  # Remove duração impossível (< 30s)
    
    return df

def extract_main_genre(genres_str):
    """
    Extrai o gênero principal de uma string/lista de gêneros.
    """
    if pd.isna(genres_str) or genres_str == '[]':
        return 'Outros'
    
    try:
        # Remove colchetes e aspas, divide por vírgula
        genres_str = str(genres_str).replace('[', '').replace(']', '').replace("'", "")
        genres = [g.strip() for g in genres_str.split(',') if g.strip()]
        
        if not genres:
            return 'Outros'
        
        # Mapeia para categorias principais
        genre_map = {
            'sertanejo': 'Sertanejo',
            'funk': 'Funk',
            'pagode': 'Pagode',
            'samba': 'Samba',
            'forro': 'Forró',
            'trap': 'Trap/Hip-Hop',
            'rap': 'Trap/Hip-Hop',
            'hip hop': 'Trap/Hip-Hop',
            'pop': 'Pop',
            'rock': 'Rock'
        }
        
        first_genre = genres[0].lower()
        for key, category in genre_map.items():
            if key in first_genre:
                return category
        
        return 'Outros'
    except:
        return 'Outros'

# ==============================================================================
# 3. INTERFACE DE USUÁRIO (FRONTEND) - MELHORADA
# ==============================================================================

def main():
    # --- Cabeçalho ---
    col_logo, col_title = st.columns([1, 6])
    with col_logo:
        st.image("https://upload.wikimedia.org/wikipedia/commons/1/19/Spotify_logo_without_text.svg", width=80)
    with col_title:
        st.title("🎵 Panorama do Mercado Musical Brasileiro")
        st.caption("Análise estratégica baseada em dados extraídos via API Spotify Web | Fatec Jundiaí")

    # --- Carregamento ---
    df = load_and_process_data()

    if df is None:
        st.error("🛑 **Erro Crítico:** Base de dados não encontrada.")
        st.info("📁 Certifique-se de que a pasta `raw_data/` contém arquivos CSV.")
        st.code("python API_SPOTIFI_TRACKS_V2.py --years 2020 2021 2022 2023 2024", language="bash")
        st.stop()

    # --- Sidebar (Controles) ---
    with st.sidebar:
        st.header("🎛️ Painel de Controle")
        
        # Filtro de Ano
        all_years = sorted(df['year'].unique())
        selected_years = st.multiselect(
            "📅 Período de Análise:", 
            options=all_years, 
            default=all_years,
            help="Selecione um ou mais anos para comparar."
        )
        
        # Filtro de Popularidade
        min_pop, max_pop = st.slider(
            "🔥 Índice de Popularidade:",
            0, 100, (0, 100),
            help="0-30: Nicho | 31-60: Moderado | 61-100: Hit"
        )

        # ✅ NOVO: Filtro por Gênero
        if 'main_genre' in df.columns:
            all_genres = ['Todos'] + sorted(df['main_genre'].unique().tolist())
            selected_genre = st.selectbox("🎸 Gênero Musical:", all_genres)
        else:
            selected_genre = 'Todos'

        

    # Aplicação dos Filtros
    df_filtered = df[
        (df['year'].isin(selected_years)) & 
        (df['track_popularity'] >= min_pop) & 
        (df['track_popularity'] <= max_pop)
    ]

    # Filtro de gênero
    if selected_genre != 'Todos' and 'main_genre' in df.columns:
        df_filtered = df_filtered[df_filtered['main_genre'] == selected_genre]

    if df_filtered.empty:
        st.warning("⚠️ Nenhum dado corresponde aos filtros selecionados.")
        st.stop()

    # --- KPIs (Indicadores Chave de Performance) ---
    st.markdown("### 📊 Métricas Globais")
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    
    with kpi1:
        st.metric("🎵 Total de Faixas", f"{len(df_filtered):,}".replace(",", "."))
    with kpi2:
        media_pop = df_filtered['track_popularity'].mean()
        st.metric("⭐ Popularidade Média", f"{media_pop:.1f}")
    with kpi3:
        media_dur = df_filtered['duration_min'].mean()
        st.metric("⏱️ Duração Média", f"{media_dur:.2f} min")
    with kpi4:
        total_artistas = df_filtered['primary_artist_id'].nunique()
        st.metric("👤 Artistas Únicos", f"{total_artistas}")
    with kpi5:
        pct_explicit = (df_filtered['content_type'] == 'Explícito').mean() * 100
        st.metric("🔞 % Explícito", f"{pct_explicit:.1f}%")

    st.markdown("---")

    # --- NAVEGAÇÃO POR ABAS ---
    tab_trends, tab_analysis, tab_artists, tab_genres, tab_raw = st.tabs([
        "📈 Tendências de Mercado", 
        "🔬 Laboratório de Dados", 
        "🏆 Top Charts",
        "🎸 Análise de Gêneros",  # ✅ NOVA ABA
        "💾 Dados Brutos"
    ])

    # ================= ABA 1: TENDÊNCIAS =================
    with tab_trends:
        st.subheader("Evolução Temporal do Consumo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 📉 Efeito 'Short Music'")
            df_year_stats = df_filtered.groupby('year').agg({
                'duration_min': 'mean',
                'track_id': 'count'
            }).reset_index()
            df_year_stats.columns = ['year', 'duration_min', 'track_count']
            
            fig_dur = go.Figure()
            
            # Linha principal
            fig_dur.add_trace(go.Scatter(
                x=df_year_stats['year'],
                y=df_year_stats['duration_min'],
                mode='lines+markers',
                name='Duração Média',
                line=dict(color=COLOR_SPOTIFY_GREEN, width=3),
                marker=dict(size=10)
            ))
            
            # ✅ NOVO: Linha de tendência
            if len(df_year_stats) > 1:
                z = np.polyfit(df_year_stats['year'], df_year_stats['duration_min'], 1)
                p = np.poly1d(z)
                fig_dur.add_trace(go.Scatter(
                    x=df_year_stats['year'],
                    y=p(df_year_stats['year']),
                    mode='lines',
                    name='Tendência',
                    line=dict(color=COLOR_ACCENT_YELLOW, dash='dash', width=2)
                ))
            
            fig_dur.update_layout(
                title="Duração Média das Músicas ao Longo dos Anos",
                xaxis_title="Ano",
                yaxis_title="Duração (minutos)",
                template="plotly_dark",
                hovermode='x unified'
            )
            
            st.plotly_chart(fig_dur, use_container_width=True)
            
            # ✅ INSIGHT AUTOMÁTICO
            if len(df_year_stats) > 1:
                first_val = df_year_stats['duration_min'].iloc[0]
                last_val = df_year_stats['duration_min'].iloc[-1]
                diff = ((last_val - first_val) / first_val) * 100
                trend = "redução" if diff < 0 else "aumento"
                st.markdown(f"""
                <div class="insight-box">
                    💡 <b>Insight:</b> Houve uma {trend} de <b>{abs(diff):.1f}%</b> 
                    na duração média entre {df_year_stats['year'].min()} e {df_year_stats['year'].max()}.
                </div>
                """, unsafe_allow_html=True)

        with col2:
            st.markdown("##### 🔞 Distribuição de Conteúdo")
            df_explicit = df_filtered.groupby(['year', 'content_type']).size().reset_index(name='count')
            
            fig_bar = px.bar(
                df_explicit, x='year', y='count', color='content_type',
                barmode='group',
                title="Volume: Explícito vs. Livre",
                color_discrete_map={'Explícito': COLOR_ACCENT_RED, 'Livre': COLOR_ACCENT_BLUE},
                template="plotly_dark"
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # ✅ NOVO: Gráfico de Popularidade ao Longo do Tempo
        st.markdown("##### 📊 Tendência de Popularidade")
        df_pop_trend = df_filtered.groupby('year')['track_popularity'].agg(['mean', 'median']).reset_index()
        
        fig_pop = go.Figure()
        fig_pop.add_trace(go.Scatter(x=df_pop_trend['year'], y=df_pop_trend['mean'], 
                                      mode='lines+markers', name='Média', 
                                      line=dict(color=COLOR_SPOTIFY_GREEN, width=2)))
        fig_pop.add_trace(go.Scatter(x=df_pop_trend['year'], y=df_pop_trend['median'], 
                                      mode='lines+markers', name='Mediana',
                                      line=dict(color=COLOR_ACCENT_BLUE, width=2, dash='dot')))
        
        fig_pop.update_layout(
            title="Evolução da Popularidade (Média vs. Mediana)",
            xaxis_title="Ano",
            yaxis_title="Popularidade",
            template="plotly_dark"
        )
        st.plotly_chart(fig_pop, use_container_width=True)

    # ================= ABA 2: ANÁLISE ESTATÍSTICA =================
    with tab_analysis:
        st.subheader("Análise Multivariada")
        
        col_stat1, col_stat2 = st.columns([2, 1])
        
        with col_stat1:
            st.markdown("##### 🔬 Correlação: Duração x Popularidade")
            
            # ✅ MELHORIA: Amostragem para performance (se dataset > 1000)
            df_sample = df_filtered.sample(min(1000, len(df_filtered)))
            
            fig_scatter = px.scatter(
                df_sample, 
                x='duration_min', 
                y='track_popularity',
                color='content_type',
                size='primary_artist_followers',
                hover_data=['track_name', 'artists', 'year'],
                title="Dispersão: Duração vs. Popularidade",
                color_discrete_map={'Explícito': COLOR_ACCENT_RED, 'Livre': COLOR_SPOTIFY_GREEN},
                opacity=0.6,
                template="plotly_dark",
                labels={'duration_min': 'Duração (min)', 'track_popularity': 'Popularidade'}
            )
            
            # Linhas de referência
            fig_scatter.add_hline(y=df_filtered['track_popularity'].mean(), 
                                 line_dash="dash", line_color="white", 
                                 annotation_text="Média Pop.", annotation_position="right")
            fig_scatter.add_vline(x=df_filtered['duration_min'].mean(), 
                                 line_dash="dash", line_color="white", 
                                 annotation_text="Média Dur.", annotation_position="top")
            
            st.plotly_chart(fig_scatter, use_container_width=True)
            
        with col_stat2:
            st.markdown("##### 📦 Distribuição por Categoria")
            fig_box = px.box(
                df_filtered, 
                x='content_type', 
                y='track_popularity',
                color='content_type',
                color_discrete_map={'Explícito': COLOR_ACCENT_RED, 'Livre': COLOR_ACCENT_BLUE},
                template="plotly_dark",
                points="outliers"
            )
            st.plotly_chart(fig_box, use_container_width=True)
            
            # ✅ NOVO: Estatísticas descritivas
            st.markdown("**Estatísticas:**")
            stats = df_filtered.groupby('content_type')['track_popularity'].describe()[['mean', '50%', 'std']]
            stats.columns = ['Média', 'Mediana', 'Desvio Padrão']
            st.dataframe(stats.round(2), use_container_width=True)
            
        # Expander com explicação técnica
        with st.expander("📚 Interpretação Estatística"):
            st.markdown("""
            **Gráfico de Dispersão:**
            - Cada ponto representa uma música
            - Tamanho do ponto = número de seguidores do artista
            - Linhas tracejadas = médias globais
            
            **Boxplot:**
            - Caixa central = 50% dos dados (IQR)
            - Linha central = mediana
            - Pontos isolados = outliers (hits virais ou fracassos)
            
            **Interpretação:**
            - Músicas entre 2-4 min tendem a ser mais populares
            - Conteúdo explícito tem mediana de popularidade similar ao livre
            """)

    # ================= ABA 3: TOP CHARTS =================
    with tab_artists:
        st.subheader("🏆 Rankings e Destaques")
        
        col_rank1, col_rank2 = st.columns(2)
        
        with col_rank1:
            st.markdown("##### 🎤 Top 10 Artistas")
            top_artists = df_filtered['artists'].value_counts().head(10).reset_index()
            top_artists.columns = ['Artista', 'Qtd Faixas']
            
            fig_rank = px.bar(
                top_artists, 
                y='Artista', 
                x='Qtd Faixas', 
                orientation='h',
                color='Qtd Faixas',
                color_continuous_scale='Greens',
                template="plotly_dark",
                text='Qtd Faixas'
            )
            fig_rank.update_layout(yaxis={'categoryorder':'total ascending'})
            fig_rank.update_traces(textposition='outside')
            st.plotly_chart(fig_rank, use_container_width=True)
            
        with col_rank2:
            st.markdown("##### 💿 Top 10 Álbuns")
            top_albums = df_filtered['album_name'].value_counts().head(10).reset_index()
            top_albums.columns = ['Álbum', 'Faixas']
            st.dataframe(top_albums, use_container_width=True, hide_index=True)

        # ✅ NOVO: Top Músicas Mais Populares
        st.markdown("---")
        st.markdown("##### 🔥 Top 20 Músicas Mais Populares")
        top_tracks = df_filtered.nlargest(20, 'track_popularity')[
            ['track_name', 'artists', 'track_popularity', 'year', 'duration_min', 'spotify_url']
        ].reset_index(drop=True)
        top_tracks.index += 1  # Começa em 1
        
        st.dataframe(
            top_tracks,
            column_config={
                "track_name": "Música",
                "artists": "Artista(s)",
                "track_popularity": st.column_config.ProgressColumn("Popularidade", max_value=100),
                "year": "Ano",
                "duration_min": st.column_config.NumberColumn("Duração (min)", format="%.2f"),
                "spotify_url": st.column_config.LinkColumn("🔗 Spotify")
            },
            use_container_width=True,
            height=400
        )

    # ================= ABA 4: ANÁLISE DE GÊNEROS (NOVA!) =================
    with tab_genres:
        st.subheader("🎸 Panorama por Gênero Musical")
        
        if 'main_genre' not in df_filtered.columns:
            st.warning("⚠️ Dados de gênero não disponíveis neste dataset.")
        else:
            col_g1, col_g2 = st.columns(2)
            
            with col_g1:
                st.markdown("##### 📊 Distribuição de Gêneros")
                genre_counts = df_filtered['main_genre'].value_counts().reset_index()
                genre_counts.columns = ['Gênero', 'Quantidade']
                
                fig_pie = px.pie(
                    genre_counts, 
                    values='Quantidade', 
                    names='Gênero',
                    title="Participação de Gêneros no Dataset",
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col_g2:
                st.markdown("##### ⭐ Popularidade por Gênero")
                genre_pop = df_filtered.groupby('main_genre')['track_popularity'].mean().sort_values(ascending=False).reset_index()
                genre_pop.columns = ['Gênero', 'Popularidade Média']
                
                fig_genre_bar = px.bar(
                    genre_pop,
                    x='Popularidade Média',
                    y='Gênero',
                    orientation='h',
                    color='Popularidade Média',
                    color_continuous_scale='Viridis',
                    template="plotly_dark"
                )
                fig_genre_bar.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_genre_bar, use_container_width=True)
            
            # Evolução de gêneros ao longo do tempo
            st.markdown("##### 📈 Evolução de Gêneros ao Longo dos Anos")
            genre_year = df_filtered.groupby(['year', 'main_genre']).size().reset_index(name='count')
            
            fig_genre_line = px.line(
                genre_year,
                x='year',
                y='count',
                color='main_genre',
                title="Número de Músicas por Gênero (Temporal)",
                template="plotly_dark",
                markers=True
            )
            st.plotly_chart(fig_genre_line, use_container_width=True)

    # ================= ABA 5: DADOS BRUTOS =================
    with tab_raw:
        st.subheader("💾 Exploração da Base de Dados")
        st.markdown("Visualize e exporte os dados tratados utilizados neste dashboard.")
        
        # Seletor de colunas
        default_cols = ['track_name', 'artists', 'year', 'track_popularity', 'duration_min', 'content_type']
        available_cols = [col for col in default_cols if col in df_filtered.columns]
        
        cols_view = st.multiselect(
            "Selecionar Colunas:", 
            options=df_filtered.columns.tolist(), 
            default=available_cols
        )
        
        if cols_view:
            st.dataframe(df_filtered[cols_view], use_container_width=True, height=400)
        else:
            st.warning("Selecione ao menos uma coluna para visualizar.")
        
        # Botão de Download
        csv = df_filtered.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button(
            label="📥 Baixar Dataset Completo (CSV)",
            data=csv,
            file_name=f'spotify_dataset_{pd.Timestamp.now().strftime("%Y%m%d")}.csv',
            mime='text/csv',
            help="Arquivo compatível com Excel, Power BI e Python"
        )
        
        # ✅ NOVO: Estatísticas gerais
        with st.expander("📊 Estatísticas Gerais do Dataset"):
            col_s1, col_s2, col_s3 = st.columns(3)
            with col_s1:
                st.metric("Total de Registros", len(df_filtered))
                st.metric("Colunas", len(df_filtered.columns))
            with col_s2:
                st.metric("Memória Utilizada", f"{df_filtered.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                st.metric("Valores Nulos", df_filtered.isnull().sum().sum())
            with col_s3:
                st.metric("Anos Cobertos", f"{df_filtered['year'].min()} - {df_filtered['year'].max()}")
                st.metric("Período", f"{df_filtered['year'].nunique()} anos")


# Execução Principal
if __name__ == "__main__":
    main()