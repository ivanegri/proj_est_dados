import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import spotipy
import streamlit as st
from spotipy.oauth2 import SpotifyClientCredentials

DEFAULT_CLIENT_ID = "cb730f30957149fcbcc3bf54e7a61b57"
DEFAULT_CLIENT_SECRET = "bd480f6529174bcf96b4a0763de19235"


@st.cache_resource(show_spinner=False)
def get_spotify_client() -> spotipy.Spotify:
    """Cria um cliente autenticado usando variáveis de ambiente ou fallback."""
    client_id = os.getenv("SPOTIPY_CLIENT_ID", DEFAULT_CLIENT_ID)
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET", DEFAULT_CLIENT_SECRET)
    auth = SpotifyClientCredentials(client_id=client_id,
                                    client_secret=client_secret)
    return spotipy.Spotify(auth_manager=auth)


def search_artists(sp_client: spotipy.Spotify, query: str, limit: int,
                   pages: int, min_popularity: int = 0,
                   offset: int = 0) -> Tuple[List[Dict], Optional[int]]:
    """Busca artistas paginando os resultados."""
    artists: List[Dict] = []
    current_offset = offset
    next_offset: Optional[int] = None

    for _ in range(pages):
        results = sp_client.search(q=query, type="artist", limit=limit,
                                   offset=current_offset)
        items = results["artists"]["items"]
        if not items:
            next_offset = None
            break

        for artist in items:
            if artist.get("popularity", 0) < min_popularity:
                continue
            artists.append({
                "name": artist.get("name"),
                "popularity": artist.get("popularity"),
                "followers": (artist.get("followers") or {}).get("total"),
                "genres": ", ".join(artist.get("genres", [])),
                "id": artist.get("id"),
                "link": (artist.get("external_urls") or {}).get("spotify")
            })

        if results["artists"].get("next"):
            current_offset += limit
            next_offset = current_offset
        else:
            next_offset = None
            break
    return artists, next_offset


def init_session_state() -> None:
    if "results" not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if "next_offset" not in st.session_state:
        st.session_state.next_offset = 0
    if "query_meta" not in st.session_state:
        st.session_state.query_meta = {}


def main() -> None:
    st.set_page_config(page_title="Explorador Spotify", layout="wide")
    st.title("Explorador de Artistas do Spotify")
    st.caption("Ajuste os filtros, pesquise artistas e leve os dados para gráficos interativos.")
    init_session_state()

    with st.sidebar:
        st.header("Parâmetros da busca")
        query = st.text_input("Query (Spotify Search API)", value="genre:pop")
        limit = st.number_input("Resultados por página", min_value=1, max_value=50,
                                value=20, step=5)
        pages = st.number_input("Páginas por requisição", min_value=1, max_value=10,
                                value=1)
        min_popularity = st.slider("Popularidade mínima", min_value=0, max_value=100,
                                   value=40)
        offset = st.number_input("Offset inicial", min_value=0, step=limit,
                                 value=int(st.session_state.next_offset or 0))
        accumulate = st.checkbox("Acumular resultados anteriores", value=True)
        st.markdown("Use <https://developer.spotify.com/documentation/web-api/reference/search> "
                    "para montar queries (ex: `genre:rock year:2020`).")

    if st.button("Buscar artistas", type="primary"):
        sp_client = get_spotify_client()
        with st.spinner("Consultando Spotify..."):
            try:
                artists, next_offset = search_artists(
                    sp_client=sp_client,
                    query=query,
                    limit=int(limit),
                    pages=int(pages),
                    min_popularity=int(min_popularity),
                    offset=int(offset)
                )
            except spotipy.SpotifyException as err:
                st.error(f"Erro ao consultar a API: {err}")
                artists, next_offset = [], None

        if not artists:
            st.warning("Nenhum artista retornado para esses parâmetros.")
        else:
            df = pd.DataFrame(artists)
            if accumulate and not st.session_state.results.empty:
                df = pd.concat([st.session_state.results, df], ignore_index=True)
                df = df.drop_duplicates(subset="id")
            st.session_state.results = df
            st.session_state.next_offset = next_offset or 0
            st.session_state.query_meta = {
                "query": query,
                "limit": limit,
                "pages": pages,
                "min_popularity": min_popularity
            }

    if not st.session_state.results.empty:
        df = st.session_state.results
        st.subheader("Resultados da busca")
        st.write(f"{len(df)} artistas carregados.")
        st.dataframe(df[["name", "popularity", "followers", "genres", "link"]])

        col1, col2 = st.columns(2)
        with col1:
            metric = st.selectbox("Métrica para gráfico", options=["popularity", "followers"])
        with col2:
            top_n = st.slider("Top N para visualizar", min_value=5, max_value=50, value=15)

        plot_df = (df.sort_values(metric, ascending=False)
                     .head(top_n)
                     .set_index("name")[metric])
        st.bar_chart(plot_df, use_container_width=True)

        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar CSV",
            csv_data,
            file_name="spotify_artists.csv",
            mime="text/csv"
        )
    else:
        st.info("Nenhum resultado carregado ainda. Faça uma busca para visualizar dados.")

    if st.session_state.next_offset:
        st.success(f"Próximo offset sugerido: {st.session_state.next_offset}. "
                   "Defina-o na barra lateral para continuar de onde parou.")
    else:
        st.caption("Execute novas buscas com outros filtros ou gêneros.")


if __name__ == "__main__":
    main()
