#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PROJETO INTEGRADOR - FATEC JUNDIAÍ
Script de Coleta de Dados do Spotify (Backend)

Objetivo:
Esse script baixa as músicas mais populares (HITS) do Brasil ano a ano.
Ele usa duas estratégias pra garantir que os dados sejam bons:
1. Pega músicas de Playlists Oficiais (Top 50, Viral, etc).
2. Se não der a meta, faz uma busca profunda por gêneros (Sertanejo, Funk, etc).

Autores: Breno - Karina - Ivan - Gabrielle - Heloíza - João.
"""

import os
import time
import base64
import argparse
import requests
import logging
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from typing import List, Dict

# ==========================================
# 1. AUTENTICAÇÃO
# ==========================================
def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Bate na API do Spotify pra pegar o token de acesso.
    Sem isso a gente não consegue fazer nenhuma busca.
    """
    # Codifica as chaves em base64 conforme a doc do Spotify pede
    auth_str = f"{client_id}:{client_secret}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()
    
    url = "https://accounts.spotify.com/api/token"
    headers = {"Authorization": f"Basic {auth_b64}"}
    data = {"grant_type": "client_credentials"}
    
    response = requests.post(url, headers=headers, data=data, timeout=60)
    response.raise_for_status() # Avisa se der erro (tipo 400 ou 500)
    
    return response.json()["access_token"]

# ==========================================
# 2. FUNÇÕES AUXILIARES (HELPERS)
# ==========================================
def _request_with_retry(url: str, headers=None, params=None, max_retries: int = 5):
    """
    Essa função é importante pra não perder dados.
    Se a API der erro 429 (Rate Limit/Bloqueio temporário), ela espera e tenta de novo.
    """
    for tentativa in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            
            # Se o Spotify mandar parar (429), a gente dorme o tempo que ele pedir
            if r.status_code == 429:
                tempo_espera = int(r.headers.get("Retry-After", "5"))
                time.sleep(tempo_espera)
                continue
            
            # Se for erro de servidor (5xx), espera um pouco e tenta
            if 500 <= r.status_code < 600:
                time.sleep(1)
                continue
                
            r.raise_for_status()
            return r
        except Exception:
            # Se falhar na última tentativa, retorna vazio
            if tentativa == max_retries - 1: return None
            time.sleep(1)
    return None

def _chunked(lista, tamanho: int):
    """
    Divide uma lista gigante em pedacinhos menores (chunks).
    Usamos isso pra pedir dados de artistas de 50 em 50.
    """
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

# ==========================================
# 3. ESTRATÉGIA 1: PLAYLISTS OFICIAIS
# ==========================================
def get_top_brazilian_playlists(tok: str, year: int) -> List[str]:
    """Busca as IDs das playlists mais bombadas do ano."""
    headers = {"Authorization": f"Bearer {tok}"}
    playlist_ids = []
    
    # Termos de busca pra achar as listas oficiais
    buscas = [f"Top Brasil {year}", f"Hits {year}", "Viral Brasil", "Top 50 Brasil", "Pop Brasil", "Esquenta Sertanejo"]
    
    for query in buscas:
        try:
            # Busca playlists
            r = _request_with_retry(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params={"q": query, "type": "playlist", "market": "BR", "limit": 3}
            )
            if r:
                items = r.json().get("playlists", {}).get("items", [])
                for item in items:
                    if item: playlist_ids.append(item["id"])
        except: continue
        
    # Retorna lista sem repetidos (set)
    return list(set(playlist_ids))

def get_tracks_from_playlist(tok: str, pid: str, year: int, min_pop: int) -> List[Dict]:
    """Entra na playlist e pega as músicas, filtrando ano e popularidade."""
    headers = {"Authorization": f"Bearer {tok}"}
    tracks = []
    try:
        r = _request_with_retry(
            f"https://api.spotify.com/v1/playlists/{pid}/tracks",
            headers=headers,
            params={"market": "BR", "limit": 100} # Pega até 100 músicas
        )
        if r:
            for item in r.json().get("items", []):
                t = item.get("track")
                if not t or not t.get("id"): continue
                
                # Validação de Ano: Aceita o ano exato ou o anterior (hits as vezes viram o ano)
                data_lancamento = t.get("album", {}).get("release_date", "")
                if not data_lancamento.startswith(str(year)) and not data_lancamento.startswith(str(year-1)):
                    continue
                
                # Filtro de qualidade (popularidade mínima)
                if t.get("popularity", 0) < min_pop: continue
                
                # Guarda os dados limpos
                tracks.append({
                    "track_id": t["id"],
                    "track_name": t["name"],
                    "track_popularity": t["popularity"],
                    "duration_ms": t["duration_ms"],
                    "explicit": t["explicit"],
                    "album_name": t["album"]["name"],
                    "release_date": data_lancamento,
                    "artists": ", ".join([a["name"] for a in t["artists"]]),
                    "artist_ids": ",".join([a["id"] for a in t["artists"]]),
                    "spotify_url": t["external_urls"]["spotify"]
                })
    except: pass
    return tracks

# ==========================================
# 4. ESTRATÉGIA 2: BUSCA POR GÊNERO (OFFSET)
# ==========================================
def search_deep(tok: str, year: int, min_pop: int, limit_needed: int) -> List[Dict]:
    """
    Se as playlists não derem conta, essa função cava fundo buscando por gênero.
    Usa paginação (offset) pra não pegar sempre as mesmas 50 músicas.
    """
    headers = {"Authorization": f"Bearer {tok}"}
    tracks = []
    ids_vistos = set()
    
    # Lista de gêneros fortes no Brasil
    generos = ["sertanejo", "funk", "pop", "pagode", "trap", "rap", "forro", "rock", "mpb", "samba"]
    
    pbar = tqdm(total=limit_needed, desc=f"🔍 Buscando hits extras de {year}", leave=False)
    
    for genero in generos:
        if len(tracks) >= limit_needed: break
        
        # Loop de Paginação: 0, 50, 100, 150... pra pegar músicas diferentes
        for offset in [0, 50, 100, 150]:
            if len(tracks) >= limit_needed: break
            
            query = f"year:{year} genre:{genero}"
            try:
                r = _request_with_retry(
                    "https://api.spotify.com/v1/search",
                    headers=headers,
                    params={
                        "q": query, "type": "track", "market": "BR", 
                        "limit": 50, "offset": offset
                    }
                )
                if not r: break
                
                items = r.json().get("tracks", {}).get("items", [])
                if not items: break # Acabou esse gênero
                
                for t in items:
                    if len(tracks) >= limit_needed: break
                    if not t: continue
                    
                    tid = t["id"]
                    if tid in ids_vistos: continue # Evita duplicatas
                    if t["popularity"] < min_pop: continue # Evita música desconhecida
                    
                    # Confere o ano
                    rel = t["album"]["release_date"]
                    if not rel.startswith(str(year)): continue

                    ids_vistos.add(tid)
                    tracks.append({
                        "track_id": tid,
                        "track_name": t["name"],
                        "track_popularity": t["popularity"],
                        "duration_ms": t["duration_ms"],
                        "explicit": t["explicit"],
                        "album_name": t["album"]["name"],
                        "release_date": rel,
                        "artists": ", ".join([a["name"] for a in t["artists"]]),
                        "artist_ids": ",".join([a["id"] for a in t["artists"]]),
                        "spotify_url": t["external_urls"]["spotify"]
                    })
                    pbar.update(1)
            except: break
            
    pbar.close()
    return tracks

# ==========================================
# 5. ENRIQUECIMENTO DE DADOS (ARTISTAS)
# ==========================================
def enrich(df: pd.DataFrame, tok: str) -> pd.DataFrame:
    """
    Pega o DataFrame com as músicas e adiciona dados dos artistas
    (Gênero, Seguidores, Popularidade do cantor).
    """
    if df.empty: return df
    
    # Cria lista de todos os IDs de artistas únicos
    ids = set()
    for x in df["artist_ids"]: 
        if x: ids.update(x.split(","))
    
    meta = {}
    headers = {"Authorization": f"Bearer {tok}"}
    
    # Busca em lotes de 50 pra ser rápido
    unique_list = list(ids)
    for batch in _chunked(unique_list, 50):
        try:
            r = _request_with_retry(
                "https://api.spotify.com/v1/artists",
                headers=headers, 
                params={"ids": ",".join(batch)}
            )
            if r:
                for a in r.json().get("artists", []):
                    if a: meta[a["id"]] = a
        except: continue
        
    # Listas pra preencher as novas colunas
    prim_ids, genres, pops, followers = [], [], [], []
    
    for x in df["artist_ids"]:
        # Pega só o primeiro artista (Principal) pra facilitar a análise
        pid = x.split(",")[0] if x else None
        m = meta.get(pid, {})
        
        prim_ids.append(pid) # ID do artista principal
        genres.append(m.get("genres", []))
        pops.append(m.get("popularity", 0))
        followers.append(m.get("followers", {}).get("total", 0))
        
    # Salva no DataFrame
    df["primary_artist_id"] = prim_ids 
    df["primary_artist_genres"] = genres
    df["primary_artist_popularity"] = pops
    df["primary_artist_followers"] = followers
    return df

# ==========================================
# 6. FLUXO PRINCIPAL (MAIN)
# ==========================================
def process_year(tok, year, min_pop, use_playlists, max_tracks):
    """Gerencia a coleta de um ano específico."""
    all_tracks = []
    seen = set()
    
    # Passo 1: Tenta Playlists (Melhor qualidade)
    if use_playlists:
        pids = get_top_brazilian_playlists(tok, year)
        for pid in tqdm(pids, desc=f"Varrendo Playlists {year}", leave=False):
            if len(all_tracks) >= max_tracks: break
            res = get_tracks_from_playlist(tok, pid, year, min_pop)
            for t in res:
                if t["track_id"] not in seen:
                    seen.add(t["track_id"])
                    all_tracks.append(t)

    # Passo 2: Se faltou música, busca por gênero
    faltam = max_tracks - len(all_tracks)
    if faltam > 0:
        res = search_deep(tok, year, min_pop, faltam + 20) # Pede um pouco a mais de margem
        for t in res:
            if len(all_tracks) >= max_tracks: break
            if t["track_id"] not in seen:
                seen.add(t["track_id"])
                all_tracks.append(t)
                
    if not all_tracks: return pd.DataFrame()
    
    # Cria DF e corta o excesso
    df = pd.DataFrame(all_tracks).head(max_tracks)
    df["year"] = year # Importante pra o gráfico de linha do tempo!
    
    print(f"👤 Enriquecendo {len(df)} faixas com dados dos artistas...")
    return enrich(df, tok)

def main():
    # Carrega variáveis de ambiente (.env)
    load_dotenv()
    if not os.getenv("SPOTIFY_CLIENT_ID"): 
        print("Erro: Faltou configurar o arquivo .env")
        return
    
    # Configura os argumentos da linha de comando
    parser = argparse.ArgumentParser(description="Coletor Spotify - Trabalho Fatec")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Anos pra baixar")
    parser.add_argument("--min-popularity", type=int, default=20, help="Filtro de qualidade")
    parser.add_argument("--outfile-prefix", default="dados_brasil", help="Nome do arquivo")
    parser.add_argument("--max-tracks", type=int, default=600, help="Meta de músicas por ano")
    parser.add_argument("--no-playlists", action="store_true", help="Pular busca de playlists")
    args = parser.parse_args()
    
    # Login
    tok = get_access_token(os.getenv("SPOTIFY_CLIENT_ID"), os.getenv("SPOTIFY_CLIENT_SECRET"))
    os.makedirs("raw_data", exist_ok=True)
    
    # Loop pelos anos pedidos
    for y in args.years:
        print(f"\n📅 Processando Ano: {y}...")
        try:
            df = process_year(tok, y, args.min_popularity, not args.no_playlists, args.max_tracks)
            if not df.empty:
                f = f"raw_data/{args.outfile_prefix}_{y}.csv"
                df.to_csv(f, index=False)
                print(f"✅ Sucesso! {len(df)} faixas salvas em {f}")
            else:
                print("⚠️ Nenhuma música encontrada.")
        except Exception as e:
            print(f"❌ Deu ruim no ano {y}: {e}")

if __name__ == "__main__":
    main()