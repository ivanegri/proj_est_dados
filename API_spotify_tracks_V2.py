#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor "quase-completo" do catálogo por ano no Spotify, com enriquecimento.

Funcionalidades:
- Busca álbuns por ano com sharding (para contornar o limite de 1000 offsets do search).
- Abre /v1/albums/{id}/tracks para coletar TODAS as faixas dos álbuns encontrados.
- Enriquecimento opcional:
  - /v1/artists: followers, genres, popularity (do artista principal).
  - /v1/tracks: popularity (da faixa).
- Trata rate-limit (429) usando Retry-After.
- CLI: escolha anos, mercados, shards simples (0-9 + a-z) ou bigramas (aa..zz), etc.

Requisitos:
  pip install requests pandas python-dotenv

Variáveis de ambiente:
  SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
"""

import os
import time
import string
import base64
import argparse
import requests
import logging
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv



# =================== AUTH ===================
def get_access_token(client_id: str, client_secret: str) -> str:
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        headers={"Authorization": f"Basic {auth}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# =================== HELPERS ===================
def _request_with_retry(
    url: str,
    headers=None,
    params=None,
    method: str = "GET",
    max_retries: int = 5,
    timeout: int = 60,
):
    """Request robusto que lida com 429 (Retry-After) e 5xx (backoff)."""
    for attempt in range(max_retries):
        r = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        # Rate limit
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(wait)
            continue
        # 5xx: tenta novamente com pequeno backoff
        if 500 <= r.status_code < 600:
            time.sleep(1.0 + attempt)
            continue
        r.raise_for_status()
        return r
    # última tentativa
    r.raise_for_status()
    return r


def _chunked(seq, n: int):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf


def build_shards(mode: str = "letters"):
    """
    mode:
      - 'letters'  => [0..9, a..z] (36 shards)
      - 'bigrams'  => [aa..zz] + [0..9] (676 + 10 = 686 shards)
    """
    digits = [str(i) for i in range(10)]
    letters = list(string.ascii_lowercase)

    if mode == "letters":
        return digits + letters
    elif mode == "bigrams":
        bigs = [a + b for a in letters for b in letters]
        return digits + bigs
    else:
        raise ValueError("Modo de shards inválido. Use 'letters' ou 'bigrams'.")


# =================== ARTISTS ENRICHMENT ===================
def fetch_artists_meta(tok: str, artist_ids):
    """
    /v1/artists em lotes de até 50 → dict:
      id -> {"followers": int, "genres": [str], "name": str, "popularity": int}
    """
    headers = {"Authorization": f"Bearer {tok}"}
    meta = {}
    unique = list({x for x in artist_ids if x})
    for batch in _chunked(unique, 50):
        r = _request_with_retry(
            "https://api.spotify.com/v1/artists",
            headers=headers,
            params={"ids": ",".join(batch)},
        )
        data = r.json() or {}
        for a in (data.get("artists") or []):
            if not a:
                continue
            meta[a["id"]] = {
                "followers": (a.get("followers") or {}).get("total"),
                "genres": a.get("genres") or [],
                "name": a.get("name"),
                "popularity": a.get("popularity"),
            }
    return meta


# =================== TRACKS ENRICHMENT ===================
def fetch_tracks_popularity(tok: str, track_ids):
    """/v1/tracks em lotes de até 50 → dict: track_id -> popularity (int)"""
    headers = {"Authorization": f"Bearer {tok}"}
    pop = {}
    unique = list({tid for tid in track_ids if tid})
    for batch in _chunked(unique, 50):
        r = _request_with_retry(
            "https://api.spotify.com/v1/tracks",
            headers=headers,
            params={"ids": ",".join(batch)},
        )
        data = r.json() or {}
        for tr in (data.get("tracks") or []):
            if tr and tr.get("id") is not None:
                pop[tr["id"]] = tr.get("popularity")
    return pop


# =================== CORE: BUSCAR ÁLBUNS (POR SHARDS) ===================
def search_albums_by_year(
    tok: str,
    year: int,
    market: str = "BR",
    limit: int = 50,
    shards=None,
    max_pages_per_shard=None,
):
    """
    Usa /v1/search?type=album com 'sharding' em artist:<shard> para aumentar recall.
    Retorna dict album_id -> payload mínimo.
    """
    if shards is None:
        shards = build_shards("letters")

    headers = {"Authorization": f"Bearer {tok}"}
    albums_index = {}

    shard_iterator = tqdm(
        shards, desc=f"Searching albums for {year} in {market}", unit="shard"
    )

    for shard in shard_iterator:
        next_url = "https://api.spotify.com/v1/search"
        params = {
            "q": f"year:{year} artist:{shard}",
            "type": "album",
            "market": market,
            "limit": limit,
            "offset": 0,
        }
        pages = 0
        while next_url:
            r = _request_with_retry(next_url, headers=headers, params=params)
            data = r.json() or {}
            block = data.get("albums")
            if not block:
                break

            for alb in block.get("items", []):
                rel = (alb.get("release_date") or "")
                # Garante que é o ano desejado
                if not rel.startswith(str(year)):
                    continue
                album_id = alb.get("id")
                if not album_id:
                    continue
                if album_id in albums_index:
                    continue
                albums_index[album_id] = {
                    "album_id": album_id,
                    "album_name": alb.get("name"),
                    "album_type": alb.get("album_type"),
                    "release_date": rel,
                    "artists": alb.get("artists") or [],
                }

            next_url = block.get("next")
            params = None  # IMPORTANTÍSSIMO ao seguir 'next'
            pages += 1
            if max_pages_per_shard and pages >= max_pages_per_shard:
                break

        shard_iterator.set_postfix(found=len(albums_index))

    return albums_index


# =================== PEGAR FAIXAS DE CADA ÁLBUM ===================
def fetch_album_tracks(tok: str, album_id: str, market: str = "BR"):
    """/v1/albums/{id}/tracks com paginação (se necessário)."""
    headers = {"Authorization": f"Bearer {tok}"}
    base = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    next_url = base
    params = {"limit": 50, "market": market}
    tracks = []
    while next_url:
        r = _request_with_retry(next_url, headers=headers, params=params)
        data = r.json() or {}
        items = data.get("items") or []
        tracks.extend(items)
        next_url = data.get("next")
        params = None
    return tracks


# =================== ORQUESTRAÇÃO: ANO → ÁLBUNS → FAIXAS (+ ENRICH) ===================
def collect_year_tracks(
    tok: str,
    year: int,
    market: str = "BR",
    shards=None,
    max_pages_per_shard=None,
    enrich_artists: bool = True,
    enrich_track_popularity: bool = True,
):
    """
    Retorna DataFrame com faixas únicas lançadas no ano, com colunas básicas + enriquecimentos.
    """
    albums_index = search_albums_by_year(
        tok, year, market=market, shards=shards, max_pages_per_shard=max_pages_per_shard
    )
    logging.info(f"[{year} | {market}] Found {len(albums_index)} unique albums.")

    rows = []
    all_artist_ids = set()

    album_iterator = tqdm(albums_index.items(), desc=f"Fetching tracks for {year} in {market}", unit="album")
    for album_id, alb in album_iterator:
        trks = fetch_album_tracks(tok, album_id, market=market)
        for t in trks:
            artist_objs = t.get("artists") or []
            artist_names = [a.get("name") for a in artist_objs if a]
            artist_ids = [a.get("id") for a in artist_objs if a]

            rows.append({
                "year": year,
                "market": market,
                "album_id": album_id,
                "album_name": alb.get("album_name"),
                "album_type": alb.get("album_type"),
                "release_date": alb.get("release_date"),
                "track_id": t.get("id"),
                "track_name": t.get("name"),
                "track_number": t.get("track_number"),
                "disc_number": t.get("disc_number"),
                "duration_ms": t.get("duration_ms"),
                "explicit": t.get("explicit"),
                "artists": ", ".join([n for n in artist_names if n]),
                "artist_ids": ",".join([aid for aid in artist_ids if aid]),
                "spotify_url": (t.get("external_urls") or {}).get("spotify"),
            })
            all_artist_ids.update([aid for aid in artist_ids if aid])

    df = pd.DataFrame(rows).drop_duplicates(subset=["track_id"]).reset_index(drop=True)

    # === ENRICH: ARTISTS ===
    if enrich_artists and not df.empty:
        meta = fetch_artists_meta(tok, list(all_artist_ids))
        prim_ids, prim_followers, prim_genres, prim_pop = [], [], [], []
        for ids_csv in df["artist_ids"].fillna(""):
            parts = [x.strip() for x in ids_csv.split(",") if x.strip()]
            p = parts[0] if parts else None
            prim_ids.append(p)
            m = meta.get(p, {}) if p else {}
            prim_followers.append(m.get("followers"))
            prim_genres.append(m.get("genres") or [])
            prim_pop.append(m.get("popularity"))

        df["primary_artist_id"] = prim_ids
        df["primary_artist_followers"] = pd.to_numeric(prim_followers, errors="coerce")
        df["primary_artist_genres"] = prim_genres
        df["primary_artist_popularity"] = pd.to_numeric(prim_pop, errors="coerce")

    # === ENRICH: TRACK POPULARITY ===
    if enrich_track_popularity and not df.empty:
        pop_map = fetch_tracks_popularity(tok, df["track_id"].tolist())
        df["track_popularity"] = df["track_id"].map(pop_map)

    # tipos úteis
    if not df.empty:
        df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
        df["explicit"] = df["explicit"].fillna(False)

    return df


# =================== CLI ===================
def parse_args():
    ap = argparse.ArgumentParser(description="Coletor de catálogo Spotify por ano, com enriquecimento.")
    ap.add_argument("--years", nargs="+", type=int, required=True, help="Anos-alvo (ex.: 2023 2024).")
    ap.add_argument("--markets", nargs="+", default=["BR"], help="Mercados (ex.: BR US GB).")
    ap.add_argument("--shards", choices=["letters", "bigrams"], default="letters",
                    help="Estratégia de shards: 'letters' (0-9 + a-z) ou 'bigrams' (aa..zz + 0-9).")
    ap.add_argument("--limit", type=int, default=50, help="limit do search (recomendado 50).")
    ap.add_argument("--max-pages-per-shard", type=int, default=None, help="Para debug; usualmente deixe None.")
    ap.add_argument("--no-enrich-artists", action="store_true", help="Não enriquecer com /v1/artists.")
    ap.add_argument("--no-enrich-track-pop", action="store_true", help="Não enriquecer com /v1/tracks.")
    ap.add_argument("--outfile-prefix", default="tracks_catalog", help="Prefixo do CSV gerado.")
    ap.add_argument("--verbose", action="store_true", help="Logs detalhados.")
    return ap.parse_args()


def main():
    load_dotenv()
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Defina SPOTIFY_CLIENT_ID e SPOTIFY_CLIENT_SECRET no ambiente (.env).")

    args = parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    tok = get_access_token(client_id, client_secret)

    shards = build_shards(args.shards)
    for year in tqdm(args.years, desc="Processing years", unit="year"):
        dfs = []
        for market in tqdm(args.markets, desc=f"Processing markets for {year}", unit="market", leave=False):
            df_market = collect_year_tracks(
                tok,
                year,
                market=market,
                shards=shards,
                max_pages_per_shard=args.max_pages_per_shard,
                enrich_artists=(not args.no_enrich_artists),
                enrich_track_popularity=(not args.no_enrich_track_pop),
            )
            dfs.append(df_market)

        # Merge de todos os markets do ano + dedup por track_id
        if dfs:
            df_year = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["track_id"]).reset_index(drop=True)
        else:
            df_year = pd.DataFrame()

        outdir = "raw_data"
        os.makedirs(outdir, exist_ok=True)
        outpath = os.path.join(outdir, f"{args.outfile_prefix}_{year}.csv")
        df_year.to_csv(outpath, index=False)
        logging.info(f"[{year}] Unique tracks: {len(df_year)} | CSV saved to: {outpath}")


if __name__ == "__main__":
    main()
