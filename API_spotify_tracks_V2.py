#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor de HITS do Spotify V6.2 (Final)
Fix: Adicionado coluna 'primary_artist_id' que faltou na versão anterior.
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
from typing import List, Dict, Set

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
def _request_with_retry(url: str, headers=None, params=None, max_retries: int = 5):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))
                time.sleep(wait)
                continue
            if 500 <= r.status_code < 600:
                time.sleep(1)
                continue
            r.raise_for_status()
            return r
        except Exception:
            if attempt == max_retries - 1: return None
            time.sleep(1)
    return None

def _chunked(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]

# =================== ESTRATÉGIA 1: PLAYLISTS ===================
def get_top_brazilian_playlists(tok: str, year: int) -> List[str]:
    headers = {"Authorization": f"Bearer {tok}"}
    pids = []
    queries = [f"Top Brasil {year}", f"Hits {year}", "Viral Brasil", "Top 50 Brasil", "Pop Brasil", "Esquenta Sertanejo"]
    
    for q in queries:
        try:
            r = _request_with_retry(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params={"q": q, "type": "playlist", "market": "BR", "limit": 3}
            )
            if r:
                items = r.json().get("playlists", {}).get("items", [])
                for i in items:
                    if i: pids.append(i["id"])
        except: continue
    return list(set(pids))

def get_tracks_from_playlist(tok: str, pid: str, year: int, min_pop: int) -> List[Dict]:
    headers = {"Authorization": f"Bearer {tok}"}
    tracks = []
    try:
        r = _request_with_retry(
            f"https://api.spotify.com/v1/playlists/{pid}/tracks",
            headers=headers,
            params={"market": "BR", "limit": 100}
        )
        if r:
            for item in r.json().get("items", []):
                t = item.get("track")
                if not t or not t.get("id"): continue
                
                rel = t.get("album", {}).get("release_date", "")
                if not rel.startswith(str(year)) and not rel.startswith(str(year-1)):
                    continue
                
                if t.get("popularity", 0) < min_pop: continue
                
                tracks.append({
                    "track_id": t["id"],
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
    except: pass
    return tracks

# =================== ESTRATÉGIA 2: BUSCA PAGINADA ===================
def search_deep(tok: str, year: int, min_pop: int, limit_needed: int) -> List[Dict]:
    headers = {"Authorization": f"Bearer {tok}"}
    tracks = []
    seen = set()
    genres = ["sertanejo", "funk", "pop", "pagode", "trap", "rap", "forro", "rock", "mpb", "samba"]
    
    pbar = tqdm(total=limit_needed, desc=f"🔍 Cavando hits de {year}", leave=False)
    
    for genre in genres:
        if len(tracks) >= limit_needed: break
        
        for offset in [0, 50, 100, 150]:
            if len(tracks) >= limit_needed: break
            
            query = f"year:{year} genre:{genre}"
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
                if not items: break 
                
                for t in items:
                    if len(tracks) >= limit_needed: break
                    if not t: continue
                    
                    tid = t["id"]
                    if tid in seen: continue
                    if t["popularity"] < min_pop: continue
                    
                    rel = t["album"]["release_date"]
                    if not rel.startswith(str(year)): continue

                    seen.add(tid)
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

# =================== ENRICHMENT (CORRIGIDO) ===================
def enrich(df: pd.DataFrame, tok: str) -> pd.DataFrame:
    if df.empty: return df
    ids = set()
    for x in df["artist_ids"]: 
        if x: ids.update(x.split(","))
    
    meta = {}
    headers = {"Authorization": f"Bearer {tok}"}
    
    unique = list(ids)
    for batch in _chunked(unique, 50):
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
        
    # --- CORREÇÃO AQUI: LISTA DE IDs PRIMÁRIOS ---
    prim_ids, genres, pops, followers = [], [], [], []
    
    for x in df["artist_ids"]:
        pid = x.split(",")[0] if x else None
        m = meta.get(pid, {})
        
        prim_ids.append(pid) # <--- O QUE FALTAVA
        genres.append(m.get("genres", []))
        pops.append(m.get("popularity", 0))
        followers.append(m.get("followers", {}).get("total", 0))
        
    df["primary_artist_id"] = prim_ids # <--- SALVANDO A COLUNA
    df["primary_artist_genres"] = genres
    df["primary_artist_popularity"] = pops
    df["primary_artist_followers"] = followers
    return df

# =================== MAIN ===================
def process_year(tok, year, min_pop, use_playlists, max_tracks):
    all_tracks = []
    seen = set()
    
    if use_playlists:
        pids = get_top_brazilian_playlists(tok, year)
        for pid in tqdm(pids, desc=f"Playlists {year}", leave=False):
            if len(all_tracks) >= max_tracks: break
            res = get_tracks_from_playlist(tok, pid, year, min_pop)
            for t in res:
                if t["track_id"] not in seen:
                    seen.add(t["track_id"])
                    all_tracks.append(t)

    needed = max_tracks - len(all_tracks)
    if needed > 0:
        res = search_deep(tok, year, min_pop, needed + 20)
        for t in res:
            if len(all_tracks) >= max_tracks: break
            if t["track_id"] not in seen:
                seen.add(t["track_id"])
                all_tracks.append(t)
                
    if not all_tracks: return pd.DataFrame()
    
    df = pd.DataFrame(all_tracks).head(max_tracks)
    df["year"] = year 
    
    print(f"👤 Enriquecendo {len(df)} faixas...")
    return enrich(df, tok)

def main():
    load_dotenv()
    if not os.getenv("SPOTIFY_CLIENT_ID"): return
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--min-popularity", type=int, default=20)
    parser.add_argument("--outfile-prefix", default="dados_brasil")
    parser.add_argument("--max-tracks", type=int, default=600)
    parser.add_argument("--no-playlists", action="store_true")
    args = parser.parse_args()
    
    tok = get_access_token(os.getenv("SPOTIFY_CLIENT_ID"), os.getenv("SPOTIFY_CLIENT_SECRET"))
    os.makedirs("raw_data", exist_ok=True)
    
    for y in args.years:
        print(f"\n📅 Ano {y}...")
        try:
            df = process_year(tok, y, args.min_popularity, not args.no_playlists, args.max_tracks)
            if not df.empty:
                f = f"raw_data/{args.outfile_prefix}_{y}.csv"
                df.to_csv(f, index=False)
                print(f"✅ {len(df)} faixas salvas!")
            else:
                print("⚠️ Nada encontrado.")
        except Exception as e:
            print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()