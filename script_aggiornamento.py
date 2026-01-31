import os
import requests
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from supabase import create_client, Client

# Configurazione API Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Configurazione Geocoder gratuito
geolocator = Nominatim(user_agent="roadwork_app_v1")

def è_un_doppione(nuova_lat, nuova_lon, lavori_esistenti, soglia_metri=50):
    for es in lavori_esistenti:
        # Controllo sicurezza se le coordinate sono nulle nel DB
        if es.get('latitudine') and es.get('longitudine'):
            distanza = geodesic((nuova_lat, nuova_lon), (es['latitudine'], es['longitudine'])).meters
            if distanza < soglia_metri:
                return True
    return False

def ottieni_coordinate(indirizzo):
    try:
        location = geolocator.geocode(indirizzo + ", Italy")
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Errore geocoding per {indirizzo}: {e}")
    return None, None

def fetch_dati_finti_esemplificativi():
    return [
        {"desc": "Lavori A1 Milano-Napoli", "pos": "A1, Italia", "lat": 45.4642, "lon": 9.1900, "inizio": "2026-01-25", "fonte": "Autostrade"},
        {"desc": "Cantiere SS1 Aurelia", "pos": "SS1 Aurelia, Italia", "lat": 41.8902, "lon": 12.4922, "inizio": "2025-06-15", "fonte": "Anas"},
        {"desc": "Rifacimento Ponte Storico", "pos": "Firenze, Italia", "lat": 43.7696, "lon": 11.2558, "inizio": "2024-12-01", "fonte": "Comune"},
        {"desc": "Lavori Binari Tram", "pos": "Torino, Italia", "lat": 45.0703, "lon": 7.6869, "inizio": "2023-01-01", "fonte": "OsservaCantieri"}
    ]

def aggiorna_database():
    print(f"--- Avvio aggiornamento del {datetime.now()} ---")
    
    # 1. Recupero lavori esistenti
    try:
        res = supabase.table("lavori").select("latitudine, longitudine").execute()
        lavori_esistenti = res.data
    except Exception as e:
        print(f"Errore recupero dati: {e}")
        lavori_esistenti = []

    nuovi_lavori = fetch_dati_finti_esemplificativi()
    
    for l in nuovi_lavori:
        lat, lon = l['lat'], l['lon']
        if not lat or not lon:
            lat, lon = ottieni_coordinate(l['pos'])
            
        if lat and lon:
            # 2. Controllo distanza 50m
            if è_un_doppione(lat, lon, lavori_esistenti, soglia_metri=50):
                print(f"⚠️ Salto doppione: {l['desc']}")
                continue

            try:
                # 3. INSERIMENTO PURO (Niente upsert, niente on_conflict)
                # NOTA: Assicurati che i nomi delle colonne ("latitudine", ecc.) 
                # siano identici a quelli su Supabase
                supabase.table("lavori").insert({
                    "latitudine": lat,
                    "longitudine": lon,
                    "data_inizio": l["inizio"],
                    "fonte": l["fonte"]
                }).execute()
                
                lavori_esistenti.append({"latitudine": lat, "longitudine": lon})
                print(f"✅ Inserito: {l['desc']}")
                
            except Exception as e:
                # Se vedi ancora l'errore 42P10 qui, significa che GitHub sta eseguendo il vecchio file
                print(f"❌ Errore critico per {l['desc']}: {e}")
        else:
            print(f"⚠️ Salto {l['desc']}: No coordinate.")

if __name__ == "__main__":
    aggiorna_database()
