import os
import requests
from datetime import datetime
from geopy.geocoders import Nominatim
from supabase import create_client, Client

# Configurazione API Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Configurazione Geocoder gratuito (OpenStreetMap)
geolocator = Nominatim(user_agent="roadwork_app_v1")

def ottieni_coordinate(indirizzo):
    """Converte un indirizzo o nome strada in coordinate lat/lon"""
    try:
        location = geolocator.geocode(indirizzo + ", Italy")
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Errore geocoding per {indirizzo}: {e}")
    return None, None

def fetch_dati_finti_esemplificativi():
    """
    Simula i dati dalle fonti (Anas, Autostrade, CCISS).
    In una versione avanzata, qui aggiungerai le chiamate API reali.
    """
    return [
        {"desc": "Lavori A1 Milano-Napoli", "pos": "A1, Italia", "lat": 45.4642, "lon": 9.1900, "inizio": "2026-01-25", "fonte": "Autostrade"},
        {"desc": "Cantiere SS1 Aurelia", "pos": "SS1 Aurelia, Italia", "lat": 41.8902, "lon": 12.4922, "inizio": "2025-06-15", "fonte": "Anas"},
        {"desc": "Rifacimento Ponte Storico", "pos": "Firenze, Italia", "lat": 43.7696, "lon": 11.2558, "inizio": "2024-12-01", "fonte": "Comune"},
        {"desc": "Lavori Binari Tram", "pos": "Torino, Italia", "lat": 45.0703, "lon": 7.6869, "inizio": "2023-01-01", "fonte": "OsservaCantieri"}
    ]

def aggiorna_database():
    print(f"--- Avvio aggiornamento del {datetime.now()} ---")
    lavori = fetch_dati_finti_esemplificativi()
    
    for l in lavori:
        lat, lon = l['lat'], l['lon']
        
        # Se non abbiamo le coordinate, proviamo a ricavarle dal nome posizione
        if not lat or not lon:
            lat, lon = ottieni_coordinate(l['pos'])
            
        if lat and lon:
            try:
                # Inserimento o aggiornamento (upsert) basato sulla descrizione
                supabase.table("lavori").upsert({
                    "descrizione": l["desc"],
                    "latitudine": lat,
                    "longitudine": lon,
                    "data_inizio": l["inizio"],
                    "fonte": l["fonte"]
                }, on_conflict="descrizione").execute()
                print(f"✅ OK: {l['desc']} ({l['fonte']})")
            except Exception as e:
                print(f"❌ Errore Supabase per {l['desc']}: {e}")
        else:
            print(f"⚠️ Salto {l['desc']}: Coordinate non trovate.")

if __name__ == "__main__":
    aggiorna_database()
