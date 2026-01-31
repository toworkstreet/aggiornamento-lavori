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

# Configurazione Geocoder gratuito (OpenStreetMap)
geolocator = Nominatim(user_agent="roadwork_app_v1")

def è_un_doppione(nuova_lat, nuova_lon, lavori_esistenti, soglia_metri=50):
    """Controlla se esiste già un punto entro la soglia di metri specificata"""
    for es in lavori_esistenti:
        distanza = geodesic((nuova_lat, nuova_lon), (es['latitudine'], es['longitudine'])).meters
        if distanza < soglia_metri:
            return True
    return False

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
    """Simulazione dati da fonti pubbliche"""
    return [
        {"desc": "Lavori A1 Milano-Napoli", "pos": "A1, Italia", "lat": 45.4642, "lon": 9.1900, "inizio": "2026-01-25", "fonte": "Autostrade"},
        {"desc": "Cantiere SS1 Aurelia", "pos": "SS1 Aurelia, Italia", "lat": 41.8902, "lon": 12.4922, "inizio": "2025-06-15", "fonte": "Anas"},
        {"desc": "Rifacimento Ponte Storico", "pos": "Firenze, Italia", "lat": 43.7696, "lon": 11.2558, "inizio": "2024-12-01", "fonte": "Comune"},
        {"desc": "Lavori Binari Tram", "pos": "Torino, Italia", "lat": 45.0703, "lon": 7.6869, "inizio": "2023-01-01", "fonte": "OsservaCantieri"}
    ]

def aggiorna_database():
    print(f"--- Avvio aggiornamento del {datetime.now()} ---")
    
    # 1. Scarichiamo i lavori già presenti nel database per il confronto
    try:
        res = supabase.table("lavori").select("latitudine, longitudine").execute()
        lavori_esistenti = res.data
    except Exception as e:
        print(f"Errore nel recupero dati esistenti: {e}")
        lavori_esistenti = []

    nuovi_lavori = fetch_dati_finti_esemplificativi()
    
    for l in nuovi_lavori:
        lat, lon = l['lat'], l['lon']
        
        if not lat or not lon:
            lat, lon = ottieni_coordinate(l['pos'])
            
        if lat and lon:
            # 2. Controllo doppioni entro 50 metri
            if è_un_doppione(lat, lon, lavori_esistenti, soglia_metri=50):
                print(f"⚠️ Salto doppione geografico (entro 50m): {l['desc']}")
                continue

            try:
                # 3. Inserimento semplice (senza upsert per evitare errori di vincoli)
                supabase.table("lavori").insert({
                    "latitudine": lat,
                    "longitudine": lon,
                    "data_inizio": l["inizio"],
                    "fonte": l["fonte"]
                    # "descrizione" rimosso se non presente nella tua tabella, 
                    # aggiungilo solo se hai creato la colonna specifica
                }).execute()
                
                # Aggiungiamo il nuovo punto alla lista locale per i controlli successivi del ciclo
                lavori_esistenti.append({"latitudine": lat, "longitudine": lon})
                print(f"✅ Inserito: {l['desc']}")
                
            except Exception as e:
                print(f"❌ Errore Supabase per {l['desc']}: {e}")
        else:
            print(f"⚠️ Salto {l['desc']}: Coordinate non trovate.")

if __name__ == "__main__":
    aggiorna_database()
