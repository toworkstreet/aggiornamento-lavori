import os
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from supabase import create_client, Client

# Configurazione API Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

geolocator = Nominatim(user_agent="roadwork_app_v1")

def è_un_doppione(nuova_lat, nuova_lon, lavori_esistenti, soglia_metri=50):
    for es in lavori_esistenti:
        if es.get('latitudine') and es.get('longitudine'):
            distanza = geodesic((nuova_lat, nuova_lon), (es['latitudine'], es['longitudine'])).meters
            if distanza < soglia_metri:
                return True
    return False

# --- FONTE 1: OPENSTREETMAP (Overpass API) ---
def fetch_osm_lavori():
    print("🛰️ Ricerca cantieri su OpenStreetMap...")
    query = """
    [out:json][timeout:25];
    area["ISO3166-1"="IT"]->.italy;
    (
      node["highway"="construction"](area.italy);
      way["highway"="construction"](area.italy);
    );
    out center;
    """
    url_osm = "https://overpass-api.de/api/interpreter"
    try:
        response = requests.post(url_osm, data={'data': query})
        data = response.json()
        risultati = []
        for element in data.get('elements', []):
            lat = element.get('lat') or element.get('center', {}).get('lat')
            lon = element.get('lon') or element.get('center', {}).get('lon')
            if lat and lon:
                risultati.append({
                    "lat": lat, "lon": lon,
                    "inizio": datetime.now().strftime("%Y-%m-%d"),
                    "fonte": "OpenStreetMap",
                    "desc": "Cantiere stradale (OSM)"
                })
        return risultati
    except Exception as e:
        print(f"❌ Errore OSM: {e}")
        return []

# --- FONTE 2: RSS FEEDS (Comuni/Enti) ---
def fetch_rss_lavori(rss_url, nome_fonte):
    print(f"📰 Lettura Feed RSS: {nome_fonte}...")
    try:
        response = requests.get(rss_url, timeout=10)
        root = ET.fromstring(response.content)
        risultati = []
        for item in root.findall('.//item'):
            title = item.find('title').text
            # Molti feed mettono le coordinate nei tag geo:lat o simili
            # In questo esempio usiamo il geocoding sul titolo/descrizione
            risultati.append({
                "lat": None, "lon": None, # Verranno cercate dopo col Geocoder
                "pos": title,
                "inizio": datetime.now().strftime("%Y-%m-%d"),
                "fonte": nome_fonte,
                "desc": title
            })
        return risultati
    except Exception as e:
        print(f"❌ Errore RSS {nome_fonte}: {e}")
        return []

def aggiorna_database():
    print(f"🚀 Avvio scansione nazionale del {datetime.now()}...")
    
    # Recupero dati esistenti per evitare doppioni
    try:
        res = supabase.table("lavori").select("latitudine, longitudine").execute()
        lavori_esistenti = res.data
    except:
        lavori_esistenti = []

    # Aggreghiamo tutte le fonti
    lista_totale = []
    lista_totale.extend(fetch_osm_lavori())
    
    # Esempio Feed RSS (puoi aggiungerne altri qui)
    feed_comuni = [
        {"url": "https://www.comune.milano.it/infopubb/server/rss/notizie.xml", "nome": "Comune Milano"},
        {"url": "https://foia.comune.roma.it/notizie/rss", "nome": "Comune Roma"}
    ]
    for f in feed_comuni:
        lista_totale.extend(fetch_rss_lavori(f['url'], f['nome']))

    for l in lista_totale:
        lat, lon = l.get('lat'), l.get('lon')
        
        # Se mancano coordinate (tipico degli RSS), le cerchiamo
        if not lat or not lon:
            try:
                location = geolocator.geocode(l['pos'] + ", Italia")
                if location:
                    lat, lon = location.latitude, location.longitude
                else:
                    continue
            except:
                continue
            
        if lat and lon:
            if è_un_doppione(lat, lon, lavori_esistenti, soglia_metri=50):
                continue

            try:
                supabase.table("lavori").insert({
                    "latitudine": lat,
                    "longitudine": lon,
                    "data_inizio": l["inizio"],
                    "fonte": l["fonte"]
                }).execute()
                lavori_esistenti.append({"latitudine": lat, "longitudine": lon})
                print(f"✅ Inserito: {l['desc']} [{l['fonte']}]")
            except Exception as e:
                print(f"❌ Errore: {e}")

if __name__ == "__main__":
    aggiorna_database()
