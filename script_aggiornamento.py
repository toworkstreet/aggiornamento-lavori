import os
import requests
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from supabase import create_client, Client
import time

# --- CONFIGURAZIONE ---
print("⚙️ Inizializzazione configurazione...")
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# User agent specifico per evitare blocchi
geolocator = Nominatim(user_agent="roadwork_tracker_italy_v3")

def estrai_costo(testo):
    if not testo: return "N.D."
    match = re.search(r'(\d+[\d\.,]*)\s*(€|Euro|euro|milioni|mln|Mln)', testo)
    if match: return match.group(0)
    return "N.D."

def è_un_doppione(nuova_lat, nuova_lon, lavori_esistenti, soglia_metri=100):
    for es in lavori_esistenti:
        if es.get('latitudine') and es.get('longitudine'):
            distanza = geodesic((nuova_lat, nuova_lon), (es['latitudine'], es['longitudine'])).meters
            if distanza < soglia_metri: return True
    return False

def fetch_osm_lavori():
    # Calcolo della data di 7 giorni fa nel formato richiesto da OSM (ISO 8601)
    data_limite = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    print(f"🛰️ Ricerca cantieri su OSM aggiornati dopo il: {data_limite}")
    
    query = f"""
    [out:json][timeout:60];
    area["ISO3166-1"="IT"]->.italy;
    (
      node["highway"="construction"](area.italy)(newer:"{data_limite}");
      way["highway"="construction"](area.italy)(newer:"{data_limite}");
    );
    out center; 
    """
    try:
        response = requests.post("https://overpass-api.de/api/interpreter", data={'data': query}, timeout=90)
        print(f"📡 Risposta OSM ricevuta (Status: {response.status_code})")
        data = response.json()
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        elements = data.get('elements', [])
        print(f"🔎 Trovati {len(elements)} elementi modificati di recente su OSM")
        
        for element in elements:
            tags = element.get('tags', {})
            lat = element.get('lat') or element.get('center', {}).get('lat')
            lon = element.get('lon') or element.get('center', {}).get('lon')
            desc = tags.get('description', tags.get('note', 'Cantiere stradale (OSM)'))
            if lat and lon:
                risultati.append({
                    "lat": lat, "lon": lon,
                    "inizio": tags.get('start_date'),
                    "ultima_segnalazione": oggi,
                    "fonte": "OpenStreetMap",
                    "desc": desc,
                    "costo": estrai_costo(desc)
                })
        return risultati
    except Exception as e:
        print(f"⚠️ Errore OSM (saltato): {e}")
        return []

def fetch_rss_lavori(rss_url, nome_fonte):
    print(f"📰 Lettura Feed: {nome_fonte}...")
    try:
        response = requests.get(rss_url, timeout=15)
        if response.status_code != 200: 
            print(f"❌ Errore HTTP {response.status_code} per {nome_fonte}")
            return []
        root = ET.fromstring(response.content)
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        items = root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry')
        for item in items:
            title_node = item.find('title') or item.find('{http://www.w3.org/2005/Atom}title')
            desc_node = item.find('description') or item.find('{http://www.w3.org/2005/Atom}summary')
            title = title_node.text if title_node is not None else ""
            desc = desc_node.text if desc_node is not None else ""
            risultati.append({
                "lat": None, "lon": None, "pos": title,
                "inizio": oggi, "ultima_segnalazione": oggi,
                "fonte": nome_fonte, "desc": title,
                "costo": estrai_costo(f"{title} {desc}")
            })
        print(f"✅ Recuperati {len(risultati)} elementi da {nome_fonte}")
        return risultati
    except Exception as e:
        print(f"⚠️ Fonte {nome_fonte} saltata: {e}")
        return []

def aggiorna_database():
    print(f"🚀 --- AVVIO SCANSIONE AUTOMATICA: {datetime.now()} ---")
    
    try:
        print("🔗 Connessione a Supabase...")
        res = supabase.table("lavori").select("latitudine, longitudine").execute()
        lavori_esistenti = res.data
        print(f"📦 Database attuale contiene {len(lavori_esistenti)} punti.")
    except Exception as e:
        print(f"❌ Errore database: {e}")
        lavori_esistenti = []

    fonti_rss = [
        {"url": "https://www.comune.roma.it/notizie/rss/aree-tematiche/mobilita-e-trasporti", "nome": "Roma Mobilità"},
        {"url": "https://www.comune.milano.it/wps/portal/ist/it/news?isRss=true", "nome": "Milano News"},
        {"url": "https://servizi.comune.fi.it/rss/viabilita", "nome": "Firenze Viabilità"}
    ]

    lista_totale = fetch_osm_lavori()
    for f in fonti_rss:
        lista_totale.extend(fetch_rss_lavori(f['url'], f['nome']))

    print(f"📋 Totale elementi da verificare: {len(lista_totale)}")
    
    da_inserire_bulk = []
    for i, l in enumerate(lista_totale):
        lat, lon = l.get('lat'), l.get('lon')
        if not lat or not lon:
            try:
                print(f"📍 Geocoding ({i+1}/{len(lista_totale)}): {l['pos'][:30]}...")
                location = geolocator.geocode(f"{l['pos']}, Italy", timeout=5)
                if location: 
                    lat, lon = location.latitude, location.longitude
                    time.sleep(1.1) # Pausa per non essere bloccati da Nominatim
                else: continue
            except: continue
            
        if lat and lon and not è_un_doppione(lat, lon, lavori_esistenti):
            da_inserire_bulk.append({
                "latitudine": lat, "longitudine": lon,
                "data_inizio": l["inizio"], "ultima_segnalazione": l["ultima_segnalazione"],
                "fonte": l["fonte"], "descrizione": l["desc"][:250],
                "costo": l.get("costo", "N.D.")
            })
            lavori_esistenti.append({"latitudine": lat, "longitudine": lon})

    if da_inserire_bulk:
        print(f"📤 Inserimento di {len(da_inserire_bulk)} nuovi record...")
        try:
            supabase.table("lavori").insert(da_inserire_bulk).execute()
            print(f"🎉 Aggiornamento terminato con successo!")
        except Exception as e:
            print(f"❌ Errore inserimento: {e}")
    else:
        print("ℹ️ Nessun nuovo cantiere trovato negli ultimi 7 giorni.")

if __name__ == "__main__":
    aggiorna_database()
