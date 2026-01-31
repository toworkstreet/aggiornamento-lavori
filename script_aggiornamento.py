import os
import requests
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from supabase import create_client, Client

# --- CONFIGURAZIONE ---
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

geolocator = Nominatim(user_agent="roadwork_app_v2")

def estrai_costo(testo):
    if not testo: return "N.D."
    # Cerca cifre seguite da €, Euro, mln ecc.
    match = re.search(r'(\d+[\d\.,]*)\s*(€|Euro|euro|milioni|mln|Mln)', testo)
    if match: return match.group(0)
    return "N.D."

def è_un_doppione(nuova_lat, nuova_lon, lavori_esistenti, soglia_metri=50):
    for es in lavori_esistenti:
        if es.get('latitudine') and es.get('longitudine'):
            distanza = geodesic((nuova_lat, nuova_lon), (es['latitudine'], es['longitudine'])).meters
            if distanza < soglia_metri: return True
    return False

def fetch_osm_lavori():
    print("🛰️ Ricerca cantieri su OpenStreetMap (Italia Intera)...")
    query = """
    [out:json][timeout:60];
    area["ISO3166-1"="IT"]->.italy;
    (node["highway"="construction"](area.italy); way["highway"="construction"](area.italy););
    out center;
    """
    try:
        response = requests.post("https://overpass-api.de/api/interpreter", data={'data': query}, timeout=90)
        data = response.json()
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        for element in data.get('elements', []):
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
    print(f"📰 Tentativo lettura Feed: {nome_fonte}...")
    try:
        response = requests.get(rss_url, timeout=15)
        if response.status_code != 200: return []
        root = ET.fromstring(response.content)
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        # Supporto sia per formato RSS che Atom
        items = root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry')
        for item in items:
            title_node = item.find('title') or item.find('{http://www.w3.org/2005/Atom}title')
            desc_node = item.find('description') or item.find('{http://www.w3.org/2005/Atom}summary')
            title = title_node.text if title_node is not None else ""
            desc = desc_node.text if desc_node is not None else ""
            testo_completo = f"{title} {desc}"
            risultati.append({
                "lat": None, "lon": None, "pos": title,
                "inizio": oggi, "ultima_segnalazione": oggi,
                "fonte": nome_fonte, "desc": title,
                "costo": estrai_costo(testo_completo)
            })
        return risultati
    except Exception as e:
        print(f"⚠️ Fonte {nome_fonte} non raggiungibile: {e}")
        return []

def aggiorna_database():
    print(f"🚀 Avvio scansione multicanale del {datetime.now()}")
    
    try:
        res = supabase.table("lavori").select("latitudine, longitudine").execute()
        lavori_esistenti = res.data
    except: lavori_esistenti = []

    # --- LISTA FONTI AGGIORNATA ---
    fonti_rss = [
        # AGGREGATORI NAZIONALI E GRANDI FONTI
        {"url": "https://www.stradeanas.it/it/viabilita/piani-interventi/feed", "nome": "ANAS Nazionale"},
        {"url": "https://www.mit.gov.it/notizie/rss.xml", "nome": "Ministero Infrastrutture"},
        {"url": "https://www.regione.lombardia.it/wps/portal/istituzionale/HP/RSS/lavori-pubblici", "nome": "Regione Lombardia"},
        {"url": "http://www.comune.torino.it/notizie/rss/viabilita.xml", "nome": "Comune Torino"},
        {"url": "https://www.comune.milano.it/wps/portal/ist/it/news?isRss=true", "nome": "Comune Milano"},
        {"url": "https://www.venetostrade.it/my-portlet/rss/ordinanze", "nome": "Veneto Strade"},
        {"url": "https://www.muoversinonostante.it/rss", "nome": "Muoversi in Toscana"},
        {"url": "https://www.luceverde.it/rss/roma", "nome": "LuceVerde Roma (ACI)"}
    ]

    lista_totale = fetch_osm_lavori()
    for f in fonti_rss:
        lista_totale.extend(fetch_rss_lavori(f['url'], f['nome']))

    da_inserire_bulk = []
    for l in lista_totale:
        lat, lon = l.get('lat'), l.get('lon')
        if not lat or not lon:
            try:
                # Il geocoding è lento, limitiamo ai titoli che sembrano indirizzi
                location = geolocator.geocode(f"{l['pos']}, Italy", timeout=5)
                if location: lat, lon = location.latitude, location.longitude
                else: continue
            except: continue
            
        if lat and lon and not è_un_doppione(lat, lon, lavori_esistenti):
            da_inserire_bulk.append({
                "latitudine": lat, "longitudine": lon,
                "data_inizio": l["inizio"], "ultima_segnalazione": l["ultima_segnalazione"],
                "fonte": l["fonte"], "descrizione": l["desc"],
                "costo": l.get("costo", "N.D.")
            })
            lavori_esistenti.append({"latitudine": lat, "longitudine": lon})

    if da_inserire_bulk:
        print(f"📦 Caricamento di {len(da_inserire_bulk)} nuovi record...")
        for i in range(0, len(da_inserire_bulk), 100):
            try:
                supabase.table("lavori").insert(da_inserire_bulk[i:i+100]).execute()
            except Exception as e:
                print(f"❌ Errore inserimento batch: {e}")
        print(f"✅ Aggiornamento terminato.")
    else:
        print("ℹ️ Nessun nuovo dato trovato.")

if __name__ == "__main__":
    aggiorna_database()
