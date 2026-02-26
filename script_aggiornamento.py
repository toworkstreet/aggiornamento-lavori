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
geolocator = Nominatim(user_agent="roadwork_tracker_italy_v4")

# --- FUNZIONI DI SUPPORTO ---
def estrai_provincia(testo):
    if not testo: return "N.D."
    testo_upper = testo.upper()

    mappa_province = {
        "AGRIGENTO": "AG", "ALESSANDRIA": "AL", "ANCONA": "AN", "AOSTA": "AO", "AREZZO": "AR", 
        "ASCOLI PICENO": "AP", "ASTI": "AT", "AVELLINO": "AV", "BARI": "BA", "BARLETTA": "BT", 
        "ANDRIA": "BT", "TRANI": "BT", "BELLUNO": "BL", "BENEVENTO": "BN", "BERGAMO": "BG", 
        "BIELLA": "BI", "BOLOGNA": "BO", "BOLZANO": "BZ", "BRESCIA": "BS", "BRINDISI": "BR", 
        "CAGLIARI": "CA", "CALTANISSETTA": "CL", "CAMPOBASSO": "CB", "CASERTA": "CE", 
        "CATANIA": "CT", "CATANZARO": "CZ", "CHIETI": "CH", "COMO": "CO", "COSENZA": "CS", 
        "CREMONA": "CR", "CROTONE": "KR", "CUNEO": "CN", "ENNA": "EN", "FERMO": "FM", 
        "FERRARA": "FE", "FIRENZE": "FI", "FOGGIA": "FG", "FORLI": "FC", "CESENA": "FC", 
        "FROSINONE": "FR", "GENOVA": "GE", "GORIZIA": "GO", "GROSSETO": "GR", "IMPERIA": "IM", 
        "ISERNIA": "IS", "L'AQUILA": "AQ", "LA SPEZIA": "SP", "LATINA": "LT", "LECCE": "LE", 
        "LECCO": "LC", "LIVORNO": "LI", "LODI": "LO", "LUCCA": "LU", "MACERATA": "MC", 
        "MANTOVA": "MN", "MASSA": "MS", "CARRARA": "MS", "MATERA": "MT", "MESSINA": "ME", 
        "MILANO": "MI", "MODENA": "MO", "MONZA": "MB", "BRIANZA": "MB", "NAPOLI": "NA", 
        "NOVARA": "NO", "NUORO": "NU", "ORISTANO": "OR", "PADOVA": "PD", "PALERMO": "PA", 
        "PARMA": "PR", "PAVIA": "PV", "PERUGIA": "PG", "PESARO": "PU", "URBINO": "PU", 
        "PESCARA": "PE", "PIACENZA": "PC", "PISA": "PI", "PISTOIA": "PT", "PORDENONE": "PN", 
        "POTENZA": "PZ", "PRATO": "PO", "RAGUSA": "RG", "RAVENNA": "RA", "REGGIO CALABRIA": "RC", 
        "REGGIO EMILIA": "RE", "RIETI": "RI", "RIMINI": "RN", "ROMA": "RM", "ROVIGO": "RO", 
        "SALERNO": "SA", "SASSARI": "SS", "SAVONA": "SV", "SIENA": "SI", "SIRACUSA": "SR", 
        "SONDRIO": "SO", "SUD SARDEGNA": "SU", "TARANTO": "TA", "TERAMO": "TE", "TERNI": "TR", 
        "TORINO": "TO", "TRAPANI": "TP", "TRENTO": "TN", "TREVISO": "TV", "TRIESTE": "TS", 
        "UDINE": "UD", "VARESE": "VA", "VENEZIA": "VE", "VERBANO": "VB", "CUSIO": "VB", 
        "OSSOLA": "VB", "VERCELLI": "VC", "VERONA": "VR", "VIBO VALENTIA": "VV", "VICENZA": "VI", 
        "VITERBO": "VT"
    }

    for nome, sigla in mappa_province.items():
        if nome in testo_upper:
            return sigla

    pattern = r'\b(AG|AL|AN|AO|AR|AP|AT|AV|BA|BT|BL|BN|BG|BI|BO|BR|BS|BZ|CA|CB|CI|CE|CH|CL|CR|CS|CT|CZ|EN|FC|FE|FG|FI|FM|FR|GE|GO|GR|IM|IS|KR|LC|LE|LI|LO|LT|LU|MB|MC|ME|MI|MN|MS|MT|NA|NO|NU|OG|OT|OR|PA|PC|PD|PE|PG|PI|PN|PO|PR|PT|PU|PV|PZ|RA|RC|RG|RI|RM|RN|RO|SA|SS|SI|SR|SU|SV|TA|TE|TR|TS|TV|UD|VA|VB|VC|VE|VI|VR|VS|VT|VV|SP|AQ|RE|MO|TP|TN)\b'
    match = re.search(pattern, testo_upper)
    if match: return match.group(0)
    return "N.D."

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

# --- 1. FONTE: OPENSTREETMAP ---
def fetch_osm_lavori():
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
        data = response.json()
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        elements = data.get('elements', [])
        print(f"🔎 Trovati {len(elements)} elementi su OSM")
        
        for element in elements:
            tags = element.get('tags', {})
            lat = element.get('lat') or element.get('center', {}).get('lat')
            lon = element.get('lon') or element.get('center', {}).get('lon')
            desc = tags.get('description', tags.get('note', 'Cantiere stradale (OSM)'))
            if lat and lon:
                risultati.append({
                    "lat": lat, "lon": lon, "inizio": tags.get('start_date', oggi),
                    "ultima_segnalazione": oggi, "fonte": "OpenStreetMap",
                    "desc": desc, "costo": estrai_costo(desc)
                })
        return risultati
    except Exception as e:
        print(f"⚠️ Errore OSM (saltato): {e}")
        return []

# --- 2. FONTE: RSS FEEDS ---
def fetch_rss_lavori(rss_url, nome_fonte, citta_riferimento=""):
    print(f"📰 Lettura Feed RSS: {nome_fonte}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(rss_url, headers=headers, timeout=15)
        if response.status_code != 200: return []
        
        root = ET.fromstring(response.content)
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")
        items = root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry')
        
        for item in items:
            title_node = item.find('title') or item.find('{http://www.w3.org/2005/Atom}title')
            desc_node = item.find('description') or item.find('{http://www.w3.org/2005/Atom}summary')
            title = title_node.text if title_node is not None else ""
            desc = desc_node.text if desc_node is not None else ""
            
            # Aggiungo la città per aiutare il Geocoder di Nominatim
            indirizzo_ricerca = f"{title}, {citta_riferimento}" if citta_riferimento else title

            risultati.append({
                "lat": None, "lon": None, "pos": indirizzo_ricerca,
                "inizio": oggi, "ultima_segnalazione": oggi,
                "fonte": nome_fonte, "desc": f"{title} {desc}",
                "costo": estrai_costo(f"{title} {desc}")
            })
        print(f"✅ Recuperati {len(risultati)} elementi da {nome_fonte}")
        return risultati
    except Exception as e:
        print(f"⚠️ Fonte {nome_fonte} saltata: {e}")
        return []

# --- 3. NUOVA FONTE: OPEN DATA GEOJSON ---
def fetch_geojson_lavori(geojson_url, nome_fonte):
    print(f"🌍 Lettura GeoJSON Open Data: {nome_fonte}...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        response = requests.get(geojson_url, timeout=15)
        if response.status_code != 200: return []
        
        data = response.json()
        risultati = []
        oggi = datetime.now().strftime("%Y-%m-%d")

        for feature in data.get('features', []):
            geom = feature.get('geometry')
            props = feature.get('properties', {})
            if not geom: continue

            lat, lon = None, None
            # Estrae le coordinate in base al tipo di geometria (Punto o Linea)
            if geom['type'] == 'Point':
                lon, lat = geom['coordinates']
            elif geom['type'] in ['LineString', 'MultiLineString']:
                # Prende il punto di inizio del cantiere
                coords = geom['coordinates'][0] if geom['type'] == 'LineString' else geom['coordinates'][0][0]
                lon, lat = coords

            if lat and lon:
                # Cerca campi descrizione tipici nei GeoJSON italiani
                desc = props.get('descrizione', props.get('oggetto', props.get('note', 'Cantiere stradale (Open Data)')))
                inizio = props.get('data_inizio', props.get('dal', oggi))
                
                risultati.append({
                    "lat": lat, "lon": lon, "pos": desc,
                    "inizio": inizio, "ultima_segnalazione": oggi,
                    "fonte": nome_fonte, "desc": str(desc),
                    "costo": estrai_costo(str(desc))
                })
        print(f"✅ Recuperati {len(risultati)} cantieri con coordinate esatte da {nome_fonte}")
        return risultati
    except Exception as e:
        print(f"⚠️ Errore GeoJSON {nome_fonte}: {e}")
        return []

# --- LOGICA PRINCIPALE ---
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

    # 1. Definizione delle Fonti RSS
    fonti_rss = [
        {"url": "https://www.stradeanas.it/it/rss.xml", "nome": "ANAS News", "citta": "Italy"},
        {"url": "https://www.comune.roma.it/notizie/rss/aree-tematiche/mobilita-e-trasporti", "nome": "Roma Mobilità", "citta": "Roma"},
        {"url": "https://www.comune.milano.it/wps/portal/ist/it/news?isRss=true", "nome": "Milano News", "citta": "Milano"},
    ]

    # 2. Definizione delle Fonti GeoJSON (Esempi di Open Data reali o standard)
    fonti_geojson = [
        # Esempio: Portale Open Data Inserisci URL reali se li trovi su dati.gov.it
        {
            "url": "https://dati.comune.bologna.it/api/explore/v2.1/catalog/datasets/cantieri-in-corso/exports/geojson",
            "nome": "Comune di Bologna"
        },
        {
            "url": "https://dati.comune.milano.it/dataset/7996df8a-530e-473d-882d-467dc0269399/resource/90097c31-3069-4560-9372-8823525a4072/download/cantieri-v-p-m-m_geo.json",
            "nome": "Comune di Milano"
        },
        {
            "url": "https://servizi.comune.fi.it/opendata/viabilita_geojson",
            "nome": "Comune di Firenze"
        },
        {
            "url": "https://geodati.fmv.it/api/v1/datasets/cantieri-stradali/geojson",
            "nome": "Città Metropolitana di Venezia"
        },
        {
            "url": "http://geoportale.comune.torino.it/geoserver/wfs?service=WFS&version=1.1.0&request=GetFeature&typeName=geoportale:cantieri_lavori_pubblici&outputFormat=application/json",
            "nome": "Comune di Torino"
        },
        {
            "url": "https://opendata.comune.bari.it/dataset/cantieri-stradali/resource/geojson",
            "nome": "Comune di Bari"
        }
    ]

    # Raccoglie tutti i dati
    lista_totale = fetch_osm_lavori()
    
    for f in fonti_rss:
        lista_totale.extend(fetch_rss_lavori(f['url'], f['nome'], f.get('citta', '')))
        
    for f in fonti_geojson:
        lista_totale.extend(fetch_geojson_lavori(f['url'], f['nome']))

    print(f"📋 Totale elementi da verificare e geolocalizzare: {len(lista_totale)}")
    
    da_inserire_bulk = []
    for i, l in enumerate(lista_totale):
        lat, lon = l.get('lat'), l.get('lon')
        
        # Geocoding per le fonti RSS che non hanno coordinate
        if not lat or not lon:
            try:
                print(f"📍 Geocoding ({i+1}/{len(lista_totale)}): {l['pos'][:40]}...")
                location = geolocator.geocode(f"{l['pos']}, Italy", timeout=5)
                if location: 
                    lat, lon = location.latitude, location.longitude
                    time.sleep(1.1) # Rispetta i limiti di Nominatim
                else: continue
            except: continue
            
        if lat and lon and not è_un_doppione(lat, lon, lavori_esistenti):
            # PROVA 1: Estrazione dal testo
            prov = estrai_provincia(l["desc"])
            
            # PROVA 2: Se N.D., prova dalle coordinate (Reverse Geocoding)
            if prov == "N.D.":
                try:
                    location = geolocator.reverse((lat, lon), timeout=5)
                    if location and 'address' in location.raw:
                        addr = location.raw['address']
                        target = addr.get('province') or addr.get('county') or addr.get('city')
                        if target:
                            prov = estrai_provincia(target)
                    time.sleep(1.1)
                except: pass

            da_inserire_bulk.append({
                "latitudine": lat, "longitudine": lon,
                "provincia": prov,
                "data_inizio": l["inizio"], "ultima_segnalazione": l["ultima_segnalazione"],
                "fonte": l["fonte"], "descrizione": l["desc"][:250],
                "costo": l.get("costo", "N.D.")
            })
            lavori_esistenti.append({"latitudine": lat, "longitudine": lon})

    if da_inserire_bulk:
        print(f"📤 Inserimento di {len(da_inserire_bulk)} nuovi record nel database...")
        try:
            supabase.table("lavori").insert(da_inserire_bulk).execute()
            print(f"🎉 Aggiornamento terminato con successo!")
        except Exception as e:
            print(f"❌ Errore inserimento: {e}")
    else:
        print("ℹ️ Nessun nuovo cantiere trovato (o tutti già presenti).")

if __name__ == "__main__":
    aggiorna_database()
