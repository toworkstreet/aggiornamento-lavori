import os
import requests
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def aggiorna_lavori_autostrade():
    # Nota: Molti enti usano file JSON o GeoJSON pubblici
    # Qui usiamo un esempio di endpoint che potresti trovare su dati.gov.it
    api_url = "https://api.strade.it/v1/lavori-in-corso" 
    
    try:
        # Per ora simuliamo il recupero per non far fallire lo script se l'URL cambia
        # In un caso reale useresti: response = requests.get(api_url).json()
        lavori_finti = [
            {"desc": "Lavori manutenzione A1", "lat": 44.4949, "lon": 11.3426, "inizio": "2026-02-01"},
            {"desc": "Rifacimento asfalto A4", "lat": 45.4642, "lon": 9.1900, "inizio": "2026-01-29"}
        ]

        for item in lavori_finti:
            # Controlliamo se il lavoro esiste già per non duplicarlo
            check = supabase.table("lavori").select("*").eq("descrizione", item["desc"]).execute()
            
            if len(check.data) == 0:
                supabase.table("lavori").insert({
                    "descrizione": item["desc"],
                    "latitudine": item["lat"],
                    "longitudine": item["lon"],
                    "data_inizio": item["inizio"],
                    "fonte": "Autostrade"
                }).execute()
                print(f"Inserito: {item['desc']}")
            else:
                print(f"Già presente: {item['desc']}")
                
    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    aggiorna_lavori_autostrade()
