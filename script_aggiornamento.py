import os
import requests
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def get_dati_anas():
    print("Recupero dati da Anas VAI...")
    # URL semplificato dell'API che alimenta la mappa Anas
    anas_url = "https://www.stradeanas.it/it/informazioni-viabilita/vai" 
    # In un caso reale, qui analizzeremmo il GeoJSON pubblico di Anas
    return [{"desc": "Cantiere Anas SS1", "lat": 41.8, "lon": 12.3, "fonte": "Anas"}]

def get_dati_autostrade():
    print("Recupero dati da Autostrade per l'Italia...")
    # API Open Data per i cantieri
    return [{"desc": "Lavori A1 KM 200", "lat": 43.5, "lon": 11.2, "fonte": "Autostrade"}]

def get_dati_cciss():
    print("Recupero bollettino CCISS...")
    # Il CCISS pubblica feed XML/RSS sulla viabilità nazionale
    return [{"desc": "Incidente/Lavori segnalati CCISS", "lat": 45.0, "lon": 9.0, "fonte": "CCISS"}]

def salva_su_supabase(lista_lavori):
    for lavoro in lista_lavori:
        try:
            # Usiamo upsert per evitare duplicati basandoci sulla descrizione
            supabase.table("lavori").upsert({
                "descrizione": lavoro["desc"],
                "latitudine": lavoro["lat"],
                "longitudine": lavoro["lon"],
                "fonte": lavoro["fonte"],
                "data_inizio": "2026-01-29" # Esempio data odierna
            }, on_conflict="descrizione").execute()
        except Exception as e:
            print(f"Errore salvataggio: {e}")

if __name__ == "__main__":
    # Eseguiamo tutte le scansioni
    lavori_totali = []
    lavori_totali.extend(get_dati_anas())
    lavori_totali.extend(get_dati_autostrade())
    lavori_totali.extend(get_dati_cciss())
    
    salva_su_supabase(lavori_totali)
    print("Aggiornamento completato con successo!")
