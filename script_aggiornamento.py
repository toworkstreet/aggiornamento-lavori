import os
from supabase import create_client, Client

# Recupera le chiavi dai Secrets di GitHub
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

def test_connessione():
    try:
        # Tenta di connettersi a Supabase
        supabase: Client = create_client(url, key)
        print("✅ Connessione a Supabase riuscita!")
        
        # Esempio: Inserisce un lavoro di test
        data = supabase.table("lavori").insert({
            "descrizione": "Cantiere di test",
            "fonte": "Script Automatico",
            "latitudine": 41.8902,
            "longitudine": 12.4922
        }).execute()
        
        print("✅ Dati inseriti correttamente!")
    except Exception as e:
        print(f"❌ Errore durante l'esecuzione: {e}")

if __name__ == "__main__":
    test_connessione()
