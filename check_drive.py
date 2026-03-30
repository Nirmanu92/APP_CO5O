from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def list_folders():
    scope = ["https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file("manuel-hernandez-19db5137c96a.json", scopes=scope)
        service = build('drive', 'v3', credentials=creds)
        
        # Buscar carpetas llamadas 'COTIZACIONES'
        query = "name = 'COTIZACIONES' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print("No se encontró ninguna carpeta llamada 'COTIZACIONES'.")
            # Listar todas las carpetas disponibles para ver qué hay
            print("\nCarpetas disponibles:")
            results_all = service.files().list(q="mimeType = 'application/vnd.google-apps.folder' and trashed = false", fields="files(id, name)").execute()
            for folder in results_all.get('files', []):
                print(f"- {folder['name']} (ID: {folder['id']})")
        else:
            for item in items:
                print(f"Encontrada: {item['name']} (ID: {item['id']})")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_folders()
