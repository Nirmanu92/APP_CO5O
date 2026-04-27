import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials as UserCredentials
from datetime import date, datetime, timedelta, timezone

# --- HELPER: TIEMPO LOCAL (CDMX) ---
def ahora_mexico():
    """Retorna datetime actual ajustado a Ciudad de México (UTC-6)."""
    # Forzar UTC y luego convertir a CDMX para evitar desfasamientos por zona horaria del servidor
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-6)))

def buscar_index(lista_completa, valor_buscado):
    """Retorna el índice exacto de un valor dentro de la lista que recibirá el widget."""
    if not valor_buscado or str(valor_buscado).strip() == "" or str(valor_buscado) == "Seleccionar...":
        return 0
    v_norm = str(valor_buscado).strip().upper()
    for i, item in enumerate(lista_completa):
        if str(item).strip().upper() == v_norm:
            return i
    return 0

import pandas as pd
import plotly.express as px
from fpdf import FPDF
import io
import os
import json
import time
import tempfile
from PIL import Image

# --- CONFIGURACIÓN UI (DEBE SER LO PRIMERO) ---
try:
    img_favicon = Image.open("ICONO.png")
    st.set_page_config(page_title="CO5O - Registro Maestro", page_icon=img_favicon, layout="wide", initial_sidebar_state="expanded")
except:
    st.set_page_config(page_title="CO5O - Registro Maestro", page_icon="ICONO.png", layout="wide", initial_sidebar_state="expanded")

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- CONFIGURACIÓN DE IDENTIDAD Y CARPETAS ---
FILE_JSON_SERVICE = "manuel-hernandez-d0db41fdbc21.json"
ID_CARPETA_COTIZACIONES = "1KsDQi-jnVyoO9cQ_8asn2PVDyIUH0x_l"
ID_CARPETA_IMAGENES = "1rekfucEG3U--N1otCQjnHl0j3hdGRwpg"
ID_CARPETA_EVIDENCIAS = "19JnLfSSLh4ICq-gHZw0pQfcOpY9fmqj0"
ID_SHEET_PEDIDOS = "1YJWY1C2OYpGypTyYWrXJRIZTU9jFwC_cvBwLNp2aQDE"

# --- 1. AUTENTICACIÓN PARA SHEETS (CUENTA DE SERVICIO) ---
@st.cache_resource
def conectar_google_sheets():
    """Conecta con Google Sheets usando Secrets de Streamlit o archivo local."""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # 1. Intentar con Secrets (Entorno Nube)
    if "gcp_service_account" in st.secrets:
        try:
            sec = st.secrets["gcp_service_account"]
            creds_info = dict(sec) if not isinstance(sec, str) else json.loads(sec)
            
            if "private_key" in creds_info:
                pk = str(creds_info["private_key"])
                # RECONSTRUCCIÓN DE LLAVE PEM (Evita errores de formato de Streamlit)
                pk = pk.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
                pk = pk.replace("\\n", "").replace("\n", "").replace(" ", "").strip()
                pk_clean = "-----BEGIN PRIVATE KEY-----\n"
                for i in range(0, len(pk), 64): pk_clean += pk[i:i+64] + "\n"
                pk_clean += "-----END PRIVATE KEY-----\n"
                creds_info["private_key"] = pk_clean
            
            creds = Credentials.from_service_account_info(creds_info, scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            return None

    if os.path.exists(FILE_JSON_SERVICE):
        try:
            creds = Credentials.from_service_account_file(FILE_JSON_SERVICE, scopes=scope)
            return gspread.authorize(creds)
        except:
            return None
    return None

# --- FUNCIONES DE LECTURA CON CACHÉ (PROTECCIÓN DE CUOTA API 429) ---
def normalizar_registros(registros):
    """Convierte todas las llaves de los registros a MAYÚSCULAS_Y_GUIONES_BAJOS."""
    if not registros: return []
    return [{str(k).upper().replace(" ", "_"): v for k, v in r.items()} for r in registros]

@st.cache_data(ttl=600) # Guardar en memoria por 10 minutos
def obtener_datos_maestros_cached(nombre_archivo, nombre_ws="sheet1"):
    """Lee datos maestros con caché para ahorrar cuota de API."""
    try:
        gc = conectar_google_sheets()
        sh = gc.open(nombre_archivo)
        try: ws = sh.worksheet(nombre_ws)
        except: ws = sh.get_worksheet(0)
        return normalizar_registros(ws.get_all_records())
    except:
        return []

@st.cache_data(ttl=300) # Guardar escaneo OVO por 5 minutos
def obtener_directorio_ejecutivo_cached(sheet_id, usuario_id):
    """Lee el directorio de un ejecutivo específico con caché."""
    try:
        gc = conectar_google_sheets()
        sh = None
        # Intentar abrir por ID si es válido (longitud típica de ID de Google > 20)
        if sheet_id and len(str(sheet_id)) > 20:
            try: sh = gc.open_by_key(sheet_id)
            except: pass
        
        # Si no hay ID o falló, intentar por nombre estándar
        if not sh:
            try: sh = gc.open(f"COTIZACIONES_{usuario_id}")
            except: 
                try: sh = gc.open(str(usuario_id))
                except: return None # No se encontró el archivo
            
        # Buscar en pestañas comunes de directorio
        for name in ["DIRECTORIO", "Directorio", "PROSPECTOS", "CLIENTES", "Contactos", "CONTACTOS", "HOJA1", "Hoja1"]:
            try:
                data = sh.worksheet(name).get_all_records()
                if data: return normalizar_registros(data)
            except: continue
            
        # Último recurso: primera hoja disponible
        return normalizar_registros(sh.get_worksheet(0).get_all_records())
    except Exception as e:
        return None # Error de acceso o permisos

# --- CARGA SÓLO USUARIOS PARA LOGIN ---
def cargar_usuarios_login():
    """Carga la base de datos de usuarios autorizados."""
    if 'usuarios_db' not in st.session_state:
        st.session_state.usuarios_db = obtener_datos_maestros_cached("CONTROL_USUARIOS")
        if not st.session_state.usuarios_db:
            # Reintento sin caché si falla
            try:
                gc = conectar_google_sheets()
                st.session_state.usuarios_db = gc.open("CONTROL_USUARIOS").sheet1.get_all_records()
            except:
                st.error("Error crítico de conexión. Favor de recargar la página.")
                st.stop()

# --- 2. AUTENTICACIÓN PARA DRIVE (OAUTH PERSONAL) ---
def procesar_callback_oauth():
    """Intercambio de código por token y guardado persistente en Sheets."""
    query_params = st.query_params
    if "code" in query_params and "state" in query_params:
        try:
            usuario_regreso = query_params["state"]
            code = query_params["code"]
            
            # Obtener configuración
            client_config = None
            if "google_oauth" in st.secrets:
                sec = st.secrets["google_oauth"]
                oauth_data = dict(sec) if not isinstance(sec, str) else json.loads(sec)
                client_config = oauth_data.get("web") or oauth_data.get("installed") or oauth_data
            
            if not client_config and os.path.exists("client_secrets.json"):
                with open("client_secrets.json", "r") as f:
                    js = json.load(f)
                    client_config = js.get("web") or js.get("installed") or js

            if client_config:
                import requests
                token_url = client_config.get("token_uri", "https://oauth2.googleapis.com/token")
                redirect_uri = "https://appco5o-gunixkfb5hakxc6r5ufshk.streamlit.app" if "google_oauth" in st.secrets else "http://localhost"
                
                data = {
                    'code': code,
                    'client_id': client_config['client_id'],
                    'client_secret': client_config['client_secret'],
                    'redirect_uri': redirect_uri,
                    'grant_type': 'authorization_code'
                }
                
                res = requests.post(token_url, data=data)
                token_data = res.json()
                
                if "access_token" in token_data:
                    # Enriquecer token con datos del cliente para que sea compatible con la librería
                    token_data["client_id"] = client_config['client_id']
                    token_data["client_secret"] = client_config['client_secret']
                    
                    if guardar_token_drive(usuario_regreso, token_data):
                        st.success(f"¡Drive de {usuario_regreso} conectado con éxito!")
                        st.query_params.clear()
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("No se pudo guardar el token en la base de datos.")
        except Exception as e:
            st.error(f"Error técnico en vinculación: {e}")

def guardar_token_drive(usuario, token_data):
    """Guarda el token de Drive en Google Sheets y limpia la caché."""
    try:
        gc = conectar_google_sheets()
        sh = gc.open("CONTROL_USUARIOS")
        ws_users = sh.sheet1
        usuarios_list = ws_users.col_values(1)
        
        if usuario in usuarios_list:
            fila_idx = usuarios_list.index(usuario) + 1
            headers = ws_users.row_values(1)
            if "TOKEN_DRIVE" not in headers:
                ws_users.update_cell(1, len(headers) + 1, "TOKEN_DRIVE")
                col_idx = len(headers) + 1
            else:
                col_idx = headers.index("TOKEN_DRIVE") + 1
            
            ws_users.update_cell(fila_idx, col_idx, json.dumps(token_data))
            # LIMPIAR CACHÉ para que la app lea los nuevos datos inmediatamente
            obtener_datos_maestros_cached.clear()
            if 'usuarios_db' in st.session_state:
                del st.session_state.usuarios_db
            return True
    except Exception as e:
        st.error(f"Error al guardar token: {e}")
    return False

def autenticar_usuario_oauth():
    """Genera el link de autorización manual."""
    usuario = st.session_state.get('usuario')
    if not usuario: return

    # Obtener configuración
    client_config = None
    if "google_oauth" in st.secrets:
        sec = st.secrets["google_oauth"]
        oauth_data = dict(sec) if not isinstance(sec, str) else json.loads(sec)
        client_config = oauth_data.get("web") or oauth_data.get("installed") or oauth_data
    
    if not client_config and os.path.exists("client_secrets.json"):
        with open("client_secrets.json", "r") as f:
            js = json.load(f)
            client_config = js.get("web") or js.get("installed") or js

    if not client_config:
        st.error("Falta configuración de Drive.")
        return

    try:
        auth_uri = client_config.get("auth_uri", "https://accounts.google.com/o/oauth2/auth")
        redirect_uri = "https://appco5o-gunixkfb5hakxc6r5ufshk.streamlit.app" if "google_oauth" in st.secrets else "http://localhost"
        scopes = "https://www.googleapis.com/auth/drive"
        
        auth_url = (
            f"{auth_uri}?client_id={client_config['client_id']}"
            f"&redirect_uri={redirect_uri}"
            f"&scope={scopes}"
            f"&response_type=code"
            f"&access_type=offline"
            f"&prompt=consent"
            f"&state={usuario}"
        )
        
        st.info("Tu cuenta requiere vinculación con Google Drive.")
        st.link_button("👉 VINCULAR MI GOOGLE DRIVE AHORA", auth_url, type="primary", use_container_width=True)
        st.caption("Al terminar en Google, regresarás aquí y tu permiso se guardará en la nube.")
    except Exception as e:
        st.error(f"Error al preparar link: {e}")

def obtener_drive_service():
    """Retorna el servicio de Drive leyendo el token desde Google Sheets."""
    usuario = st.session_state.get('usuario')
    if not usuario or 'usuarios_db' not in st.session_state: return None
    
    try:
        # Buscar token en los datos cargados del usuario
        datos_user = next((u for u in st.session_state.usuarios_db if u['USUARIO'] == usuario), None)
        token_json = datos_user.get('TOKEN_DRIVE')
        
        if token_json:
            token_data = json.loads(token_json)
            # Reconstruir credenciales
            creds = UserCredentials.from_authorized_user_info(token_data, ["https://www.googleapis.com/auth/drive"])
            
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    # Guardar el nuevo token generado tras el refresh
                    new_token_data = json.loads(creds.to_json())
                    # Asegurar que mantenemos client_id/secret si to_json no los incluyó (depende de la versión)
                    if "client_id" not in new_token_data: new_token_data["client_id"] = token_data.get("client_id")
                    if "client_secret" not in new_token_data: new_token_data["client_secret"] = token_data.get("client_secret")
                    
                    guardar_token_drive(usuario, new_token_data)
                except Exception as e:
                    # Si falla el refresh (ej. invalid_grant), devolvemos None para forzar re-vinculación
                    return None
            
            # Si el token sigue expirado y no hay refresh_token, o falló lo anterior
            if creds and creds.expired and not creds.refresh_token:
                return None
                
            return build('drive', 'v3', credentials=creds)
    except Exception as e:
        return None
    return None

# --- GENERADOR DE FOLIO AUTOMÁTICO (OPTIMIZADO) ---
def generar_folio_automatico(cliente_rs, ejecutivo_id):
    """Genera un folio usando los datos ya cargados en sesión para evitar error 429."""
    try:
        # 1. Siglas Cliente
        info_c = next((c for c in st.session_state.directorio if c['RAZON_SOCIAL'] == cliente_rs), {})
        siglas_c = str(info_c.get('SIGLAS', 'SCL')).upper()[:3]
        
        # 2. Siglas Ejecutivo y Sucursal
        info_e = next((u for u in st.session_state.usuarios_db if u['USUARIO'] == ejecutivo_id), {})
        siglas_e = str(info_e.get('SIGLAS', 'SEJ')).upper()[:3]
        sucursal = str(info_e.get('SUCURSAL', 'MX')).upper()[:2]
        
        # 3. Fecha (YYMMDD)
        fecha_str = ahora_mexico().strftime("%y%m%d")
        prefijo = f"{siglas_c}-{siglas_e}-{fecha_str}-{sucursal}"
        
        # 4. Consecutivo (Usar lo que ya tenemos en el historial del Dashboard)
        # Si no hay historial cargado, hacemos una lectura rápida única
        count = 1
        if 'df_resumen_folios' in st.session_state:
            folios = st.session_state.df_resumen_folios
        else:
            ws_res = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
            folios = ws_res.col_values(1)
            st.session_state.df_resumen_folios = folios
            
        for f in folios:
            if str(f).startswith(prefijo):
                count += 1
        
        return f"{prefijo}-{str(count).zfill(3)}"
    except:
        return f"ERROR-{ahora_mexico().strftime('%y%m%d')}-001"

# --- ESTILO DE ALTA DEFINICIÓN (PRO UI) ---
st.markdown("""
    <style>
    /* Importar Inter para un look más moderno */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
        -webkit-font-smoothing: antialiased;
    }

    /* Fondo Degradado Profundo */
    .stApp {
        background: radial-gradient(circle at 50% 0%, #1a1f2c 0%, #050505 100%);
        color: #E2E8F0;
    }
    
    /* Pestañas Modernas */
    [data-baseweb="tab-list"] {
        justify-content: center !important;
        gap: 20px !important;
        background-color: transparent !important;
        padding-bottom: 20px;
    }
    
    [data-baseweb="tab"] {
        min-width: 180px !important;
        text-align: center !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        color: #94A3B8 !important;
        border-radius: 8px 8px 0 0 !important;
        transition: all 0.3s ease;
    }
    
    [aria-selected="true"] {
        color: #FFFFFF !important;
        background: rgba(52, 152, 219, 0.1) !important;
        border-bottom: 3px solid #3498DB !important;
    }

    /* Tarjetas de Métricas (Glassmorphism) */
    div[data-testid="metric-container"] {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.07) !important;
        padding: 20px !important;
        border-radius: 16px !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06) !important;
        transition: transform 0.3s ease;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        background: rgba(255, 255, 255, 0.05) !important;
        border-color: rgba(52, 152, 219, 0.3) !important;
    }

    /* Botones Pro */
    .stButton > button {
        border-radius: 10px !important;
        background: linear-gradient(135deg, #3498DB 0%, #2980B9 100%) !important;
        color: white !important;
        border: none !important;
        padding: 12px 24px !important;
        font-weight: 700 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        box-shadow: 0 4px 15px rgba(52, 152, 219, 0.2) !important;
    }

    .stButton > button:hover {
        box-shadow: 0 6px 20px rgba(52, 152, 219, 0.4) !important;
        transform: scale(1.02);
    }

    .stButton > button:active {
        transform: scale(0.98);
    }

    /* Inputs y Selects */
    div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {
        background-color: rgba(255, 255, 255, 0.02) !important;
        border-radius: 10px !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
    }

    textarea:focus, input:focus, div[data-baseweb="select"] > div:focus-within {
        border-color: #3498DB !important;
        box-shadow: 0 0 0 1px #3498DB !important;
    }

    /* Scrollbar minimalista */
    ::-webkit-scrollbar {
        width: 8px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: #334155;
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #475569;
    }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

def subir_archivo_a_drive(archivo_bytes, nombre_archivo, mimetype='application/pdf'):
    try:
        service = obtener_drive_service()
        
        # Seleccionar ID de carpeta destino según tipo
        folder_id = ID_CARPETA_IMAGENES if "image" in mimetype else ID_CARPETA_COTIZACIONES
        
        file_metadata = {'name': nombre_archivo, 'parents': [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(archivo_bytes), mimetype=mimetype, resumable=False)
        
        file = service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='id, webViewLink'
        ).execute()
        
        # Intentar dar permisos de lectura a cualquiera con el link (opcional en cuentas personales)
        try:
            service.permissions().create(
                fileId=file.get('id'), 
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()
        except: pass
        
        return file.get('webViewLink')
    except Exception as e:
        st.error(f"Error al subir {nombre_archivo}: {e}")
        return ""

def obtener_link_directo_drive(url):
    """Convierte un webViewLink de Drive en un link directo que st.image pueda renderizar."""
    if not url or "drive.google.com" not in url:
        return url
    try:
        if "/d/" in url:
            file_id = url.split("/d/")[1].split("/")[0]
            return f"https://drive.google.com/uc?export=view&id={file_id}"
    except:
        pass
    return url

# Alias para mantener compatibilidad si se usa en otros lados
def subir_pdf_a_drive(pdf_bytes, nombre_archivo):
    return subir_archivo_a_drive(pdf_bytes, nombre_archivo, 'application/pdf')

class PDF(FPDF):
    def __init__(self, **kwargs):
        self.tipo_pdf = kwargs.pop('tipo_pdf', 'COTIZACION')
        super().__init__(**kwargs)

    def header(self):
        # Seleccionar fondo según tipo de PDF
        archivo_fondo = "membrete-pedido.png" if self.tipo_pdf == 'PEDIDO' else "membrete.png"
        
        if os.path.exists(archivo_fondo):
            self.image(archivo_fondo, 0, 0, 215.9, 279.4)
        elif os.path.exists("membrete.png") and self.tipo_pdf != 'PEDIDO':
            self.image("membrete.png", 0, 0, 215.9, 279.4)
        else:
            self.set_draw_color(220, 220, 220)
            self.rect(5, 5, 205.9, 269.4)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(128)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

def limpiar_texto(texto):
    """Reemplaza caracteres Unicode no soportados por fuentes estándar de FPDF."""
    if not texto: return ""
    replacements = {
        "™": "(TM)", "®": "(R)", "©": "(C)", "•": "-", 
        "—": "-", "–": "-", "…": "...", "‘": "'", "’": "'",
        "“": '"', "”": '"'
    }
    t = str(texto)
    for char, rep in replacements.items():
        t = t.replace(char, rep)
    # Forzar a latin-1 para asegurar compatibilidad con fuentes estándar
    return t.encode('latin-1', 'replace').decode('latin-1')

import requests

import tempfile

from PIL import Image

# --- HELPER: DESCARGAR IMAGEN PARA PDF ---
def descargar_imagen_para_pdf(img_obj):
    """Retorna la ruta a un archivo temporal con la imagen para que FPDF la procese con total compatibilidad."""
    if not img_obj: return None
    try:
        content = None
        # 1. Caso: UploadedFile de Streamlit
        if hasattr(img_obj, "read"):
            img_obj.seek(0)
            content = img_obj.read()
        
        # 2. Caso: Link de Google Drive
        elif isinstance(img_obj, str) and "drive.google.com" in img_obj:
            file_id = None
            if "/d/" in img_obj:
                file_id = img_obj.split("/d/")[1].split("/")[0]
            elif "id=" in img_obj:
                file_id = img_obj.split("id=")[1].split("&")[0]
            
            if file_id:
                try:
                    service = obtener_drive_service()
                    content = service.files().get_media(fileId=file_id).execute()
                except Exception as drive_err:
                    # Fallback a descarga pública si la API falla
                    direct_link = obtener_link_directo_drive(img_obj)
                    res = requests.get(direct_link, timeout=10)
                    if res.status_code == 200:
                        content = res.content
        
        if content:
            # Guardar en archivo temporal con extensión genérica .png (FPDF detecta el contenido real)
            fd, path = tempfile.mkstemp(suffix=".png")
            with os.fdopen(fd, 'wb') as tmp:
                tmp.write(content)
            return path
                
    except Exception as e:
        pass
    return None

def generar_pdf_blob(datos_cab, df_partidas, dict_fotos, dict_links={}):
    # --- PROCESAR AGRUPACIÓN ---
    partidas_pdf = []
    current_p = None
    for _, row in df_partidas.iterrows():
        tipo = row.get("Tipo", "PARTIDA")
        if tipo == "PARTIDA":
            if current_p is not None: partidas_pdf.append(current_p)
            current_p = row.copy()
        else:
            if current_p is not None:
                current_p["Venta (Sub)"] += row["Venta (Sub)"]
                current_p["Venta (IVA)"] += row["Venta (IVA)"]
            else:
                partidas_pdf.append(row.copy())
    if current_p is not None: partidas_pdf.append(current_p)
    df_pdf = pd.DataFrame(partidas_pdf)

    pdf = PDF(format="letter")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_left_margin(10)
    pdf.set_right_margin(10)
    pdf.add_page()
    
    # Paleta Elite
    oxford = (40, 40, 40)
    azul_deep = (20, 45, 75)
    gris_borde = (220, 220, 220)
    gris_fondo = (250, 250, 250)
    texto_fuerte = (30, 30, 30)
    texto_suave = (80, 80, 80)

    # --- ENCABEZADO (Folio y Fechas) ---
    pdf.set_xy(10, 25) # Ajuste a margen 10mm
    pdf.set_font("helvetica", "B", 10) # Folio reducido a 10
    pdf.set_text_color(255, 255, 255)
    pdf.cell(100, 6, f"FOLIO: {limpiar_texto(datos_cab['folio'])}", ln=False)

    pdf.set_font("helvetica", "", 7.5)
    pdf.set_x(140)
    fecha_emision = date.today().strftime("%d/%m/%Y")
    try: fecha_vigencia = datetime.strptime(datos_cab['vigencia'], "%Y-%m-%d").strftime("%d/%m/%Y")
    except: fecha_vigencia = datos_cab['vigencia']
    
    pdf.cell(65, 4, f"EMISIÓN: {fecha_emision}", ln=True, align='R')
    pdf.set_x(140)
    pdf.cell(65, 4, f"VIGENCIA: {limpiar_texto(fecha_vigencia)}", ln=True, align='R')

    # --- CUADROS DE CONTACTO (Alineados a 10mm) ---
    pdf.ln(3)
    y_bloque = pdf.get_y()
    pdf.set_draw_color(*gris_borde)
    pdf.set_fill_color(*gris_fondo)
    pdf.rect(10, y_bloque, 95, 14, "F")
    pdf.rect(110, y_bloque, 95, 14, "F")
    
    pdf.set_xy(13, y_bloque + 2)
    pdf.set_font("helvetica", "B", 7.5)
    pdf.set_text_color(*texto_fuerte)
    pdf.cell(85, 3.5, "CLIENTE / ATENCIÓN", ln=True)
    pdf.set_font("helvetica", "", 7.5)
    pdf.set_text_color(*texto_suave)
    pdf.set_x(13)
    pdf.multi_cell(85, 3, f"{limpiar_texto(datos_cab['cliente'])}\n{limpiar_texto(datos_cab['contacto'])}")
    
    pdf.set_xy(113, y_bloque + 2)
    pdf.set_font("helvetica", "B", 7.5)
    pdf.set_text_color(*texto_fuerte)
    pdf.cell(85, 3.5, "EMISOR / CONTACTO", ln=True)
    pdf.set_font("helvetica", "", 7.5)
    pdf.set_text_color(*texto_suave)
    pdf.set_xy(113, y_bloque + 5.5)
    pdf.multi_cell(85, 3, f"{limpiar_texto(datos_cab['ejecutivo'])}\n{limpiar_texto(datos_cab['email'])}")

    # --- TABLA DE PRODUCTOS ---
    pdf.set_xy(10, y_bloque + 17)
    pdf.set_fill_color(*oxford)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 7.5)
    
    # Anchos ajustados para margen 10mm (Total 195mm)
    w_cant, w_desc, w_img, w_unit, w_total = 10, 110, 20, 25, 30
    pdf.cell(w_cant, 7, "CANT", 0, 0, "C", True)
    pdf.cell(w_desc, 7, " DESCRIPCIÓN TÉCNICA", 0, 0, "L", True)
    pdf.cell(w_img, 7, "", 0, 0, "C", True)
    pdf.cell(w_unit, 7, "UNITARIO", 0, 0, "R", True)
    pdf.cell(w_total, 7, "TOTAL (IVA INC) ", 0, 1, "R", True)

    pdf.set_text_color(*texto_fuerte)
    for i, (idx, row) in enumerate(df_pdf.iterrows()):
        pdf.set_font("helvetica", "", 7)
        desc_txt = f"{limpiar_texto(row['Concepto'])}\n{limpiar_texto(row['Descripción'])}"
        lineas = len(pdf.multi_cell(w_desc - 4, 3, desc_txt, split_only=True))
        h_fila = max((lineas * 3) + 4, 20) # Altura mínima de 20mm para la foto

        if pdf.get_y() + h_fila > 260:
            pdf.add_page()
            pdf.set_y(25)
            # Re-header
            pdf.set_fill_color(*oxford)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("helvetica", "B", 7.5)
            pdf.cell(w_cant, 7, "CANT", 0, 0, "C", True)
            pdf.cell(w_desc, 7, " DESCRIPCIÓN TÉCNICA", 0, 0, "L", True)
            pdf.cell(w_img, 7, "", 0, 0, "C", True)
            pdf.cell(w_unit, 7, "UNITARIO", 0, 0, "R", True)
            pdf.cell(w_total, 7, "TOTAL (IVA INC) ", 0, 1, "R", True)
            pdf.set_text_color(*texto_fuerte)

        y_antes = pdf.get_y()
        if i % 2 != 0: pdf.set_fill_color(*gris_fondo)
        else: pdf.set_fill_color(255, 255, 255)
        pdf.rect(10, y_antes, 195, h_fila, "F")
        pdf.set_draw_color(240, 240, 240)
        pdf.line(10, y_antes + h_fila, 205, y_antes + h_fila)

        pdf.set_font("helvetica", "B", 8)
        pdf.cell(w_cant, h_fila, str(int(row['Pzas'])), 0, 0, "C")
        
        # Descripción
        x_d = pdf.get_x()
        pdf.set_xy(x_d + 2, y_antes + 2)
        pdf.set_font("helvetica", "B", 7.5)
        pdf.cell(w_desc - 4, 3.5, limpiar_texto(row['Concepto']), ln=True)
        pdf.set_x(x_d + 2)
        pdf.set_font("helvetica", "", 7)
        pdf.multi_cell(w_desc - 4, 3, limpiar_texto(row['Descripción']))
        
        # Imagen (CONFINAMIENTO TOTAL: Max 18x18mm sin distorsión)
        pos_x_img = x_d + w_desc + 1
        pos_y_img = y_antes + 1
        foto_path = descargar_imagen_para_pdf(dict_fotos.get(idx) or dict_links.get(idx))
        
        if foto_path:
            try:
                from PIL import Image
                with Image.open(foto_path) as img:
                    width, height = img.size
                    aspect = width / height
                    
                    # Calcular dimensiones proporcionales dentro de un cuadro de 18x18
                    if aspect > 1: # Es más ancha
                        w_img_p = 18
                        h_img_p = 18 / aspect
                    else: # Es más alta o cuadrada
                        h_img_p = 18
                        w_img_p = 18 * aspect
                    
                    # Centrado horizontal dentro del espacio de 20mm
                    offset_x = (20 - w_img_p) / 2
                    # Centrado vertical dentro de la fila (si h_fila > h_img_p)
                    offset_y = (h_fila - h_img_p) / 2
                    
                    pdf.image(foto_path, x_d + w_desc + offset_x, y_antes + offset_y, w=w_img_p, h=h_img_p)
            except Exception as e:
                pass # Si falla Pillow, no pintar imagen para no romper el PDF
        
        # Precios
        pdf.set_xy(x_d + w_desc + w_img, y_antes)
        pdf.set_font("helvetica", "", 7.5)
        pdf.cell(w_unit, h_fila, f"$ {row['Venta (Sub)']:,.2f}", 0, 0, "R")
        pdf.set_font("helvetica", "B", 7.5)
        pdf.cell(w_total, h_fila, f"$ {row['Venta (IVA)'] * row['Pzas']:,.2f} ", 0, 1, "R")
        
        pdf.set_y(y_antes + h_fila)

    # --- TOTALES (Compacto) ---
    pdf.ln(5)
    if pdf.get_y() > 220: pdf.add_page(); pdf.set_y(32)
    
    y_final = pdf.get_y()
    pdf.set_font("helvetica", "B", 8)
    pdf.set_text_color(*texto_fuerte)
    pdf.set_xy(15, y_final)
    pdf.cell(100, 5, "TÉRMINOS Y CONDICIONES", ln=True)
    pdf.set_font("helvetica", "", 7)
    pdf.set_text_color(*texto_suave)
    pdf.set_x(15)
    txt_t = f"Entrega: {datos_cab['entrega']} | Pago: {datos_cab['pago']}\nNotas: {datos_cab['condiciones']}"
    pdf.multi_cell(90, 3.5, limpiar_texto(txt_t))

    subt_v = (df_partidas["Venta (Sub)"] * df_partidas["Pzas"]).sum()
    iva_v = subt_v * 0.16
    total_v = subt_v + iva_v

    pdf.set_xy(135, y_final)
    pdf.set_font("helvetica", "", 9)
    pdf.set_text_color(*texto_suave)
    pdf.cell(30, 6, "Subtotal:", 0, 0)
    pdf.cell(30, 6, f"$ {subt_v:,.2f}", 0, 1, "R")
    pdf.set_x(135)
    pdf.cell(30, 6, "IVA (16%):", 0, 0)
    pdf.cell(30, 6, f"$ {iva_v:,.2f}", 0, 1, "R")
    
    pdf.set_x(135)
    pdf.set_fill_color(*azul_deep)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(60, 10, f"TOTAL MXN:  $ {total_v:,.2f} ", 0, 1, "C", True)

    resultado = pdf.output(dest='S')
    return resultado.encode('latin-1') if isinstance(resultado, str) else bytes(resultado)

    resultado = pdf.output(dest='S')
    if isinstance(resultado, str):
        return resultado.encode('latin-1')
    return bytes(resultado)

def generar_remision_blob(datos_cab, df_partidas, dict_fotos, dict_links={}):
    # --- PROCESAR AGRUPACIÓN ---
    partidas_pdf = []
    current_p = None
    for _, row in df_partidas.iterrows():
        tipo = row.get("Tipo", "PARTIDA")
        if tipo == "PARTIDA":
            if current_p is not None: partidas_pdf.append(current_p)
            current_p = row.copy()
        else:
            if current_p is not None: pass # En remisión solo importa la partida principal
            else: partidas_pdf.append(row.copy())
    if current_p is not None: partidas_pdf.append(current_p)
    df_pdf = pd.DataFrame(partidas_pdf)

    pdf = PDF(format="letter")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    
    azul_corp = (31, 73, 125)
    gris_suave = (245, 246, 247)
    gris_texto = (60, 60, 60)

    # --- ENCABEZADO ---
    pdf.set_font("helvetica", "B", 16)
    pdf.set_xy(15, 35)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(100, 7, f"REMISION: {limpiar_texto(datos_cab['folio'])}", ln=False)
    
    fecha_emision = date.today().strftime("%d/%m/%Y")
    pdf.set_font("helvetica", "", 10)
    pdf.set_x(130)
    pdf.cell(65, 5, f"Fecha de entrega: {fecha_emision}", ln=True, align='R')

    pdf.ln(6)
    
    y_bloque = pdf.get_y()
    pdf.set_fill_color(*gris_suave)
    pdf.rect(15, y_bloque, 85, 18, "F")
    pdf.rect(110, y_bloque, 85, 18, "F")
    
    pdf.set_xy(18, y_bloque + 2)
    pdf.set_font("helvetica", "", 9)
    pdf.set_text_color(*gris_texto)
    pdf.multi_cell(78, 4.5, f"{limpiar_texto(datos_cab['cliente'])}\nAtención: {limpiar_texto(datos_cab['contacto'])}")
    
    pdf.set_xy(113, y_bloque + 2)
    pdf.multi_cell(78, 4.5, f"{limpiar_texto(datos_cab['ejecutivo'])}\n{limpiar_texto(datos_cab['email'])}")

    # --- TABLA DE PRODUCTOS ---
    pdf.set_xy(15, y_bloque + 22)
    pdf.set_fill_color(*azul_corp)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 9)
    
    w_cant, w_desc, w_img = 15, 140, 25
    pdf.cell(w_cant, 10, "CANT", 0, 0, "C", True)
    pdf.cell(w_desc, 10, " DESCRIPCIÓN DEL MATERIAL", 0, 0, "L", True)
    pdf.cell(w_img, 10, "", 0, 1, "C", True)

    pdf.set_text_color(*gris_texto)
    
    for i, (idx, row) in enumerate(df_pdf.iterrows()):
        pdf.set_font("helvetica", "", 8)
        desc_limpia = f"{limpiar_texto(row['Concepto'])}\n{limpiar_texto(row['Descripción'])}"
        lineas = len(pdf.multi_cell(w_desc - 4, 4, desc_limpia, split_only=True))
        h_fila = max((lineas * 4) + 6, 24)

        if pdf.get_y() + h_fila > 250:
            pdf.add_page()
            pdf.set_y(35)
            # Re-dibujar encabezado
            pdf.set_fill_color(*azul_corp)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("helvetica", "B", 9)
            pdf.cell(w_cant, 10, "CANT", 0, 0, "C", True)
            pdf.cell(w_desc, 10, " DESCRIPCIÓN DEL MATERIAL", 0, 0, "L", True)
            pdf.cell(w_img, 10, "", 0, 1, "C", True)
            pdf.set_text_color(*gris_texto)

        y_antes = pdf.get_y()
        pdf.set_fill_color(252, 252, 252) if i % 2 == 0 else pdf.set_fill_color(*gris_suave)
        pdf.rect(15, y_antes, 180, h_fila, "F")

        pdf.set_font("helvetica", "B", 10)
        pdf.cell(w_cant, h_fila, str(int(row['Pzas'])), 0, 0, "C")
        
        x_desc = pdf.get_x()
        pdf.set_xy(x_desc + 2, y_antes + 4)
        pdf.set_font("helvetica", "B", 9)
        pdf.cell(w_desc - 4, 4, limpiar_texto(row['Concepto']), ln=True)
        pdf.set_x(x_desc + 2)
        pdf.set_font("helvetica", "", 8)
        pdf.multi_cell(w_desc - 4, 3.5, limpiar_texto(row['Descripción']))
        
        pdf.set_xy(x_desc + w_desc, y_antes)
        foto_final = descargar_imagen_para_pdf(dict_fotos.get(idx) or dict_links.get(idx))
        if foto_final:
            try: pdf.image(foto_final, pdf.get_x() + 2, y_antes + 2, w=21)
            except: pass
        
        pdf.set_y(y_antes + h_fila)

    # --- FIRMA ---
    if pdf.get_y() > 220:
        pdf.add_page()
        pdf.set_y(35)
    else:
        pdf.ln(15)
    
    y_f = pdf.get_y()
    pdf.set_xy(15, y_f)
    pdf.line(15, y_f, 100, y_f)
    pdf.set_y(y_f + 2)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(85, 5, "RECIBÍ DE CONFORMIDAD", 0, 1, "C")
    pdf.set_font("helvetica", "", 8)
    pdf.cell(85, 5, "Nombre, Firma y Sello de Recibido", 0, 1, "C")

    resultado = pdf.output(dest='S')
    if isinstance(resultado, str):
        return resultado.encode('latin-1')
    return bytes(resultado)

def generar_pedido_tecnico_blob_v2(cab, df_partidas, datos_fisc, datos_log, datos_oper, detalles_compra={}):
    """Genera el PDF técnico avanzado para administración con los 3 rubros detallados, incluyendo precios de venta y comentarios."""
    pdf = PDF(format="letter", tipo_pdf='PEDIDO')
    pdf.set_auto_page_break(auto=True, margin=30)
    pdf.add_page()
    
    azul_corp = (31, 73, 125)
    gris_suave = (245, 246, 247)
    gris_texto = (60, 60, 60)

    # --- ENCABEZADO SUPERIOR ---
    pdf.set_font("helvetica", "B", 14)
    pdf.set_xy(15, 35)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(100, 7, f"FICHA DE PEDIDO: {limpiar_texto(cab['folio'])}", ln=False)

    fecha_emision = date.today().strftime("%d/%m/%Y")
    pdf.set_font("helvetica", "", 10)
    pdf.set_x(130)
    pdf.cell(65, 5, f"Fecha: {fecha_emision}", ln=True, align='R')
    pdf.set_x(130)
    pdf.cell(65, 5, f"Prioridad: {cab.get('prioridad', 'Normal').upper()}", ln=True, align='R')

    pdf.ln(4)
    
    # --- BLOQUE 1: DATOS DE FACTURACIÓN Y LOGÍSTICA ---
    y_bloques = pdf.get_y()
    pdf.set_fill_color(*gris_suave)
    pdf.rect(15, y_bloques, 90, 42, "F") # Facturación
    pdf.rect(110, y_bloques, 90, 42, "F") # Logística
    
    # Facturación
    pdf.set_xy(18, y_bloques + 3)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*azul_corp)
    pdf.cell(80, 5, "DATOS DE FACTURACIÓN (CLIENTE)", ln=True)
    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(*gris_texto)
    pdf.set_x(18)
    pdf.multi_cell(80, 4, f"Razón Social: {limpiar_texto(datos_fisc['razon_fiscal'])}\nRFC: {limpiar_texto(datos_fisc['rfc'])}\nUso CFDI: {limpiar_texto(datos_fisc['uso_cfdi'])}\nMétodo Pago: {limpiar_texto(datos_fisc['metodo_pago'])}")
    
    # Logística
    pdf.set_xy(113, y_bloques + 3)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*azul_corp)
    pdf.cell(80, 5, "DATOS DE ENTREGA (LOGÍSTICA)", ln=True)
    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(*gris_texto)
    pdf.set_xy(113, y_bloques + 8)
    pdf.multi_cell(80, 4, f"Origen: {limpiar_texto(datos_log.get('origen', 'N/A'))}\nMétodo: {limpiar_texto(datos_log.get('metodo', 'N/A'))}\nDirección: {limpiar_texto(datos_log['dir_entrega'])}\nRecibe: {limpiar_texto(datos_log['persona_recibe'])}\nTel: {limpiar_texto(datos_log['tel_contacto'])}\nMaps: {limpiar_texto(datos_log.get('maps', 'N/A'))}")

    pdf.ln(8)
    
    # --- BLOQUE 2: DATOS DE PROVEEDOR Y OPERACIÓN ---
    y_oper = pdf.get_y() + 5
    pdf.set_fill_color(*gris_suave)
    pdf.rect(15, y_oper, 185, 25, "F")
    
    pdf.set_xy(18, y_oper + 3)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*azul_corp)
    pdf.cell(180, 5, "DATOS DE OPERACIÓN", ln=True)
    
    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(*gris_texto)
    pdf.set_x(18)
    c1, c2, c3 = 60, 60, 60
    # Fila 1 Operación
    pdf.cell(c1, 4, f"Ejecutivo: {limpiar_texto(cab['ejecutivo'])}")
    pdf.cell(c2, 4, f"Arrendamiento: {cab.get('arrendamiento', 'No')}")
    pdf.cell(c3, 4, f"Financiera: {limpiar_texto(cab.get('financiera', 'N/A'))}", ln=True)
    # Fila 2 Operación
    pdf.set_x(18)
    pdf.cell(c1, 4, f"Moneda: {limpiar_texto(cab.get('moneda', 'MXN'))}")
    pdf.cell(c2, 4, f"T. Cambio: {cab.get('tc', 1.0)}")
    pdf.cell(c3, 4, f"Vendedor Prov: {limpiar_texto(datos_oper.get('vendedor', 'N/A'))}", ln=True)

    # --- TABLA DE PRODUCTOS (DESGLOSE COMPLETO) ---
    pdf.set_xy(15, y_oper + 30)
    pdf.set_fill_color(*azul_corp)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 7)
    
    w_cant, w_desc, w_sku, w_prov, w_costo, w_venta, w_util = 8, 65, 25, 25, 20, 20, 22
    pdf.cell(w_cant, 8, "CANT", 0, 0, "C", True)
    pdf.cell(w_desc, 8, " CONCEPTO / DESCRIPCIÓN", 0, 0, "L", True)
    pdf.cell(w_sku, 8, "SKU", 0, 0, "L", True)
    pdf.cell(w_prov, 8, "PROVEEDOR", 0, 0, "L", True)
    pdf.cell(w_costo, 8, "COSTO U.", 0, 0, "R", True)
    pdf.cell(w_venta, 8, "VENTA U.", 0, 0, "R", True)
    pdf.cell(w_util, 8, "UTIL. TOTAL", 0, 1, "R", True)

    pdf.set_text_color(*gris_texto)
    for i, (idx, row) in enumerate(df_partidas.iterrows()):
        pdf.set_font("helvetica", "", 7)
        
        # Info de compra específica de este producto
        det_c = detalles_compra.get(idx, {})
        link_c = det_c.get("link", "N/A")
        cont_c = det_c.get("contacto", "N/A")
        
        desc_limpia = f"{limpiar_texto(row['Concepto'])}\n{limpiar_texto(row['Descripción'])}\n> LINK: {link_c}\n> EJECUTIVO: {cont_c}"
        
        lineas = len(pdf.multi_cell(w_desc - 4, 3.5, desc_limpia, split_only=True))
        h_fila = max((lineas * 3.5) + 4, 12)

        if pdf.get_y() + h_fila > 240:
            pdf.add_page()
            pdf.set_y(35)
            # Re-encabezado
            pdf.set_fill_color(*azul_corp)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("helvetica", "B", 7)
            pdf.cell(w_cant, 8, "CANT", 0, 0, "C", True)
            pdf.cell(w_desc, 8, " CONCEPTO / DESCRIPCIÓN", 0, 0, "L", True)
            pdf.cell(w_sku, 8, "SKU", 0, 0, "L", True)
            pdf.cell(w_prov, 8, "PROVEEDOR", 0, 0, "L", True)
            pdf.cell(w_costo, 8, "COSTO U.", 0, 0, "R", True)
            pdf.cell(w_venta, 8, "VENTA U.", 0, 0, "R", True)
            pdf.cell(w_util, 8, "UTIL. TOTAL", 0, 1, "R", True)
            pdf.set_text_color(*gris_texto)

        y_antes = pdf.get_y()
        pdf.set_fill_color(252, 252, 252) if i % 2 == 0 else pdf.set_fill_color(*gris_suave)
        pdf.rect(15, y_antes, 185, h_fila, "F")

        pdf.set_font("helvetica", "B", 8)
        pdf.cell(w_cant, h_fila, str(int(row['Pzas'])), 0, 0, "C")
        
        x_desc = pdf.get_x()
        pdf.set_xy(x_desc + 2, y_antes + 2)
        pdf.multi_cell(w_desc - 4, 3, desc_limpia)
        
        pdf.set_xy(x_desc + w_desc, y_antes)
        pdf.cell(w_sku, h_fila, limpiar_texto(row.get('SKU', '')))
        pdf.cell(w_prov, h_fila, limpiar_texto(row.get('Proveedor', '')))
        
        costo_u = row.get('Costo (Sub)', 0)
        venta_u = row.get('Venta (Sub)', 0)
        util_t = (venta_u - costo_u - row.get('Envio Sec', 0)) * row['Pzas']
        
        pdf.cell(w_costo, h_fila, f"$ {costo_u:,.2f}", 0, 0, "R")
        pdf.cell(w_venta, h_fila, f"$ {venta_u:,.2f}", 0, 0, "R")
        pdf.set_font("helvetica", "B", 8)
        pdf.cell(w_util, h_fila, f"$ {util_t:,.2f}", 0, 1, "R")
        
        pdf.set_y(y_antes + h_fila)

    # --- TOTALES Y COMENTARIOS ---
    pdf.ln(5)
    if pdf.get_y() > 200: pdf.add_page(); pdf.set_y(35)
    
    y_final = pdf.get_y()
    
    # Comentarios y Términos
    pdf.set_xy(15, y_final)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*azul_corp)
    pdf.cell(100, 5, "COMENTARIOS Y RESUMEN EJECUTIVO", ln=True)
    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(*gris_texto)
    pdf.set_x(15)
    pdf.multi_cell(100, 3.5, limpiar_texto(cab.get('comentarios', 'Sin comentarios adicionales.')))
    
    pdf.ln(2)
    pdf.set_x(15)
    pdf.set_font("helvetica", "B", 8)
    pdf.cell(100, 4, "TÉRMINOS COMERCIALES:", ln=True)
    pdf.set_font("helvetica", "", 7)
    pdf.set_x(15)
    pdf.multi_cell(100, 3, f"Entrega: {limpiar_texto(cab.get('entrega', 'N/A'))}\nPago: {limpiar_texto(cab.get('pago', 'N/A'))}\nCondiciones: {limpiar_texto(cab.get('condiciones', 'N/A'))}")

    # Cuadro de Totales
    total_costo = (df_partidas['Costo (Sub)'] * df_partidas['Pzas']).sum()
    total_venta = (df_partidas['Venta (Sub)'] * df_partidas['Pzas']).sum()
    total_util = total_venta - total_costo - (df_partidas['Envio Sec'] * df_partidas['Pzas']).sum()
    
    pdf.set_xy(125, y_final)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*gris_texto)
    pdf.cell(35, 6, "Total Inversión:", 0, 0)
    pdf.cell(30, 6, f"$ {total_costo:,.2f}", 0, 1, "R")
    pdf.set_x(125)
    pdf.cell(35, 6, "Total Venta (Sub):", 0, 0)
    pdf.cell(30, 6, f"$ {total_venta:,.2f}", 0, 1, "R")
    pdf.set_x(125)
    pdf.set_fill_color(*azul_corp)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(65, 8, f"UTILIDAD: $ {total_util:,.2f}", 0, 1, "C", True)

    pdf.ln(10)
    y_firmas = pdf.get_y()
    pdf.set_draw_color(*gris_texto)
    pdf.set_text_color(*gris_texto)
    
    # Firma Ejecutivo
    pdf.line(25, y_firmas + 15, 90, y_firmas + 15)
    pdf.set_xy(25, y_firmas + 16)
    pdf.set_font("helvetica", "B", 9)
    pdf.cell(65, 5, "SOLICITA (EJECUTIVO)", 0, 0, "C")
    pdf.set_xy(25, y_firmas + 20)
    pdf.set_font("helvetica", "", 8)
    pdf.cell(65, 5, limpiar_texto(cab['ejecutivo']), 0, 0, "C")
    
    # Firma Director
    pdf.set_font("helvetica", "B", 9)
    pdf.line(125, y_firmas + 15, 190, y_firmas + 15)
    pdf.set_xy(125, y_firmas + 16)
    pdf.cell(65, 5, "AUTORIZA (DIRECTOR)", 0, 1, "C")

    resultado = pdf.output(dest='S')
    if isinstance(resultado, str): return resultado.encode('latin-1')
    return bytes(resultado)

def obtener_primera_fila_vacia(ws):
    """Retorna el índice de la primera fila vacía en un worksheet."""
    try:
        valores = ws.col_values(1)
        return len(valores) + 1
    except:
        return 1

# --- 2. FUNCIONES DE APOYO (UI) ---
def trigger_generar_folio():
    """Callback disparado al cambiar ejecutivo o cliente para generar folio instantáneo."""
    ej_nom = st.session_state.get('ej_sel_final')
    cli_rs = st.session_state.get('rs_sel_final')
    
    if ej_nom and cli_rs and ej_nom != "Seleccionar..." and cli_rs != "Seleccionar...":
        # Evitar generar si ya hay un folio (protección para edición)
        if not st.session_state.get('folio_val'):
            # Buscar ID técnico del ejecutivo
            ej_id = next((u['USUARIO'] for u in st.session_state.usuarios_db if u['NOMBRE'] == ej_nom), st.session_state.usuario)
            nuevo_f = generar_folio_automatico(cli_rs, ej_id)
            if nuevo_f:
                st.session_state.folio_val = nuevo_f

def cargar_datos_sesion_usuario():
    """Carga los datos específicos del usuario una vez autenticado."""
    if st.session_state.get('datos_usuario_listos'):
        return

    try:
        with st.spinner("Iniciando sesión en la Nube..."):
            gc = conectar_google_sheets()
            
            # Buscar el registro del usuario actual
            datos_user = next((u for u in st.session_state.usuarios_db if u['USUARIO'] == st.session_state.usuario), None)
            
            if not datos_user:
                st.error(f"No se encontró configuración para el usuario: {st.session_state.usuario}")
                st.stop()
            
            # Abrir su hoja personal (ID_SHEET es el estándar en este proyecto)
            sheet_id = str(datos_user.get('ID_SHEET') or datos_user.get('SPREADSHEET_ID') or "").strip()
            st.session_state.sh_personal = None # Inicializar como None

            if sheet_id and len(sheet_id) > 20:
                try:
                    st.session_state.sh_personal = gc.open_by_key(sheet_id)
                except Exception as e:
                    if st.session_state.rol == "EJECUTIVO":
                        st.error(f"❌ Error al abrir la hoja personal de {st.session_state.usuario}")
                        st.info(f"Verifique permisos: {e}")
                        st.stop()
            else:
                # Intento por nombre solo para EJECUTIVOS si no hay ID
                if st.session_state.rol == "EJECUTIVO":
                    try:
                        st.session_state.sh_personal = gc.open(f"COTIZACIONES_{st.session_state.usuario}")
                    except:
                        try: st.session_state.sh_personal = gc.open(st.session_state.usuario)
                        except:
                            st.error(f"❌ No se encontró hoja personal para {st.session_state.usuario}")
                            st.stop()
            
            # --- CARGA DE DATOS MAESTROS (PROVEEDORES, TERMINOS, DIRECTORIO) ---
            def cargar_maestro(nombre_ws):
                """Lee datos y normaliza las llaves (headers) a MAYÚSCULAS_Y_GUIONES_BAJOS."""
                import unicodedata
                
                def limpiar_llave(txt):
                    if not txt: return ""
                    s = "".join(c for c in unicodedata.normalize('NFD', str(txt)) if unicodedata.category(c) != 'Mn')
                    return s.upper().strip().replace(" ", "_")

                def normalizar_registros(registros):
                    if not registros: return []
                    res = []
                    for r in registros:
                        norm = {limpiar_llave(k): v for k, v in r.items()}
                        # Mapeos especiales para DIRECTORIO
                        if nombre_ws == "DIRECTORIO":
                            if "RAZON_SOCIAL" not in norm:
                                for alias in ["CLIENTE", "EMPRESA", "RAZON_SOCIAL_FISCAL", "NOMBRE_CLIENTE", "NOMBRE", "RS"]:
                                    if alias in norm:
                                        norm["RAZON_SOCIAL"] = norm[alias]
                                        break
                        # Mapeos especiales para TERMINOS
                        if nombre_ws == "TERMINOS":
                            if "CATEGORIA" not in norm:
                                for alias in ["TIPO", "CLASE", "RUBRO"]:
                                    if alias in norm: norm["CATEGORIA"] = norm[alias]; break
                            if "VALOR" not in norm:
                                for alias in ["OPCION", "DESCRIPCION", "TEXTO"]:
                                    if alias in norm: norm["VALOR"] = norm[alias]; break
                        res.append(norm)
                    return res

                # 1. Intentar en Archivo Central (PRIORIDAD PARA TERMINOS)
                if nombre_ws == "TERMINOS":
                    try:
                        sh_m = gc.open_by_key("14W4fYj-9_mAcic2XDDWx1XGNh38xlBCZRFA_AUhrc0s")
                        ws_m = sh_m.worksheet("Hoja 1")
                        vals = ws_m.get_all_values()
                        if vals:
                            res = []
                            for r_idx in range(1, len(vals)):
                                fila = vals[r_idx]
                                d = {}
                                if len(fila) > 0 and fila[0].strip(): d['ENTREGA'] = fila[0].strip()
                                if len(fila) > 1 and fila[1].strip(): d['PAGO'] = fila[1].strip()
                                if len(fila) > 2 and fila[2].strip(): d['CONDICIONES'] = fila[2].strip()
                                if d: res.append(d)
                            if res: return res
                    except: pass

                if nombre_ws == "PROVEEDORES" or (nombre_ws == "TERMINOS" and 'res' not in locals()):
                    # Usar el ID específico proporcionado por el usuario
                    archivos_centrales = [
                        ("1YJWY1C2OYpGypTyYWrXJRIZTU9jFwC_cvBwLNp2aQDE", "MAESTRO_CO5O"),
                        (None, "TERMINOS_Y_CONDICIONES")
                    ]
                    
                    for f_id, f_name in archivos_centrales:
                        try:
                            if f_id: sh_m = gc.open_by_key(f_id)
                            else: sh_m = gc.open(f_name)
                            
                            for ws_name in [nombre_ws, "TERMINOS", "PROVEEDORES", "Hoja 1", "Hoja1", "HOJA 1", "HOJA1", "Sheet1"]:
                                try:
                                    ws_m = sh_m.worksheet(ws_name)
                                    recs = ws_m.get_all_records()
                                    if recs: 
                                        data_norm = normalizar_registros(recs)
                                        if data_norm: return data_norm
                                except: continue
                        except: continue
                    
                    # Si falla todo para TERMINOS, avisar al usuario
                    if nombre_ws == "TERMINOS":
                        st.session_state.error_maestro_terminos = True

                # 2. Intentar en hoja personal
                if st.session_state.sh_personal:
                    pestanas_intento = [nombre_ws]
                    if nombre_ws == "DIRECTORIO":
                        pestanas_intento = ["DIRECTORIO", "Directorio", "PROSPECTOS", "CLIENTES", "Contactos", "CONTACTOS"]
                    
                    for p in pestanas_intento:
                        try:
                            ws = st.session_state.sh_personal.worksheet(p)
                            data = ws.get_all_records()
                            if data: return normalizar_registros(data)
                        except: continue
                
                return []

            st.session_state.directorio = cargar_maestro("DIRECTORIO")
            st.session_state.terminos_db = cargar_maestro("TERMINOS")
            st.session_state.proveedores_db = cargar_maestro("PROVEEDORES")
            st.session_state.datos_fiscales = cargar_maestro("DATOS FISCALES")
                
            st.session_state.datos_usuario_listos = True
    except Exception as e:
        st.error(f"Error al conectar con la base de datos personal: {e}")
        st.stop()

# --- 1. GESTIÓN DE EDICIÓN ---
def cargar_cotizacion_para_editar(row, df_resumen):
    """Extrae toda la lógica de carga de una cotización de forma robusta."""
    # 1. Normalizar nombres de columnas del DataFrame de entrada
    row_norm = {str(k).upper().replace(" ", "_"): v for k, v in row.items()}
    row_list = list(row.values) if hasattr(row, 'values') else list(row)
    
    # 2. Identificar columnas clave (Folio)
    f_id = str(row_norm.get('FOLIO', row_list[0] if len(row_list)>0 else "")).strip()
    
    # 3. Limpieza de estados anteriores
    st.session_state.dict_fotos = {}
    st.session_state.dict_fotos_links = {}
    st.session_state.dict_evidencias = {}
    st.session_state.dict_evidencias_links = {}
    st.session_state.editor_key = st.session_state.get('editor_key', 0) + 1
    
    for k in ['registro_exitoso', 'pedido_exitoso', 'pdf_actual', 'remision_actual', 'pdf_tecnico_actual', 'nombre_pdf']:
        if k in st.session_state: del st.session_state[k]

    # 4. Carga de Cabecera (Sincronización con Widgets)
    st.session_state.folio_val = f_id
    st.session_state.folio_original_edicion = f_id
    
    # Ejecutivo: nombre o posición 1
    ej_val = row_norm.get('EJECUTIVO') or (row_list[1] if len(row_list)>1 else None)
    ej_nom_cargado = str(ej_val) if ej_val else st.session_state.usuario
    st.session_state.ejecutivo_nom = ej_nom_cargado
    st.session_state.ultimo_ejecutivo_folio = ej_nom_cargado
    
    # Cliente: nombre o posición 6
    cli_val = row_norm.get('CLIENTE') or row_norm.get('RAZON_SOCIAL') or (row_list[6] if len(row_list)>6 else None)
    cli_nom_cargado = str(cli_val) if cli_val else 'Seleccionar...'
    st.session_state.cliente_sel = cli_nom_cargado
    st.session_state.ultimo_cliente_folio = cli_nom_cargado
    
    # Contacto: nombre o posición 7
    cont_val = row_norm.get('CONTACTO') or (row_list[7] if len(row_list)>7 else None)
    st.session_state.contacto_sel = str(cont_val) if cont_val else 'Seleccionar...'
    
    # Entrega: nombre o posición 9
    ent_val = row_norm.get('TIEMPO_DE_ENTREGA') or row_norm.get('ENTREGA') or (row_list[9] if len(row_list)>9 else None)
    st.session_state.entrega_val = str(ent_val) if ent_val else 'Seleccionar...'
    
    # Pago: nombre o posición 10
    pag_val = row_norm.get('FORMA_DE_PAGO') or row_norm.get('PAGO') or (row_list[10] if len(row_list)>10 else None)
    st.session_state.pago_val = str(pag_val) if pag_val else 'Seleccionar...'
    
    # Condiciones: nombre o posición 11
    con_val = row_norm.get('CONDICIONES_ESPECIALES') or row_norm.get('CONDICIONES') or (row_list[11] if len(row_list)>11 else None)
    st.session_state.condic_val = str(con_val) if con_val else 'Seleccionar...'
    
    # Comentarios: nombre o posición 12
    com_val = row_norm.get('COMENTARIOS') or row_norm.get('RESUMEN') or (row_list[12] if len(row_list)>12 else None)
    st.session_state.coment_val = str(com_val) if com_val else ""
    
    # Moneda y TC: nombre o posiciones 14 y 15
    mon_val = row_norm.get('MONEDA') or (row_list[14] if len(row_list)>14 else 'MXN')
    st.session_state.moneda_val = str(mon_val)
    
    tc_val = row_norm.get('TC') or row_norm.get('TIPO_DE_CAMBIO') or (row_list[15] if len(row_list)>15 else 1.0)
    try: st.session_state.tc_val = float(tc_val)
    except: st.session_state.tc_val = 1.0

    # Estatus: nombre o posición 13
    est_val = row_norm.get('ESTATUS') or (row_list[13] if len(row_list)>13 else '60% Propuesta')
    st.session_state.estatus_val = str(est_val)

    # Último Contacto: nombre o posición 16 (Columna Q)
    cont_date_val = row_norm.get('ULTIMO_CONTACTO') or (row_list[16] if len(row_list)>16 else None)
    try:
        val_cd = str(cont_date_val) if cont_date_val else ""
        st.session_state.ultimo_contacto_val = datetime.strptime(val_cd, "%Y-%m-%d").date() if "-" in val_cd else date.today()
    except:
        st.session_state.ultimo_contacto_val = date.today()

    try:
        vig_val = row_norm.get('VIGENCIA') or (row_list[8] if len(row_list)>8 else None)
        val_v = str(vig_val) if vig_val else ""
        st.session_state.vigencia_val = datetime.strptime(val_v, "%Y-%m-%d").date() if "-" in val_v else date.today()
    except:
        st.session_state.vigencia_val = date.today()

    # 5. Carga de Partidas (Detalle)
    try:
        ws_det = st.session_state.sh_personal.worksheet("COTIZACIONES_DETALLE")
        todas_p = ws_det.get_all_records()
        
        # Filtrar partidas por folio (normalizando ambos para comparar)
        partidas = [p for p in todas_p if str(next(iter(p.values()), "")).strip() == f_id]
        
        if partidas:
            df_edit = pd.DataFrame(partidas)
            # Normalizar columnas del detalle
            mapa_cols = {
                "CONCEPTO": "Concepto", "DESCRIPCION": "Descripción", "PZAS": "Pzas", "CANTIDAD": "Pzas",
                "SKU": "SKU", "FOLIO_PROVEEDOR": "Folio Prov", "PRECIO_PROVEEDOR": "PM",
                "PROVEEDOR": "Proveedor", "LINK": "Link", "ENVIO_PROVEEDOR": "Envio Prov",
                "ENVIO_SECUNDARIO": "Envio Sec", "UTILIDAD%": "Util %", "FOTO_LINK": "Foto_Link",
                "FINANCIAMIENTO": "Financiamiento", "FINANCIERA": "Financiera", "EVIDENCIA_LINK": "Evidencia_Link",
                "MONEDA_ITEM": "Moneda"
            }
            # Renombrar usando una búsqueda insensible
            new_cols = {}
            for c in df_edit.columns:
                c_norm = str(c).upper().replace(" ", "_")
                if c_norm in mapa_cols:
                    new_cols[c] = mapa_cols[c_norm]
            
            df_edit = df_edit.rename(columns=new_cols)
            
            # Asegurar columnas mínimas para el editor
            cols_necesarias = ["Tipo", "Moneda", "Concepto", "Descripción", "Pzas", "SKU", "PM", "Proveedor", "Folio Prov", "Link", "Envio Prov", "Envio Sec", "Util %", "Financiamiento", "Financiera"]
            for c in cols_necesarias:
                if c not in df_edit.columns:
                    if c == "Tipo": df_edit[c] = "PARTIDA"
                    elif c == "Moneda": df_edit[c] = "MXN"
                    elif c == "Util %": df_edit[c] = 15.0
                    elif c == "Pzas": df_edit[c] = 1
                    elif c in ["PM", "Envio Prov", "Envio Sec"]: df_edit[c] = 0.0
                    elif c == "Financiamiento": df_edit[c] = "Sin Financiera"
                    elif c == "Financiera": df_edit[c] = "N/A"
                    else: df_edit[c] = ""

            # Carga de links para visualización
            if "Foto_Link" in df_edit.columns:
                st.session_state.dict_fotos_links = {idx: r["Foto_Link"] for idx, r in df_edit.iterrows() if r["Foto_Link"]}
            if "Evidencia_Link" in df_edit.columns:
                st.session_state.dict_evidencias_links = {idx: r["Evidencia_Link"] for idx, r in df_edit.iterrows() if r["Evidencia_Link"]}

            # Normalizar Margen % (de decimal 0.15 a 15.0)
            if "Util %" in df_edit.columns:
                df_edit["Util %"] = pd.to_numeric(df_edit["Util %"], errors='coerce').fillna(0)
                if df_edit["Util %"].max() <= 1.0: df_edit["Util %"] = (df_edit["Util %"] * 100).round(1)
            
            st.session_state.df_partidas = df_edit[cols_necesarias]
    except Exception as e:
        st.error(f"Error cargando detalle: {e}")

# --- 3. BUSCADOR DE VÍNCULOS Y OPERACIONES (OVO) ---
def buscar_en_todos_los_sheets(query):
    """Busca coincidencias ignorando acentos, puntuación y mayúsculas."""
    import unicodedata
    def normalizar(t):
        if not t: return ""
        return "".join(c for c in unicodedata.normalize('NFD', str(t)) if unicodedata.category(c) != 'Mn').upper()

    q_norm = normalizar(query)
    resultados = []
    resumen_escaneo = []
    
    with st.spinner("Escaneando ecosistema de proyectos..."):
        for u in st.session_state.usuarios_db:
            nombre_ej = u.get('NOMBRE', 'Ejecutivo')
            sheet_id = str(u.get('ID_SHEET') or u.get('SPREADSHEET_ID') or "").strip()
            
            try:
                # USAR CACHÉ PARA EL ESCANEO
                datos_dir = obtener_directorio_ejecutivo_cached(sheet_id, u['USUARIO'])
                
                if datos_dir:
                    resumen_escaneo.append(f"✅ {nombre_ej}: {len(datos_dir)} reg.")
                    for d in datos_dir:
                        toda_la_fila = " ".join([str(v) for v in d.values()])
                        if q_norm in normalizar(toda_la_fila):
                            d_norm = {str(k).upper().replace(" ", "_"): v for k, v in d.items()}
                            res_final = {
                                'RAZON_SOCIAL': d_norm.get('RAZON_SOCIAL', d_norm.get('CLIENTE', 'N/A')),
                                'CONTACTO': d_norm.get('CONTACTO', d_norm.get('ATENCION', 'N/A')),
                                'EMAIL': d_norm.get('EMAIL', 'N/A'),
                                'TELEFONO': d_norm.get('TELEFONO', 'N/A'),
                                'EJECUTIVO_DUEÑO': nombre_ej
                            }
                            # No buscamos última actividad aquí para ahorrar cuota
                            res_final['ULTIMA_ACTIVIDAD'] = "Ver en expediente"
                            resultados.append(res_final)
                else:
                    resumen_escaneo.append(f"❌ {nombre_ej}: Sin acceso/datos")

            except Exception:
                resumen_escaneo.append(f"❌ {nombre_ej}: Error en proceso")
                continue 
    
    with st.expander("Detalle del escaneo (Diagnóstico)"):
        st.write(" | ".join(resumen_escaneo))
    
    return resultados

def renderizar_buscador_ovo():
    st.title("🔎 Buscador de Vínculos y Operaciones")
    st.info("Escribe el nombre de la empresa o contacto para verificar disponibilidad.")
    
    col_s1, col_s2 = st.columns([3, 1])
    with col_s1:
        query = st.text_input("Empresa o Contacto a buscar:", placeholder="Ej. FEMSA, Juan Pérez...")
    with col_s2:
        st.write("")
        st.write("")
        btn_buscar = st.button("BUSCAR AHORA", use_container_width=True, type="primary")

    if btn_buscar and query:
        encontrados = buscar_en_todos_los_sheets(query)
        if not encontrados:
            st.success(f"✅ No se encontraron vínculos para '{query}'. El prospecto parece estar LIBRE.")
        else:
            st.warning(f"⚠️ Se encontraron {len(encontrados)} coincidencias en el ecosistema.")
            rol = st.session_state.rol
            for res in encontrados:
                with st.expander(f"Resultado: {res.get('RAZON_SOCIAL', 'N/A')}"):
                    if rol == "EJECUTIVO":
                        st.error("❌ ESTE CLIENTE YA ESTÁ SIENDO ATENDIDO.")
                        st.markdown("**Para más información, contactar con el administrador o Dirección.**")
                    elif rol == "OPERACIONES":
                        st.subheader("Información de Vínculo")
                        c1, c2 = st.columns(2)
                        c1.write(f"**Atendido por:** {res.get('EJECUTIVO_DUEÑO')}")
                        c2.write(f"**Última Actividad:** {res.get('ULTIMA_ACTIVIDAD')}")
                    elif rol == "DIRECCION":
                        st.subheader("Expediente Completo (Visión Dirección)")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.write(f"**Empresa:** {res.get('RAZON_SOCIAL')}")
                            st.write(f"**Ejecutivo:** {res.get('EJECUTIVO_DUEÑO')}")
                        with c2:
                            st.write(f"**Contacto:** {res.get('CONTACTO')}")
                            st.write(f"**Email:** {res.get('EMAIL')}")
                        with c3:
                            st.write(f"**Teléfono:** {res.get('TELEFONO')}")
                            st.write(f"**Última Fecha:** {res.get('ULTIMA_ACTIVIDAD')}")
                        st.divider()
                        st.json(res)

def renderizar_dashboard_operaciones():
    st.title("📊 Dashboard de Sales Operation Support")
    st.markdown("### Análisis de Leads y Contactos del Ecosistema")
    
    # 1. Análisis Global de Contactos por Ejecutivo
    resumen_ejecutivos = []
    total_leads_salvador = 0
    leads_asignados = 0
    
    with st.spinner("Analizando bases de datos..."):
        for u in st.session_state.usuarios_db:
            nombre_ej = u.get('NOMBRE', 'Ejecutivo')
            usuario_id = u['USUARIO']
            sheet_id = str(u.get('ID_SHEET') or u.get('SPREADSHEET_ID') or "").strip()
            
            # Cargar directorio del ejecutivo
            datos_dir = obtener_directorio_ejecutivo_cached(sheet_id, usuario_id)
            if datos_dir:
                df_dir = pd.DataFrame(datos_dir)
                df_dir.columns = [str(c).upper().replace(" ", "_") for c in df_dir.columns]
                
                num_contactos = len(df_dir)
                
                # Calcular días promedio de contacto si existe la columna
                dias_promedio = 0
                if 'FECHA_ULTIMO_CONTACTO' in df_dir.columns or 'ULTIMA_FECHA' in df_dir.columns:
                    col_f = 'FECHA_ULTIMO_CONTACTO' if 'FECHA_ULTIMO_CONTACTO' in df_dir.columns else 'ULTIMA_FECHA'
                    def diff_dias(x):
                        try:
                            f = pd.to_datetime(x).date()
                            return (date.today() - f).days
                        except: return None
                    dias = df_dir[col_f].apply(diff_dias).dropna()
                    dias_promedio = dias.mean() if not dias.empty else 0

                resumen_ejecutivos.append({
                    "Ejecutivo": nombre_ej,
                    "Contactos": num_contactos,
                    "Días Prom. Seguimiento": round(dias_promedio, 1)
                })

                # Si es la base de Salvador, contar leads y asignaciones
                if usuario_id == "SALVADOR_LAMEGOS":
                    total_leads_salvador = num_contactos
                    if 'EJECUTIVO_ASIGNADO' in df_dir.columns:
                        leads_asignados = df_dir[df_dir['EJECUTIVO_ASIGNADO'].notstr.strip() != ""].shape[0]
                    elif 'ASIGNADO' in df_dir.columns:
                        leads_asignados = df_dir[df_dir['ASIGNADO'].astype(str).str.len() > 2].shape[0]

    # 2. Visualización de KPIs
    k1, k2, k3, k4 = st.columns(4)
    with k1: st.metric("Leads Generados (Salvador)", total_leads_salvador)
    with k2: st.metric("Leads Asignados", leads_asignados)
    with k3: 
        porcentaje = (leads_asignados/total_leads_salvador*100) if total_leads_salvador > 0 else 0
        st.metric("% de Asignación", f"{porcentaje:.1f}%")
    with k4:
        total_contactos_global = sum([r['Contactos'] for r in resumen_ejecutivos])
        st.metric("Contactos Globales", total_contactos_global)

    st.divider()
    
    c_op1, c_op2 = st.columns([2, 1])
    
    with c_op1:
        st.subheader("Concentración de Contactos por Ejecutivo")
        df_resumen = pd.DataFrame(resumen_ejecutivos)
        if not df_resumen.empty:
            fig_op = px.bar(df_resumen, x='Ejecutivo', y='Contactos', color='Días Prom. Seguimiento',
                           title="Volumen vs Tiempos de Respuesta",
                           color_continuous_scale=px.colors.sequential.Reds)
            st.plotly_chart(fig_op, use_container_width=True)
    
    with c_op2:
        st.subheader("Ranking de Seguimiento")
        if not df_resumen.empty:
            st.dataframe(df_resumen.sort_values('Días Prom. Seguimiento', ascending=False), hide_index=True)

    st.divider()
    st.info("💡 Salvador: Puedes usar el 'Buscador de Vínculos' en el menú superior para analizar si un nuevo lead ya está en la base de algún ejecutivo.")

# --- ACTIVACIÓN DE RECEPCIÓN DE DRIVE (OAUTH) ---
# Esta función debe correr antes de que Streamlit detenga el flujo por el Login
procesar_callback_oauth()

# Asegurar que la base de usuarios esté cargada siempre (evita AttributeError en refrescos de sesión)
cargar_usuarios_login()

# --- LÓGICA DE LOGIN ---
if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    
    # Contenedor de Login Centrado - Columnas más anchas a los lados para achicar el centro
    _, col_l2, _ = st.columns([1.5, 1, 1.5]) 
    with col_l2:
        st.write("") 
        st.write("")
        if os.path.exists("LOGOPURO.png"):
            # Logo al 50% de ancho del contenedor central
            _, mid_logo, _ = st.columns([1, 2, 1])
            with mid_logo:
                st.image("LOGOPURO.png", use_container_width=True)
        else:
            st.title("CO5O")
        
        with st.form("form_login"):
            st.markdown("<h3 style='text-align: center; color: #1F497D;'>Acceso al Sistema</h3>", unsafe_allow_html=True)
            if 'usuarios_db' in st.session_state:
                user_list = ["Seleccionar..."] + [u['USUARIO'] for u in st.session_state.usuarios_db]
                user_input = st.selectbox("Usuario:", user_list)
                pass_input = st.text_input("Contraseña:", type="password")
                
                if st.form_submit_button("INGRESAR", use_container_width=True):
                    datos_user = next((u for u in st.session_state.usuarios_db if u['USUARIO'] == user_input), None)
                    if datos_user and str(datos_user['PASSWORD']) == pass_input:
                        st.session_state.autenticado = True
                        st.session_state.usuario = user_input
                        # CARGAR EL ROL (Default: EJECUTIVO)
                        st.session_state.rol = str(datos_user.get('ROL', 'EJECUTIVO')).upper()
                        st.rerun()
                    else:
                        st.error("Credenciales incorrectas")
        
        # Logo discreto al final (reducido)
        st.write("")
        if os.path.exists("CONSULTINGLOGO.png"):
            _, sc2, _ = st.columns([1.2, 1, 1.2])
            with sc2:
                st.image("CONSULTINGLOGO.png", use_container_width=True)
    st.stop()
else:
    # --- MOSTRAR BANNER DINÁMICO ---
    def obtener_banner_dinamico():
        ahora = ahora_mexico()
        hora = ahora.hour
        if 6 <= hora < 12: prefijo = "DIA"
        elif 12 <= hora < 19: prefijo = "TARDE"
        else: prefijo = "NOCHE"
        
        # Intentar banner específico por horario, si no el general
        for b in [f"BANNER_{prefijo}.png", "BANNER_TOP.png"]:
            if os.path.exists(b): return b
        return None

    banner = obtener_banner_dinamico()
    if banner:
        st.image(banner, use_container_width=True)
    
    # --- ACTIVACIÓN DE CARGA POST-LOGIN ---
    cargar_datos_sesion_usuario()

    if st.session_state.get('datos_usuario_listos'):
        # --- GESTIÓN DE NAVEGACIÓN ---
        if 'menu_actual' not in st.session_state:
            st.session_state.menu_actual = 'menu'

        # --- BARRA DE NAVEGACIÓN SUPERIOR ---
        with st.container():
            col_nav1, col_nav2, col_nav3, col_nav4 = st.columns([1, 2, 1, 1])
            with col_nav1:
                if st.session_state.menu_actual != 'menu':
                    if st.button("MENU PRINCIPAL", use_container_width=True):
                        st.session_state.menu_actual = 'menu'
                        st.rerun()
                else:
                    st.write("")

            with col_nav2:
                nombre_ej = next((u['NOMBRE'] for u in st.session_state.usuarios_db if u['USUARIO'] == st.session_state.usuario), st.session_state.usuario)
                st.markdown(f"<p style='text-align: center; font-size: 16px; margin-top: 5px; color: #3498DB;'><b>EJECUTIVO: {nombre_ej.upper()}</b></p>", unsafe_allow_html=True)

            with col_nav3:
                # Mostrar fecha actual en lugar de hora (que suele fallar por zona horaria en la nube)
                meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
                ahora = ahora_mexico()
                fecha_str = f"{ahora.day} de {meses[ahora.month-1]}, {ahora.year}"
                st.markdown(f"<p style='text-align: center; font-size: 16px; margin-top: 5px; color: #94A3B8;'>{fecha_str}</p>", unsafe_allow_html=True)

            with col_nav4:
                if st.button("CERRAR SESION", use_container_width=True):
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.rerun()

        # Solo pedir vinculación de Drive a EJECUTIVOS
        if st.session_state.rol == "EJECUTIVO":
            if not obtener_drive_service():
                st.warning("⚠️ CONEXIÓN DE DRIVE PENDIENTE")
                autenticar_usuario_oauth()
                st.divider()

def renderizar_gestion_pedidos_central():
    st.title("📦 Centro de Gestión de Pedidos (Administración)")
    st.info("Visualización y control de pedidos de todo el ecosistema CO5O.")

    try:
        gc = conectar_google_sheets()
        sh_p = gc.open_by_key(ID_SHEET_PEDIDOS)
        ws_p = sh_p.sheet1
        data_p = ws_p.get_all_records()
        df_p = pd.DataFrame(data_p)
        
        if df_p.empty:
            st.info("No hay pedidos registrados en el sistema central.")
            return

        # --- FILTROS SUPERIORES ---
        c_f1, c_f2, c_f3 = st.columns(3)
        with c_f1:
            ej_lista = ["Todos"] + sorted(list(df_p['EJECUTIVO'].unique()))
            ej_sel = st.selectbox("Filtrar por Ejecutivo:", ej_lista)
        with c_f2:
            st_lista = ["Todos"] + sorted(list(df_p['ESTATUS'].unique()))
            st_sel = st.selectbox("Filtrar por Estatus:", st_lista)
        with c_f3:
            busqueda = st.text_input("Buscar por Folio o Cliente:")

        # Aplicar Filtros
        if ej_sel != "Todos": df_p = df_p[df_p['EJECUTIVO'] == ej_sel]
        if st_sel != "Todos": df_p = df_p[df_p['ESTATUS'] == st_sel]
        if busqueda:
            df_p = df_p[df_p['FOLIO'].astype(str).str.contains(busqueda, case=False) | df_p['CLIENTE'].astype(str).str.contains(busqueda, case=False)]

        # --- KPIs RÁPIDOS ---
        st.divider()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Pedidos Totales", len(df_p))
        k2.metric("Nuevos", len(df_p[df_p['ESTATUS'] == "PEDIDO NUEVO"]))
        monto_total_val = pd.to_numeric(df_p['MONTO_TOTAL'], errors='coerce').sum()
        k3.metric("Monto en Gestión", f"$ {monto_total_val:,.2f}")
        k4.metric("Ejecutivos Activos", len(df_p['EJECUTIVO'].unique()))

        st.divider()

        # --- LISTA DE PEDIDOS ---
        for i, row in df_p.iloc[::-1].iterrows():
            folio = row.get('FOLIO', 'N/A')
            cliente = row.get('CLIENTE', 'N/A')
            monto = row.get('MONTO_TOTAL', 0)
            estatus = row.get('ESTATUS', 'N/A')
            ejecutivo = row.get('EJECUTIVO', 'N/A')
            
            with st.expander(f"📄 {folio} | {cliente} | {ejecutivo} | ${monto:,.2f} | {estatus}"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Datos Fiscales**")
                    st.write(f"RFC: {row.get('RFC', 'N/A')}")
                    st.write(f"RS: {row.get('RAZON_SOCIAL_FISCAL', 'N/A')}")
                    st.write(f"Uso: {row.get('USO_CFDI', 'N/A')}")
                    st.write(f"Método: {row.get('METODO_PAGO', 'N/A')}")
                
                with c2:
                    st.markdown("**Logística**")
                    st.write(f"Origen: {row.get('ORIGEN_ENTREGA', 'N/A')}")
                    st.write(f"Método: {row.get('METODO_ENVIO', 'N/A')}")
                    st.write(f"Dirección: {row.get('DIRECCION_ENTREGA', 'N/A')}")
                    st.write(f"Recibe: {row.get('PERSONA_RECIBE', 'N/A')}")
                    st.write(f"Tel: {row.get('TEL_CONTACTO', 'N/A')}")
                    maps = row.get('LINK_MAPS', '')
                    if maps and str(maps).startswith("http"):
                        st.link_button("📍 VER UBICACIÓN MAPS", maps, use_container_width=True)
                
                with c3:
                    st.markdown("**Pago y Crédito**")
                    st.write(f"Tipo: {row.get('TIPO_PAGO', 'N/A')}")
                    if row.get('DIAS_CREDITO') != "N/A": st.write(f"Días: {row.get('DIAS_CREDITO')}")
                    if row.get('VIGENCIA_FINANCIAMIENTO') != "N/A": st.write(f"Vigencia: {row.get('VIGENCIA_FINANCIAMIENTO')}")
                    st.write(f"Financiera: {row.get('FINANCIERA', 'N/A')}")

                st.divider()
                
                # --- BOTONES DE DOCUMENTOS ---
                st.markdown("**Expediente Digital**")
                bd1, bd2, bd3, bd4 = st.columns(4)
                links = {
                    "TÉCNICO": row.get('PDF_TECNICO'),
                    "PAGO / OC": row.get('COMPROBANTE_RESPALDO'),
                    "CSF": row.get('CONSTANCIA_FISCAL'),
                    "ARREND.": row.get('ARRENDAMIENTO_PROPUESTA')
                }
                
                for b_idx, (label, link) in enumerate(links.items()):
                    with [bd1, bd2, bd3, bd4][b_idx]:
                        if link and str(link).startswith("http"):
                            st.link_button(f"📄 ABRIR {label}", link, use_container_width=True)
                        else:
                            st.button(f"🚫 SIN {label}", disabled=True, use_container_width=True, key=f"dis_{folio}_{label}")

                st.divider()
                
                # --- GESTIÓN DE ESTATUS ---
                c_st1, c_st2 = st.columns([2, 1])
                with c_st1:
                    nuevo_estatus = st.selectbox("Cambiar Estatus del Pedido:", 
                                               ["PEDIDO NUEVO", "EN REVISIÓN", "VISTO BUENO", "FACTURADO", "EN RUTA / PAQUETERÍA", "ENTREGADO", "ERROR EN DATOS"], 
                                               index=0, key=f"st_sel_{folio}")
                with c_st2:
                    st.write("")
                    if st.button("ACTUALIZAR ESTATUS", key=f"btn_st_{folio}", use_container_width=True, type="primary"):
                        try:
                            # Buscar fila por Folio para actualizar estatus
                            folios_col = ws_p.col_values(2) # Columna B es Folio
                            if str(folio) in folios_col:
                                fila_idx = folios_col.index(str(folio)) + 1
                                headers = ws_p.row_values(1)
                                col_st_idx = headers.index("ESTATUS") + 1
                                ws_p.update_cell(fila_idx, col_st_idx, nuevo_estatus)
                                st.success(f"Estatus de {folio} actualizado a {nuevo_estatus}")
                                time.sleep(1)
                                st.rerun()
                        except Exception as e_st:
                            st.error(f"Error al actualizar: {e_st}")

    except Exception as e:
        st.error(f"Error al cargar gestión de pedidos: {e}")

# --- NAVEGACIÓN DE VISTAS ---
if st.session_state.menu_actual == 'ovo':
    renderizar_buscador_ovo()
elif st.session_state.menu_actual == 'gestion_pedidos':
    renderizar_gestion_pedidos_central()

elif st.session_state.menu_actual == 'menu':
    if st.session_state.rol == "OPERACIONES":
        # --- VISTA PARA SALES OPERATION SUPPORT (ADMIN_OP) ---
        st.title("Panel Administrativo")
        col_acc1, col_acc2, _ = st.columns([1, 1, 1])
        with col_acc1:
            if st.button("📦 GESTIÓN DE PEDIDOS", use_container_width=True, type="primary"):
                st.session_state.menu_actual = 'gestion_pedidos'
                st.rerun()
        with col_acc2:
            if st.button("🔎 BUSCADOR OVO", use_container_width=True):
                st.session_state.menu_actual = 'ovo'
                st.rerun()
        st.divider()
        st.info("Bienvenido. Selecciona una opción para gestionar los pedidos del ecosistema.")
    
    else:
        # --- VISTA PARA EJECUTIVOS Y DIRECCIÓN ---
        st.title(f"Panel de Control - {st.session_state.usuario}")
        col_acc1, col_acc2, col_acc3 = st.columns([1, 1, 1])
        with col_acc1:
            if st.button("Crear Cotización Nueva", use_container_width=True, type="primary"):
                st.session_state.menu_actual = 'nuevo'
                st.rerun()
        with col_acc2:
            if st.button("Buscador OVO", use_container_width=True):
                st.session_state.menu_actual = 'ovo'
                st.rerun()
        
        # Botón extra solo para DIRECCION
        if st.session_state.rol == "DIRECCION":
            with col_acc3:
                if st.button("📦 GESTIÓN CENTRAL", use_container_width=True):
                    st.session_state.menu_actual = 'gestion_pedidos'
                    st.rerun()
        
        st.divider()

        # 2. CÁLCULO DE MÉTRICAS (KPIs) e HISTORIAL
        if st.session_state.sh_personal is None:
            st.info("💡 Su cuenta no tiene una hoja de cotizaciones personales vinculada. Utilice los botones de navegación superiores para gestionar el sistema.")
        else:
            try:
                ws_res = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
                df_resumen = pd.DataFrame(ws_res.get_all_records())
            
                ws_det = st.session_state.sh_personal.worksheet("COTIZACIONES_DETALLE")
                df_det_all = pd.DataFrame(ws_det.get_all_records())
            
                if not df_resumen.empty:
                    col_folio = 'FOLIO' if 'FOLIO' in df_resumen.columns else df_resumen.columns[0]
                    col_cliente = 'CLIENTE' if 'CLIENTE' in df_resumen.columns else ('RAZON_SOCIAL' if 'RAZON_SOCIAL' in df_resumen.columns else df_resumen.columns[6])
                    
                    # Normalizar nombres de columnas del detalle para el cálculo
                    df_det_norm = df_det_all.copy()
                    df_det_norm.columns = [c.upper().replace(" ", "_") for c in df_det_norm.columns]
                    col_folio_det = df_det_norm.columns[0]

                    # Identificar columnas de monto y utilidad de forma robusta
                    col_monto_src = next((c for c in df_det_norm.columns if "VENTA_TOTAL" in c or "PFACTURA" in c), df_det_norm.columns[-3])
                    # Búsqueda muy flexible para Utilidad
                    col_util_src = next((c for c in df_det_norm.columns if "UTILIDAD" in c or "UTIL_$" in c), None)
                    
                    # Si no encuentra por nombre, usar el índice típico de la columna T (índice 19)
                    if not col_util_src:
                        col_util_src = df_det_norm.columns[19] if len(df_det_norm.columns) > 19 else df_det_norm.columns[-1]

                    def clean_num(x):
                        if isinstance(x, str):
                            # Eliminar $, comas y espacios para convertir a número puro
                            limpio = x.replace("$", "").replace(",", "").replace(" ", "").strip()
                            return pd.to_numeric(limpio, errors='coerce')
                        return pd.to_numeric(x, errors='coerce')

                    df_montos_util = df_det_norm.groupby(col_folio_det).agg({
                        col_monto_src: lambda x: x.apply(clean_num).sum(),
                        col_util_src: lambda x: x.apply(clean_num).sum()
                    }).reset_index()
                    df_montos_util.columns = [col_folio, 'MONTO_TOTAL', 'UTILIDAD_TOTAL']

                    df_stats = pd.merge(df_resumen, df_montos_util, on=col_folio, how='left').fillna(0)

                    # 2. Definir universos
                    estatus_cerrados = ["100% Ganada", "0% Cancelada", "100% Pedido"]
                    df_activos_stats = df_stats[~df_stats['ESTATUS'].isin(estatus_cerrados)]
                    df_cerrados_stats = df_stats[df_stats['ESTATUS'].isin(estatus_cerrados)]

                    monto_activo = df_activos_stats['MONTO_TOTAL'].sum()
                    util_activa = df_activos_stats['UTILIDAD_TOTAL'].sum()
                    monto_ganado = df_stats[df_stats['ESTATUS'].isin(["100% Ganada", "100% Pedido"])]['MONTO_TOTAL'].sum()

                    # --- NUEVO: CÁLCULO DE DÍAS SIN CONTACTO ---
                    def calcular_dias(fecha_str):
                        try:
                            f = datetime.strptime(str(fecha_str), "%Y-%m-%d").date()
                            return (date.today() - f).days
                        except: return 0

                    df_activos_stats['DIAS_SIN_CONTACTO'] = df_activos_stats['ULTIMO_CONTACTO'].apply(calcular_dias) if 'ULTIMO_CONTACTO' in df_activos_stats.columns else 0

                    # --- BLOQUE DE INTELIGENCIA VISUAL (NUEVO) ---
                    st.markdown("### 🚀 Vista Rápida")

                    # 1. Tarjetas de Proyectos Recientes
                    df_recientes = df_stats.iloc[::-1].head(3)
                    c_rec = st.columns(3)
                    for idx_r, (_, row_r) in enumerate(df_recientes.iterrows()):
                        with c_rec[idx_r]:
                            color_st = "#3498DB" if "Propuesta" in row_r['ESTATUS'] else ("#2ECC71" if "Ganada" in row_r['ESTATUS'] else "#94A3B8")
                            st.markdown(f"""
                            <div style='background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 15px;'>
                                <p style='margin:0; font-size: 12px; color: #94A3B8;'>{row_r[col_folio]}</p>
                                <p style='margin:0; font-size: 16px; font-weight: bold;'>{row_r[col_cliente][:20]}...</p>
                                <p style='margin:0; font-size: 14px; color: {color_st}; font-weight: bold;'>{row_r['ESTATUS']}</p>
                                <p style='margin:0; font-size: 18px; margin-top: 10px;'>$ {row_r['MONTO_TOTAL']:,.2f}</p>
                            </div>
                            """, unsafe_allow_html=True)

                    st.write("")

                    # --- NUEVO: ALERTA DE SEGUIMIENTO ---
                    if 'DIAS_SIN_CONTACTO' in df_activos_stats.columns and not df_activos_stats.empty:
                        st.markdown("### ⚠️ Alerta de Seguimiento (Sin contacto)")
                        df_alertas = df_activos_stats.sort_values('DIAS_SIN_CONTACTO', ascending=False).head(3)
                        c_al = st.columns(3)
                        for idx_al, (_, row_al) in enumerate(df_alertas.iterrows()):
                            with c_al[idx_al]:
                                dias = row_al['DIAS_SIN_CONTACTO']
                                color_al = "#E74C3C" if dias > 15 else ("#F39C12" if dias > 7 else "#2ECC71")
                                st.markdown(f"""
                                <div style='background: rgba(231, 76, 60, 0.05); border: 1px solid {color_al}; border-radius: 10px; padding: 10px;'>
                                    <p style='margin:0; font-size: 11px; color: #94A3B8;'>{row_al[col_folio]}</p>
                                    <p style='margin:0; font-size: 14px; font-weight: bold;'>{row_al[col_cliente][:18]}</p>
                                    <p style='margin:0; font-size: 13px; color: {color_al}; font-weight: bold;'>Hace {dias} días</p>
                                    <p style='margin:0; font-size: 11px;'>Último: {row_al.get('ULTIMO_CONTACTO', 'N/A')}</p>
                                </div>
                                """, unsafe_allow_html=True)
                        st.write("")

                    # 2. Gráfico de Pipeline (Tubería)
                    col_g1, col_g2 = st.columns([2, 1])
                    with col_g1:
                        # Agrupar por estatus para el gráfico
                        df_pipe = df_stats.groupby('ESTATUS')['MONTO_TOTAL'].sum().reset_index()
                        # Ordenar por importancia de estatus (aproximado)
                        orden_estatus = ["10% Prospecto", "30% Levantamiento", "60% Propuesta", "90% Negociación", "100% Ganada", "100% Pedido", "0% Cancelada"]
                        df_pipe['ESTATUS'] = pd.Categorical(df_pipe['ESTATUS'], categories=orden_estatus, ordered=True)
                        df_pipe = df_pipe.sort_values('ESTATUS')

                        fig = px.bar(df_pipe, x='ESTATUS', y='MONTO_TOTAL', 
                                    title="Distribución de la Tubería (Monto)",
                                    color='ESTATUS',
                                    color_discrete_sequence=px.colors.sequential.Blues_r)

                        fig.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font_color='#E2E8F0',
                            xaxis_title="",
                            yaxis_title="Monto ($)",
                            showlegend=False,
                            height=350
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with col_g2:
                        # Gráfico de dona: Proporción de clientes
                        df_pie = df_stats[col_cliente].value_counts().head(5).reset_index()
                        df_pie.columns = ['Cliente', 'Cant']
                        fig_pie = px.pie(df_pie, values='Cant', names='Cliente', 
                                    title="Top 5 Clientes",
                                    hole=0.6,
                                    color_discrete_sequence=px.colors.sequential.Aggrnyl)
                        fig_pie.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font_color='#E2E8F0',
                            showlegend=False,
                            height=350
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)

                    st.subheader("Tu Inteligencia de Negocio")
                    m1, m2, m3, m4 = st.columns(4)
                    with m1: st.metric("Ventas Activas", f"$ {monto_activo:,.0f}", help="Valor total en la tubería")
                    with m2: st.metric("Utilidad en Tubería", f"$ {util_activa:,.0f}", help="Suma de utilidad de proyectos abiertos")
                    with m3: st.metric("Total Ganado", f"$ {monto_ganado:,.0f}", help="Suma de montos de proyectos al 100%")

                    # Encontrar Proveedor más usado
                    prov_top = df_det_all['PROVEEDOR'].mode().iloc[0] if not df_det_all.empty and 'PROVEEDOR' in df_det_all.columns else "N/A"
                    with m4: st.metric("Proveedor Estrella", prov_top)

                    st.write("")

                    # --- BLOQUE DE RANKINGS ---
                    col_r1, col_r2 = st.columns(2)

                    with col_r1:
                        st.markdown("### 💎 Proyectos más Rentables (Utilidad)")
                        df_top_util = df_stats.sort_values('UTILIDAD_TOTAL', ascending=False).head(5)
                        for _, r in df_top_util.iterrows():
                            st.caption(f"**{r[col_folio]}** | {r[col_cliente]} | Util: ${r['UTILIDAD_TOTAL']:,.2f}")

                        st.write("")
                        st.markdown("### 📈 Top Clientes (Frecuencia)")
                        df_top_clientes = df_stats[col_cliente].value_counts().head(5).reset_index()
                        df_top_clientes.columns = ['Cliente', 'Cant']
                        st.dataframe(df_top_clientes, use_container_width=True, hide_index=True)

                    with col_r2:
                        st.markdown("### 💰 Proyectos de Mayor Volumen")
                        df_top_vol = df_stats.sort_values('MONTO_TOTAL', ascending=False).head(5)
                        for _, r in df_top_vol.iterrows():
                            st.caption(f"**{r[col_folio]}** | {r[col_cliente]} | Total: ${r['MONTO_TOTAL']:,.2f}")

                        st.write("")
                        # Listar Arrendamientos
                        st.markdown("### 🏦 Radar de Arrendamientos")
                        # Buscar en detalle folios que tengan Arrendamiento o Financiamiento
                        folios_arr = []
                        if 'FINANCIAMIENTO' in df_det_all.columns:
                            folios_arr = df_det_all[df_det_all['FINANCIAMIENTO'].isin(['Arrendamiento', 'Financiamiento'])][df_det_all.columns[0]].unique()

                        df_arr = df_stats[df_stats[col_folio].isin(folios_arr)]
                        if not df_arr.empty:
                            st.dataframe(df_arr[[col_folio, col_cliente, 'ESTATUS']], use_container_width=True, hide_index=True)
                        else:
                            st.info("No hay proyectos con financiera registrados.")

                    st.divider()
                    st.subheader("Historial Detallado")

                    # Buscador Integrado
                    busqueda = st.text_input("Buscar por Folio o Cliente:", placeholder="Escribe para filtrar...")

                    if busqueda:
                        df_filtrado = df_resumen[(df_resumen[col_folio].astype(str).str.contains(busqueda, case=False)) | 
                                                (df_resumen[col_cliente].astype(str).str.contains(busqueda, case=False))]
                    else:
                        df_filtrado = df_resumen.iloc[::-1] # Todas en reversa

                    # Clasificación por estatus
                    estatus_cerrados = ["100% Ganada", "0% Cancelada", "100% Pedido"] # Incluimos Pedido por registros antiguos
                    df_cerradas = df_filtrado[df_filtrado['ESTATUS'].isin(estatus_cerrados)]
                    df_abiertas = df_filtrado[~df_filtrado['ESTATUS'].isin(estatus_cerrados)]

                    t_ab, t_ce = st.tabs([f"Abiertas ({len(df_abiertas)})", f"Cerradas ({len(df_cerradas)})"])

                    def renderizar_lista_cotizaciones(df_fuente):
                        if df_fuente.empty:
                            st.info("No hay registros en esta categoría.")
                            return

                        for i, row in df_fuente.iterrows():
                            f_id = str(row[col_folio]).strip()
                            # Ignorar si el folio está vacío para evitar errores de Key duplicada
                            if not f_id or f_id.lower() == "nan":
                                continue

                            # Obtener info del detalle para este folio
                            det_f = df_det_all[df_det_all[df_det_all.columns[0]].astype(str) == f_id]

                            conceptos = ", ".join([str(x) for x in det_f['CONCEPTO'].unique()]) if 'CONCEPTO' in det_f.columns else "N/A"
                            proveedores = ", ".join([str(x) for x in det_f['PROVEEDOR'].unique()]) if 'PROVEEDOR' in det_f.columns else "N/A"
                            estatus = row.get('ESTATUS', 'N/A')
                            if estatus == 'N/A' and len(row) > 13: estatus = row.iloc[13]

                            col_venta = 'PFACTURA_TOTAL_IVA_INC' if 'PFACTURA_TOTAL_IVA_INC' in det_f.columns else det_f.columns[-3]
                            try: monto_total = pd.to_numeric(det_f[col_venta], errors='coerce').sum()
                            except: monto_total = 0.0

                            label = f"Folio: {f_id} | {row[col_cliente]} | ${monto_total:,.2f} | {estatus}"
                            with st.expander(label):
                                c1, c2, c3 = st.columns(3)
                                c1.write(f"**Fecha Emisión:** {row.get('FECHA_ELABORACION', 'N/A')}")
                                c1.write(f"**Estatus:** {estatus}")

                                # --- INFO DE CONTACTO ---
                                u_cont = row.get('ULTIMO_CONTACTO', 'N/A')
                                dias_oc = calcular_dias(u_cont) if u_cont != 'N/A' else 0
                                color_txt = "#E74C3C" if dias_oc > 15 else ("#F39C12" if dias_oc > 7 else "#2ECC71")
                                c2.markdown(f"**Último contacto:** {u_cont}")
                                c2.markdown(f"**Días sin contacto:** <span style='color:{color_txt}; font-weight:bold;'>{dias_oc} días</span>", unsafe_allow_html=True)

                                c2.write(f"**Atención:** {row.get('CONTACTO', 'N/A')}")
                                c3.write(f"**Monto Total:** ${monto_total:,.2f}")
                                c3.write(f"**Productos:** {conceptos}")
                                c3.write(f"**Proveedores:** {proveedores}")

                                st.divider()
                                c_b1, c_b2, c_b3, c_b4 = st.columns([1, 1, 1, 1])
                                with c_b1:
                                    if st.button(f"Editar {f_id}", key=f"edit_{f_id}", use_container_width=True):
                                        with st.spinner("Cargando información..."):
                                            cargar_cotizacion_para_editar(row, df_resumen)
                                            st.session_state.menu_actual = 'nuevo'
                                            st.rerun()
                                with c_b2:
                                    # Deshabilitar botón de pedido si ya está cerrada
                                    btn_disabled = estatus in estatus_cerrados
                                    if st.button(f"Meter Pedido {f_id}", key=f"ped_{f_id}", type="primary", use_container_width=True, disabled=btn_disabled):
                                        with st.spinner("Preparando formalización..."):
                                            cargar_cotizacion_para_editar(row, df_resumen)
                                            st.session_state.menu_actual = 'pedido'
                                            st.rerun()
                                with c_b3:
                                    # Botón Cancelar con popover de confirmación
                                    if not btn_disabled:
                                        with st.popover("CANCELAR", use_container_width=True):
                                            st.error(f"¿Confirmas cancelar el folio {f_id}?")
                                            if st.button("SÍ, CANCELAR (0%)", key=f"canc_{f_id}", use_container_width=True, type="primary"):
                                                try:
                                                    with st.spinner("Cancelando..."):
                                                        # Referencia fresca a la hoja
                                                        ws_res_fresh = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
                                                        headers = ws_res_fresh.row_values(1)
                                                        try:
                                                            col_estatus = headers.index("ESTATUS") + 1
                                                        except:
                                                            col_estatus = 14 # Fallback

                                                        folios_res = ws_res_fresh.col_values(1)
                                                        if f_id in [str(f) for f in folios_res]:
                                                            idx_res = [str(f) for f in folios_res].index(f_id) + 1
                                                            ws_res_fresh.update_cell(idx_res, col_estatus, "0% Cancelada")
                                                            st.success("Proyecto cancelado")
                                                            time.sleep(1)
                                                            st.rerun()
                                                except Exception as e: st.error(f"Error al cancelar: {e}")
                                    else:
                                        st.button("CANCELAR", use_container_width=True, disabled=True, key=f"btn_canc_dis_{f_id}")

                                with c_b4:
                                    with st.popover("ELIMINAR", use_container_width=True):
                                        st.warning(f"¿Desea eliminar el folio {f_id}?")
                                        if st.button("CONFIRMAR BORRADO", key=f"del_{f_id}", type="primary", use_container_width=True):
                                            try:
                                                with st.spinner("Eliminando registros..."):
                                                    folios_res = ws_res.col_values(1)
                                                    if f_id in [str(f) for f in folios_res]:
                                                        idx_res = [str(f) for f in folios_res].index(f_id) + 1
                                                        ws_res.delete_rows(idx_res)
                                                    folios_det = ws_det.col_values(1)
                                                    indices_det = [i + 1 for i, val in enumerate(folios_det) if str(val) == f_id]
                                                    if indices_det:
                                                        for fila in reversed(indices_det): ws_det.delete_rows(fila)
                                                    st.success("Eliminada correctamente")
                                                    time.sleep(1)
                                                    st.rerun()
                                            except Exception as e: st.error(f"Error: {e}")

                    with t_ab: renderizar_lista_cotizaciones(df_abiertas)
                    with t_ce: renderizar_lista_cotizaciones(df_cerradas)

                else:
                    st.info("Aún no tienes cotizaciones registradas. ¡Crea la primera!")
            except Exception as e:
                st.error(f"Error cargando el Dashboard: {e}")

# --- VISTA: METER PEDIDO (COTIZACIÓN GANADA) ---
elif st.session_state.menu_actual == 'pedido':
    st.title(f"🚀 Formalizar Pedido: {st.session_state.get('folio_val', 'N/A')}")

    # 1. Obtener datos base
    df_p_actual = st.session_state.df_partidas
    prov_principal = df_p_actual["Proveedor"].iloc[0] if not df_p_actual.empty else ""
    cliente_actual = st.session_state.get('cliente_sel', '')

    # 2. Buscar datos fiscales del cliente
    info_fiscal = next((f for f in st.session_state.get('datos_fiscales', []) if f.get('RAZON_SOCIAL') == cliente_actual or f.get('CLIENTE') == cliente_actual), {})
    rfc_sugerido = info_fiscal.get('RFC', '')
    credito_info = info_fiscal.get('CREDITO', 'Sin crédito')

    # --- SECCIÓN 1: DATOS FISCALES Y FACTURACIÓN ---
    st.markdown("### 📄 Datos Fiscales y Facturación")
    f1, f2 = st.columns(2)
    with f1:
        razon_f = st.text_input("Razón Social Fiscal:", value=cliente_actual)
        rfc_f = st.text_input("RFC:", value=rfc_sugerido)
    with f2:
        metodos_p = ["PUE - Pago en una sola exhibición", "PPD - Pago en parcialidades o diferido"]
        metodo_p = st.selectbox("Método de Pago:", metodos_p)
        usos_cfdi = [
            "G01 - Adquisición de mercancías", "G03 - Gastos en general", "I04 - Equipo de cómputo", "S01 - Sin efectos fiscales", "CP01 - Pagos"
        ]
        uso_cfdi = st.selectbox("Uso de CFDI:", usos_cfdi)

    # Mover Condiciones de Pago a una variable interna
    condiciones_pago = st.session_state.get('pago_val', '')
    st.divider()

    # --- SECCIÓN 2: LOGÍSTICA DETALLADA ---
    st.markdown("### 🚚 Logística y Entrega")
    l1, l2 = st.columns(2)
    with l1:
        origen_ent = st.radio("Origen de entrega:", ["Directo proveedor", "Directo de oficina"], horizontal=True, key="or_p")
        metodo_ent = st.radio("Método:", ["Paquetería", "Ruta interna", "recolección de cliente"], horizontal=True, key="met_p")
    with l2:
        dir_ent = st.text_area("Dirección completa a entregar:", value=st.session_state.get('entrega_val', ''), key="dir_p")

    lc1, lc2, lc3 = st.columns(3)
    with lc1: persona_rec = st.text_input("Persona que recibe:", key="per_p")
    with lc2: tel_rec = st.text_input("Teléfono de quien recibe:", key="tel_p")
    with lc3: maps_link = st.text_input("Link de ubicación Maps:", key="map_p")

    st.divider()

    # --- SECCIÓN 3: SOLICITUD DE PEDIDO (POR PRODUCTO) ---
    st.markdown("### 🛒 Solicitud de Pedido (Detalle de Compra)")
    detalles_compra = {}
    db_prov_full = st.session_state.get('proveedores_db', [])

    for idx, row in df_p_actual.iterrows():
        prov_item = row.get("Proveedor", "N/A")
        with st.expander(f"📦 Producto: {row.get('Concepto', 'N/A')}", expanded=True):
            c_ped0, c_ped1, c_ped2 = st.columns([1, 1.5, 1.5])
            with c_ped0: st.text_input(f"Proveedor:", value=prov_item, disabled=True, key=f"p_d_{idx}")
            with c_ped1: link_c = st.text_input(f"Link de producto:", value=row.get("Link", ""), key=f"l_d_{idx}")
            with c_ped2:
                # Normalizar búsqueda de ejecutivos por proveedor (ignorar mayúsculas/espacios)
                prov_item_norm = str(prov_item).strip().upper()
                ejs = [p.get("NOMBRE", "") for p in db_prov_full 
                       if str(p.get("PROVEEDOR")).strip().upper() == prov_item_norm]
                ejs = sorted(list(set([e.strip() for e in ejs if e and e.strip()])))
                
                if not ejs:
                    cont_c = st.text_input(f"Ejecutivo de Ventas:", key=f"m_d_{idx}", help="No se encontraron ejecutivos para este proveedor. Escriba uno manualmente.")
                else:
                    opciones = ["N/A", "OTRO (Escribir manual)..."] + ejs
                    sel_ej = st.selectbox(f"Ejecutivo de Ventas:", opciones, key=f"s_d_{idx}")
                    if sel_ej == "OTRO (Escribir manual)...":
                        cont_c = st.text_input(f"Nombre del Ejecutivo:", key=f"m_d_{idx}")
                    else:
                        cont_c = sel_ej
            detalles_compra[idx] = {"link": link_c, "contacto": cont_c}

    st.divider()

    # --- SECCIÓN 4: PAGO Y DOCUMENTACIÓN ---
    st.markdown("### 💰 Pago y Documentación")
    p1, p2 = st.columns(2)
    with p1:
        modo_respaldo = st.radio("Modo de respaldo del pedido:", ["Comprobante de pago", "Orden de compra (OC)"], horizontal=True, key="mod_resp_p")
        pago_cliente = st.selectbox("Pago de cliente:", ["Anticipado", "Linea de crédito", "Financiamiento", "Otro modo"], key="p_cli_p")

    # Inicializar variables de pago para evitar errores de referencia
    dias_credito, vigencia_fin, financiera_fin, file_arrendamiento = "N/A", "N/A", "N/A", None

    pc1, pc2 = st.columns(2)
    if pago_cliente == "Linea de crédito":
        with pc1:
            dias_credito = st.selectbox("Opciones de crédito:", ["7 Días", "15 Días", "30 Días"], key="dias_c_p")
    elif pago_cliente == "Financiamiento":
        with pc1:
            financiera_fin = st.selectbox("Financiera:", ["DFS", "HPE", "Otro"], key="finan_p")
            vigencia_fin = st.selectbox("Vigencia:", ["2 años", "3 años", "4 años"], key="vig_p")
        with pc2:
            file_arrendamiento = st.file_uploader("Cargar propuesta", type=["pdf"], key="f_arr_p")

    st.divider()
    a1, a2 = st.columns(2)
    with a1: file_respaldo = st.file_uploader("Documento de Respaldo", type=["pdf", "jpg", "png"], key="f_resp_p")
    with a2: file_csf = st.file_uploader("Constancia Fiscal (CSF)", type=["pdf"], key="f_csf_p")

    if st.button("VALIDAR Y ENVIAR PEDIDO A OPERACIONES", use_container_width=True, type="primary", key="btn_env_p"):
        if not rfc_f or not persona_rec or not file_respaldo:
            st.error("Campos obligatorios faltantes.")
        else:
            try:
                with st.spinner("Procesando..."):
                    gc = conectar_google_sheets()
                    ws_p = gc.open_by_key(ID_SHEET_PEDIDOS).sheet1
                    folio_actual = st.session_state.folio_val

                    # --- ASEGURAR COLUMNAS DE CÁLCULO PARA EL PDF TÉCNICO ---
                    df_p_final = df_p_actual.copy()
                    db_prov = st.session_state.get('proveedores_db', [])
                    mapa_iva = {p['PROVEEDOR']: (1.0 if p.get('SUMA_IVA', 'SI') == 'SI' else 1.16) for p in db_prov}
                    tc = st.session_state.get('tc_val', 1.0)
                    moneda_cot = st.session_state.get('moneda_val', 'MXN')

                    def conv(precio, moneda_item):
                        if moneda_cot == "MXN" and moneda_item == "USD": return precio * tc
                        if moneda_cot == "USD" and moneda_item == "MXN": return precio / tc
                        return precio

                    # Inyectar cálculos si no existen (viniendo del editor)
                    if "Costo (Sub)" not in df_p_final.columns:
                        df_p_final["PM_C"] = df_p_final.apply(lambda r: conv(r.get("PM", 0), r.get("Moneda", "MXN")), axis=1)
                        df_p_final["Envio_P_C"] = df_p_final.apply(lambda r: conv(r.get("Envio Prov", 0), r.get("Moneda", "MXN")), axis=1)
                        df_p_final["Envio_S_C"] = df_p_final.apply(lambda r: conv(r.get("Envio Sec", 0), r.get("Moneda", "MXN")), axis=1)
                        divs = df_p_final["Proveedor"].map(mapa_iva).fillna(1.0)
                        
                        df_p_final["Costo (Sub)"] = (df_p_final["PM_C"] / divs) + df_p_final["Envio_P_C"]
                        df_p_final["Envio Sec"] = df_p_final["Envio_S_C"]
                        df_p_final["Venta (Sub)"] = (df_p_final["Costo (Sub)"] + df_p_final["Envio Sec"]) * (1 + (df_p_final.get("Util %", 15) / 100))
                        df_p_final["Venta (IVA)"] = df_p_final["Venta (Sub)"] * 1.16

                    # Subir archivos a Drive
                    l_pago = subir_archivo_a_drive(file_respaldo.read(), f"PAGO_{folio_actual}.pdf") if file_respaldo else ""
                    l_csf = subir_archivo_a_drive(file_csf.read(), f"CSF_{folio_actual}.pdf") if file_csf else ""

                    p_final_str = f"{pago_cliente} ({dias_credito if pago_cliente=='Linea de crédito' else vigencia_fin})"

                    pdf_t = generar_pedido_tecnico_blob_v2(
                        {"folio": folio_actual, "ejecutivo": st.session_state.ejecutivo_nom, "cliente": cliente_actual, "pago": p_final_str}, 
                        df_p_final, 
                        {"rfc": rfc_f, "razon_fiscal": razon_f, "uso_cfdi": uso_cfdi, "metodo_pago": metodo_p}, 
                        {
                            "dir_entrega": dir_ent, 
                            "persona_recibe": persona_rec, 
                            "tel_contacto": tel_rec,
                            "origen": origen_ent,
                            "metodo": metodo_ent,
                            "maps": maps_link
                        }, 
                        {}, 
                        detalles_compra
                    )
                    l_pdf = subir_archivo_a_drive(pdf_t, f"PEDIDO_{folio_actual}.pdf")

                    def guardar_fila_inteligente(ws, datos_dict):
                        headers = [str(h).strip().upper() for h in ws.row_values(1)]
                        fila = [""] * len(headers)
                        for k, v in datos_dict.items():
                            if k.upper() in headers: fila[headers.index(k.upper())] = v
                        ws.append_row(fila)

                    # Calcular Monto Total para el registro central
                    monto_total = (df_p_final["Venta (IVA)"] * df_p_final["Pzas"]).sum()

                    datos_m = {
                        "FECHA": str(date.today()), 
                        "FOLIO": folio_actual, 
                        "EJECUTIVO": st.session_state.ejecutivo_nom, 
                        "CLIENTE": cliente_actual, 
                        "RAZON_SOCIAL_FISCAL": razon_f, 
                        "RFC": rfc_f, 
                        "USO_CFDI": uso_cfdi,
                        "METODO_PAGO": metodo_p,
                        "ORIGEN_ENTREGA": origen_ent,
                        "METODO_ENVIO": metodo_ent,
                        "DIRECCION_ENTREGA": dir_ent,
                        "PERSONA_RECIBE": persona_rec,
                        "TEL_CONTACTO": tel_rec,
                        "LINK_MAPS": maps_link,
                        "TIPO_PAGO": pago_cliente,
                        "DIAS_CREDITO": dias_credito,
                        "VIGENCIA_FINANCIAMIENTO": vigencia_fin,
                        "FINANCIERA": financiera_fin,
                        "MONTO_TOTAL": monto_total,
                        "ESTATUS": "PEDIDO NUEVO", 
                        "PDF_TECNICO": l_pdf, 
                        "COMPROBANTE_RESPALDO": l_pago,
                        "CONSTANCIA_FISCAL": l_csf,
                        "ARRENDAMIENTO_PROPUESTA": subir_archivo_a_drive(file_arrendamiento.read(), f"ARR_{folio_actual}.pdf") if pago_cliente == "Financiamiento" and file_arrendamiento else ""
                    }
                    guardar_fila_inteligente(ws_p, datos_m)

                    try:
                        ws_l = st.session_state.sh_personal.worksheet("PEDIDOS")
                        guardar_fila_inteligente(ws_l, datos_m)
                    except: pass

                    st.session_state.pedido_exitoso = True
                    st.session_state.pdf_tecnico_actual = pdf_t
                    st.balloons()
                    st.rerun()
            except Exception as e: st.error(f"Error: {e}")

    if st.session_state.get('pedido_exitoso'):
        st.success("✅ Pedido enviado. En espera de Visto Bueno.")
        st.download_button("Descargar Pedido Técnico", data=st.session_state.pdf_tecnico_actual, file_name=f"PEDIDO_{st.session_state.folio_val}.pdf", use_container_width=True)
        if st.button("Volver al Dashboard"):
            del st.session_state['pedido_exitoso']
            st.session_state.menu_actual = 'menu'
            st.rerun()

# --- VISTA: GENERADOR DE COTIZACIÓN (NUEVO / EDITAR) ---
elif st.session_state.menu_actual == 'nuevo':
    st.header(f"{'Editando' if st.session_state.get('folio_val') else 'Nueva'} Cotización")
    
    # Crear las pestañas
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Generales", "Partidas", "Ilustraciones", "Evidencias", "Finalizar"])

    with tab1:
        st.subheader("Datos del Emisor y Cliente")
        col_e1, col_e2, col_e3 = st.columns(3)
        
        # Función de búsqueda de valor seguro (evita errores de índice)
        def get_val(key, default="Seleccionar..."):
            val = st.session_state.get(key, default)
            return val if val else default

        with col_e1:
            ejecutivos_lista = sorted([u['NOMBRE'] for u in st.session_state.usuarios_db])
            opciones_ej = ["Seleccionar..."] + ejecutivos_lista
            val_ej_actual = get_val('ejecutivo_nom')
            
            if val_ej_actual != "Seleccionar..." and val_ej_actual not in opciones_ej:
                nombre_match = next((u['NOMBRE'] for u in st.session_state.usuarios_db if u['USUARIO'] == val_ej_actual), None)
                if nombre_match: val_ej_actual = nombre_match
            
            idx_ej = opciones_ej.index(val_ej_actual) if val_ej_actual in opciones_ej else 0
            ejecutivo_nom = st.selectbox("Ejecutivo que firma:", opciones_ej, index=idx_ej, key="ej_sel_final")
            st.session_state.ejecutivo_nom = ejecutivo_nom
        
        v_tel, v_mail = ("", "")
        if ejecutivo_nom != "Seleccionar...":
            try:
                d = next(u for u in st.session_state.usuarios_db if u['NOMBRE'] == ejecutivo_nom)
                v_tel, v_mail = d.get('TELEFONO', ''), d.get('EMAIL', '')
            except: pass

        with col_e2:
            tel_e = st.text_input("Teléfono:", value=v_tel)
        with col_e3:
            mail_e = st.text_input("Email:", value=v_mail)

        st.divider()
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            lista_rs = sorted(list(set([c.get('RAZON_SOCIAL') for c in st.session_state.directorio if c.get('RAZON_SOCIAL')])))
            opciones_rs = ["Seleccionar..."] + lista_rs
            val_rs_actual = st.session_state.get('cliente_sel', 'Seleccionar...')
            idx_rs = opciones_rs.index(val_rs_actual) if val_rs_actual in opciones_rs else 0
            cliente_sel = st.selectbox("Razón Social:", opciones_rs, index=idx_rs, key="rs_sel_final")
            st.session_state.cliente_sel = cliente_sel
        
        with col_c2:
            if cliente_sel != "Seleccionar...":
                contactos = sorted(list(set([c.get('CONTACTO') for c in st.session_state.directorio if c.get('RAZON_SOCIAL') == cliente_sel and c.get('CONTACTO')])))
                opciones_c = ["Seleccionar..."] + contactos
                val_c_actual = st.session_state.get('contacto_sel', 'Seleccionar...')
                idx_c = opciones_c.index(val_c_actual) if val_c_actual in opciones_c else 0
                contacto_sel = st.selectbox("Atención a:", opciones_c, index=idx_c, key="cont_sel_final")
                st.session_state.contacto_sel = contacto_sel
            else:
                st.selectbox("Atención a:", ["Seleccionar..."], disabled=True, key="cont_dis")
                contacto_sel = "Seleccionar..."

        # --- LÓGICA DE FOLIO SÚPER-DIRECTA ---
        if cliente_sel != "Seleccionar..." and ejecutivo_nom != "Seleccionar...":
            cambio_cliente = st.session_state.get("ultimo_cliente_folio") != cliente_sel
            cambio_ejecutivo = st.session_state.get("ultimo_ejecutivo_folio") != ejecutivo_nom
            if not st.session_state.get("folio_val") or cambio_cliente or cambio_ejecutivo:
                if "folio_original_edicion" not in st.session_state:
                    ej_id = next((u["USUARIO"] for u in st.session_state.usuarios_db if u["NOMBRE"] == ejecutivo_nom), st.session_state.usuario)
                    nuevo_f = generar_folio_automatico(cliente_sel, ej_id)
                    if nuevo_f:
                        st.session_state.folio_val = nuevo_f
                        st.session_state.folio_widget_input = nuevo_f
                        st.session_state.ultimo_cliente_folio = cliente_sel
                        st.session_state.ultimo_ejecutivo_folio = ejecutivo_nom
                        st.rerun()

        st.divider()
        col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 1, 1, 1])
        with col_f1:
            folio_input = st.text_input("Folio de Cotización:", value=st.session_state.get('folio_val', ""), key="folio_widget_input")
            st.session_state.folio_val = folio_input
        with col_f2:
            moneda_opciones = ["MXN", "USD"]
            val_mon_actual = st.session_state.get('moneda_val', 'MXN')
            idx_m = moneda_opciones.index(val_mon_actual) if val_mon_actual in moneda_opciones else 0
            st.session_state.moneda_val = st.selectbox("Moneda Cotización:", moneda_opciones, index=idx_m, key="mon_sel_final")
        with col_f3:
            st.session_state.tc_val = st.number_input("Tipo de Cambio:", value=st.session_state.get('tc_val', 18.00), format="%.2f", key="tc_val_input")
        with col_f4:
            st.session_state.vigencia_val = st.date_input("Vigencia:", value=st.session_state.get('vigencia_val', date.today()), key="vigencia_val_d")

        st.divider()
        st.subheader("Términos y Condiciones")
        
        # --- CARGA FORZADA CON FALLBACKS INTEGRADOS ---
        terminos_raw = st.session_state.get('terminos_db', [])
        
        t1, t2, t3 = st.columns(3)
        
        with t1:
            # Leer directamente de la lista vertical procesada (Columna A)
            db_ent = [str(t.get('ENTREGA', '')) for t in terminos_raw if t.get('ENTREGA')]
            lista_ent = sorted(list(set([x for x in db_ent if str(x).strip()])))
            if not lista_ent:
                lista_ent = ["Inmediata", "3 a 5 días hábiles", "1 a 2 semanas", "Sujeto a existencias"]
            
            opciones_ent = ["Seleccionar..."] + lista_ent
            val_ent_actual = st.session_state.get('entrega_val', 'Seleccionar...')
            idx_ent = buscar_index(opciones_ent, val_ent_actual)
            st.session_state.entrega_val = st.selectbox("Tiempo de Entrega:", opciones_ent, index=idx_ent, key="ent_f")
            
        with t2:
            # Leer directamente de la lista vertical procesada (Columna B)
            db_pag = [str(t.get('PAGO', '')) for t in terminos_raw if t.get('PAGO')]
            lista_pag = sorted(list(set([x for x in db_pag if str(x).strip()])))
            if not lista_pag:
                lista_pag = ["Contado", "50% Anticipo / 50% Entrega", "Crédito 15 días", "Crédito 30 días"]
            
            opciones_pag = ["Seleccionar..."] + lista_pag
            val_pag_actual = st.session_state.get('pago_val', 'Seleccionar...')
            idx_pag = buscar_index(opciones_pag, val_pag_actual)
            st.session_state.pago_val = st.selectbox("Forma de Pago:", opciones_pag, index=idx_pag, key="pag_f")
            
        with t3:
            # Intentar leer de columna específica (Hoja 1 Col C) o formato CATEGORIA/VALOR
            db_con = [str(t.get('CONDICIONES', '')) for t in terminos_raw if t.get('CONDICIONES')]
            if not db_con:
                db_con = [str(t.get('VALOR', '')) for t in terminos_raw if str(t.get('CATEGORIA', '')).strip().upper() == 'CONDICIONES']
            
            lista_con = sorted(list(set([x for x in db_con if str(x).strip()])))
            if not lista_con:
                lista_con = ["Precios sujetos a cambio sin previo aviso", "Garantía de 1 año", "L.A.B. Nuestras oficinas"]
            
            opciones_con = ["Seleccionar..."] + lista_con
            val_con_actual = st.session_state.get('condic_val', 'Seleccionar...')
            idx_con = buscar_index(opciones_con, val_con_actual)
            st.session_state.condic_val = st.selectbox("Condiciones Especiales:", opciones_con, index=idx_con, key="con_f")

        if not terminos_raw:
            st.caption("ℹ️ Nota: Mostrando opciones estándar del sistema.")


    with tab2:
        st.subheader("Análisis de Partidas")
        db_prov = st.session_state.get('proveedores_db', [])
        lista_prov = [p['PROVEEDOR'] for p in db_prov]
        mapa_iva = {p['PROVEEDOR']: (1.0 if p.get('SUMA_IVA', 'SI') == 'SI' else 1.16) for p in db_prov}

        if 'df_partidas' not in st.session_state:
            st.session_state.df_partidas = pd.DataFrame([{
                "Tipo": "PARTIDA", "Moneda": "MXN", "Concepto": "", "Descripción": "", "Pzas": 1, "SKU": "",
                "PM": 0.0, "Proveedor": lista_prov[0] if lista_prov else "", 
                "Folio Prov": "", "Link": "",
                "Envio Prov": 0.0, "Envio Sec": 0.0, "Util %": 15.0,
                "Financiamiento": "Sin Financiera", "Financiera": "N/A"
            }])

        config_editor = {
            "Tipo": st.column_config.SelectboxColumn("Tipo", options=["PARTIDA", "COMPONENTE"], required=True),
            "Moneda": st.column_config.SelectboxColumn("Moneda", options=["MXN", "USD"], required=True),
            "Descripción": st.column_config.TextColumn("Descripción", width="medium", required=True),
            "PM": st.column_config.NumberColumn("P. Mayorista", format="$ %.2f"),
            "Proveedor": st.column_config.SelectboxColumn("Proveedor", options=lista_prov),
            "Util %": st.column_config.NumberColumn("Margen %", format="%.1f%%"),
            "Pzas": st.column_config.NumberColumn("Cant", min_value=1),
            "Financiamiento": st.column_config.SelectboxColumn("Financiamiento", options=["Sin Financiera", "Arrendamiento", "Financiamiento"], required=True),
            "Financiera": st.column_config.SelectboxColumn("Financiera", options=["N/A", "DFS", "HPE", "Otro"], required=True),
        }

        key_dinamica = f"editor_{st.session_state.get('editor_key', 0)}"
        editado = st.data_editor(st.session_state.df_partidas, column_config=config_editor, num_rows="dynamic", use_container_width=True, key=key_dinamica)

        if editado is not None and not editado.empty:
            df_analisis = editado.copy()
            for col in ["PM", "Pzas", "Util %", "Envio Prov", "Envio Sec"]:
                if col in df_analisis.columns: df_analisis[col] = pd.to_numeric(df_analisis[col], errors='coerce').fillna(0)

            tc = st.session_state.get('tc_val', 1.0)
            moneda_cot = st.session_state.get('moneda_val', 'MXN')

            def normalizar_a_cotizacion(precio, moneda_item):
                if moneda_cot == "MXN" and moneda_item == "USD": return precio * tc
                if moneda_cot == "USD" and moneda_item == "MXN": return precio / tc
                return precio

            df_analisis["PM_CONV"] = df_analisis.apply(lambda r: normalizar_a_cotizacion(r["PM"], r.get("Moneda", "MXN")), axis=1)
            df_analisis["Envio_P_CONV"] = df_analisis.apply(lambda r: normalizar_a_cotizacion(r["Envio Prov"], r.get("Moneda", "MXN")), axis=1)
            df_analisis["Envio_S_CONV"] = df_analisis.apply(lambda r: normalizar_a_cotizacion(r["Envio Sec"], r.get("Moneda", "MXN")), axis=1)

            divisores = df_analisis["Proveedor"].map(mapa_iva).fillna(1.0)
            df_analisis["Costo (Sub)"] = ((df_analisis["PM_CONV"] / divisores) + df_analisis["Envio_P_CONV"]).round(2)
            df_analisis["Costo (IVA)"] = (df_analisis["Costo (Sub)"] * 1.16).round(2)
            costo_final_v = df_analisis["Costo (Sub)"] + df_analisis["Envio_S_CONV"]
            df_analisis["Venta (Sub)"] = (costo_final_v * (1 + (df_analisis["Util %"] / 100))).round(2)
            df_analisis["Venta (IVA)"] = (df_analisis["Venta (Sub)"] * 1.16).round(2)
            df_analisis["Util $ (Uni)"] = (df_analisis["Venta (Sub)"] - costo_final_v).round(2)
            df_analisis["Total Línea"] = (df_analisis["Venta (IVA)"] * df_analisis["Pzas"]).round(2)

            st.write("Análisis Detallado de Partidas")

            # --- DESGLOSE FINANCIERO MAESTRO ---
            df_analisis["Costo unit. prod.prov. sin iva"] = df_analisis["Costo (Sub)"]
            df_analisis["Costo unit. prod. prov. con iva"] = df_analisis["Costo (IVA)"]
            df_analisis["total prov sin iva"] = (df_analisis["Costo (Sub)"] * df_analisis["Pzas"]).round(2)
            df_analisis["total prov con iva"] = (df_analisis["Costo (IVA)"] * df_analisis["Pzas"]).round(2)
            df_analisis["Envío Local (Unit)"] = df_analisis["Envio Sec"]
            df_analisis["Envío Local (Total)"] = (df_analisis["Envio Sec"] * df_analisis["Pzas"]).round(2)

            df_analisis["venta unitaria sin iva"] = df_analisis["Venta (Sub)"]
            df_analisis["venta unitaria con iva"] = df_analisis["Venta (IVA)"]
            df_analisis["venta total sin iva"] = (df_analisis["Venta (Sub)"] * df_analisis["Pzas"]).round(2)
            df_analisis["venta total con iva"] = df_analisis["Total Línea"]

            df_analisis["utilidad total"] = (df_analisis["Util $ (Uni)"] * df_analisis["Pzas"]).round(2)

            cols_finales = [
                "Concepto", "Pzas",
                "Costo unit. prod.prov. sin iva", "Costo unit. prod. prov. con iva", 
                "total prov sin iva", "total prov con iva",
                "Envío Local (Unit)", "Envío Local (Total)",
                "venta unitaria sin iva", "venta unitaria con iva", 
                "venta total sin iva", "venta total con iva",
                "utilidad total"
            ]
            def aplicar_estilo_financiero(styler):
                # Costos Proveedor (Verde)
                styler.set_properties(subset=["Costo unit. prod.prov. sin iva", "Costo unit. prod. prov. con iva", "total prov sin iva", "total prov con iva"], 
                                    **{'background-color': '#1B5E20', 'color': 'white'})
                # Envío (Gris oscuro)
                styler.set_properties(subset=["Envío Local (Unit)", "Envío Local (Total)"], 
                                    **{'background-color': '#424242', 'color': 'white'})
                # Ventas (Amarillo)
                styler.set_properties(subset=["venta unitaria sin iva", "venta unitaria con iva", "venta total sin iva", "venta total con iva"], 
                                    **{'background-color': '#FBC02D', 'color': 'black'})
                # Utilidad (Morado)
                styler.set_properties(subset=["utilidad total"], 
                                    **{'background-color': '#4A148C', 'color': 'white'})
                return styler

            st.dataframe(
                df_analisis[cols_finales].style.pipe(aplicar_estilo_financiero).format(precision=2, thousands=",", decimal="."),
                use_container_width=True, 
                hide_index=True
            )

            with tab3:
                st.subheader("Fotografías de Productos")
                if 'dict_fotos' not in st.session_state: st.session_state.dict_fotos = {}
                if not editado.empty:
                    filas_imgs = [editado.iloc[i:i+3] for i in range(0, len(editado), 3)]
                    for f_idx, fila_df in enumerate(filas_imgs):
                        cols = st.columns(3)
                        for c_idx, (real_idx, row) in enumerate(fila_df.iterrows()):
                            nombre_p = row["Concepto"] if row["Concepto"] else f"Partida {real_idx+1}"
                            with cols[c_idx]:
                                if 'dict_fotos_links' in st.session_state and real_idx in st.session_state.dict_fotos_links:
                                    raw_link = st.session_state.dict_fotos_links[real_idx]
                                    # Solo intentar mostrar si es un link de Drive o URL válida
                                    if raw_link and str(raw_link).startswith("http"):
                                        link_img = obtener_link_directo_drive(raw_link)
                                        st.image(link_img, caption=f"Actual: {nombre_p}", use_container_width=True)
                                    else:
                                        st.info(f"Foto previa: {nombre_p} (Solo nombre guardado)")
                                
                                foto = st.file_uploader(f"Cargar {nombre_p}", type=["png", "jpg", "jpeg"], key=f"f_{real_idx}")
                                if foto:
                                    st.session_state.dict_fotos[real_idx] = foto
                                    st.image(foto, use_container_width=True)

            with tab4:
                st.subheader("Documentos de Respaldo por Producto")
                st.info("Sube aquí cotizaciones de proveedores, propuestas de financiera o capturas de pantalla de cada producto.")
                if 'dict_evidencias' not in st.session_state: st.session_state.dict_evidencias = {}
                
                if not editado.empty:
                    for real_idx, row in editado.iterrows():
                        nombre_p = row["Concepto"] if row["Concepto"] else f"Partida {real_idx+1}"
                        col_ev1, col_ev2 = st.columns([2, 1])
                        
                        with col_ev1:
                            if 'dict_evidencias_links' in st.session_state and real_idx in st.session_state.dict_evidencias_links:
                                link_previo = st.session_state.dict_evidencias_links[real_idx]
                                if link_previo:
                                    st.markdown(f"✅ **{nombre_p}**: [Ver documento actual]({link_previo})")
                                else:
                                    st.markdown(f"⚪ **{nombre_p}**: Sin documento")
                            else:
                                st.markdown(f"⚪ **{nombre_p}**: Sin documento")
                        
                        with col_ev2:
                            evidencia = st.file_uploader(f"Cargar para: {nombre_p}", type=["pdf", "png", "jpg", "jpeg", "docx"], key=f"ev_{real_idx}")
                            if evidencia:
                                st.session_state.dict_evidencias[real_idx] = evidencia

            with tab5:
                # Cálculos Maestros
                costo_t_sin = (costo_final_v * df_analisis["Pzas"]).sum()
                costo_t_con = costo_t_sin * 1.16
                venta_t_sin = (df_analisis["Venta (Sub)"] * df_analisis["Pzas"]).sum()
                venta_t_con = df_analisis["Total Línea"].sum()
                util_t = (df_analisis["Util $ (Uni)"] * df_analisis["Pzas"]).sum()
                comision_t = util_t * 0.03

                # --- MÉTRICAS EN GRANDE ---
                m1, m2, m3 = st.columns(3)
                with m1: 
                    st.metric("COSTO TOTAL (Sin IVA)", f"$ {costo_t_sin:,.2f}")
                    st.metric("COSTO TOTAL (Con IVA)", f"$ {costo_t_con:,.2f}")
                with m2:
                    st.metric("VENTA TOTAL (Sin IVA)", f"$ {venta_t_sin:,.2f}")
                    st.metric("VENTA TOTAL (Con IVA)", f"$ {venta_t_con:,.2f}", delta_color="normal")
                with m3:
                    st.metric("UTILIDAD TOTAL", f"$ {util_t:,.2f}")
                    st.metric("COMISIÓN (3%)", f"$ {comision_t:,.2f}")

                st.divider()

                # --- RESUMEN DE DATOS ---
                c_r1, c_r2 = st.columns(2)
                with c_r1:
                    st.markdown(f"**Cliente:** {st.session_state.get('cliente_sel', 'No seleccionado')}")
                    st.markdown(f"**Atención:** {st.session_state.get('contacto_sel', 'No seleccionado')}")
                    st.markdown(f"**Folio:** {st.session_state.get('folio_val', 'N/A')}")
                    st.markdown(f"**Vigencia:** {st.session_state.get('vigencia_val', 'N/A')}")
                with c_r2:
                    st.markdown(f"**Pago:** {st.session_state.get('pago_val', 'No seleccionado')}")
                    st.markdown(f"**Entrega:** {st.session_state.get('entrega_val', 'No seleccionado')}")
                    st.markdown(f"**Condiciones:** {st.session_state.get('condic_val', 'No seleccionado')}")
                    st.markdown(f"**Partidas:** {len(df_analisis)} conceptos")

                st.write("**Conceptos a registrar:**", ", ".join([str(x) for x in df_analisis["Concepto"].tolist()]))
                
                st.divider()
                col_fin1, col_fin2 = st.columns(2)
                with col_fin1:
                    val_coment = st.session_state.get('coment_val', "")
                    comentarios = st.text_area("✍️ Comentarios / Resumen Ejecutivo:", value=val_coment, key="coment_val_input", help="Estos comentarios se guardarán en el historial y son útiles para el seguimiento.")
                    st.session_state.coment_val = comentarios
                
                with col_fin2:
                    opciones_estatus = ["1% Planificación", "10% Descubrir", "30% Clasificación", "60% Propuesta", "90% Compromiso", "100% Ganada", "0% Cancelada"]
                    val_est_actual = st.session_state.get('estatus_val', '60% Propuesta')
                    idx_est = buscar_index(opciones_estatus, val_est_actual)
                    estatus_sel = st.selectbox("Estatus del Proyecto (Embudo):", opciones_estatus, index=idx_est, key="estatus_sel_final")
                    st.session_state.estatus_val = estatus_sel
                    
                    # --- NUEVO: ÚLTIMO CONTACTO ---
                    val_cont_prev = st.session_state.get('ultimo_contacto_val', date.today())
                    fecha_contacto = st.date_input("📅 Fecha de último contacto:", value=val_cont_prev, key="u_cont_input")
                    st.session_state.ultimo_contacto_val = fecha_contacto

                if st.button("CONFIRMAR Y GUARDAR TODO", use_container_width=True, type="primary"):
                    # Extraer valores de sesión para validación y cabecera
                    val_ent = st.session_state.get('entrega_val', 'Seleccionar...')
                    val_pag = st.session_state.get('pago_val', 'Seleccionar...')
                    val_con = st.session_state.get('condic_val', 'Seleccionar...')
                    val_vig = st.session_state.get('vigencia_val', date.today())
                    
                    if "Seleccionar..." in [ejecutivo_nom, cliente_sel, contacto_sel, val_ent, val_pag, val_con] or not st.session_state.folio_val:
                        st.error("Revisa que todos los campos obligatorios en 'Generales' estén llenos.")
                    else:
                        try:
                            with st.spinner("Guardando en Sheets y Drive..."):
                                folio_actual = st.session_state.folio_val
                                estatus_final = st.session_state.get('estatus_val', '60% Propuesta')
                                fecha_cont_str = str(st.session_state.get('ultimo_contacto_val', date.today()))
                                
                                dict_links_drive = st.session_state.get('dict_fotos_links', {}).copy()
                                if st.session_state.dict_fotos:
                                    for idx_f, bytes_f in st.session_state.dict_fotos.items():
                                        try:
                                            bytes_f.seek(0)
                                            link_d = subir_archivo_a_drive(bytes_f.read(), f"Partida_{folio_actual}_{idx_f}.png", 'image/png')
                                            if link_d: dict_links_drive[idx_f] = link_d
                                        except Exception as e:
                                            st.warning(f"No se pudo subir la imagen de la partida {idx_f}: {e}")
                                
                                dict_evidencias_drive = st.session_state.get('dict_evidencias_links', {}).copy()
                                if st.session_state.get('dict_evidencias'):
                                    for idx_ev, file_ev in st.session_state.get('dict_evidencias').items():
                                        try:
                                            file_ev.seek(0)
                                            link_ev = subir_archivo_a_drive(file_ev.read(), f"Evidencia_{folio_actual}_{idx_ev}_{file_ev.name}", file_ev.type)
                                            if link_ev: dict_evidencias_drive[idx_ev] = link_ev
                                        except Exception as e:
                                            st.warning(f"No se pudo subir el archivo de evidencia {idx_ev}: {e}")

                                ws_res = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
                                folios_res = ws_res.col_values(1)
                                
                                if str(folio_actual) in [str(f) for f in folios_res]:
                                    idx_fila = [str(f) for f in folios_res].index(str(folio_actual)) + 1
                                    ws_res.delete_rows(idx_fila)

                                ws_det = st.session_state.sh_personal.worksheet("COTIZACIONES_DETALLE")
                                folios_det = ws_det.col_values(1)
                                filas_a_borrar = [i + 1 for i, val in enumerate(folios_det) if str(val) == str(folio_actual)]
                                if filas_a_borrar:
                                    for fila in reversed(filas_a_borrar): ws_det.delete_rows(fila)

                                cab = {
                                    "folio": folio_actual, "ejecutivo": ejecutivo_nom, "email": mail_e, "tel": tel_e, 
                                    "cliente": cliente_sel, "contacto": contacto_sel, "vigencia": str(val_vig), 
                                    "entrega": val_ent, "pago": val_pag, "condiciones": val_con,
                                    "moneda": st.session_state.get('moneda_val', 'MXN'),
                                    "tc": st.session_state.get('tc_val', 1.0)
                                }
                                pdf_blob = generar_pdf_blob(cab, df_analisis, st.session_state.dict_fotos, dict_links_drive)

                                nombre_pdf = f"{folio_actual}.pdf"
                                link_pdf_drive = ""
                                try:
                                    link_pdf_drive = subir_archivo_a_drive(pdf_blob, nombre_pdf, 'application/pdf')
                                except Exception as e:
                                    st.error(f"Error crítico: No se pudo subir el PDF a Drive. {e}")
                                
                                # Registro en Resumen
                                datos_res = [
                                    folio_actual, ejecutivo_nom, mail_e, tel_e, str(date.today()), 
                                    link_pdf_drive, cliente_sel, contacto_sel, str(val_vig), 
                                    val_ent, val_pag, val_con, comentarios, estatus_final,
                                    st.session_state.get('moneda_val', 'MXN'),
                                    st.session_state.get('tc_val', 1.0),
                                    fecha_cont_str
                                ]
                                ws_res.update(f"A{obtener_primera_fila_vacia(ws_res)}", [datos_res])

                                filas_det = []
                                for real_idx, r in df_analisis.iterrows():
                                    util_linea = r["Util $ (Uni)"] * r["Pzas"]
                                    # Columna AC (29) es Evidencia, Columna AD (30) es Moneda_Item
                                    filas_det.append([
                                        folio_actual, r.get("Tipo", "PARTIDA"), r["Concepto"], r["Descripción"], 
                                        r["Pzas"], 0, r["SKU"], r["Folio Prov"], r["PM"], str(date.today()), 
                                        r["Proveedor"], r["Link"], r["Envio Prov"], r["Costo (Sub)"], 
                                        r["Costo (IVA)"], r["Costo (Sub)"]*r["Pzas"], r["Costo (IVA)"]*r["Pzas"], 
                                        r["Envio Sec"], r["Util %"]/100, util_linea, r["Venta (Sub)"], 
                                        r["Venta (IVA)"], r["Venta (Sub)"]*r["Pzas"], r["Venta (IVA)"]*r["Pzas"], 
                                        util_linea*0.03, dict_links_drive.get(real_idx, ""),
                                        r.get("Financiamiento", "Sin Financiera"),
                                        r.get("Financiera", "N/A"),
                                        dict_evidencias_drive.get(real_idx, ""),
                                        r.get("Moneda", "MXN")
                                    ])
                                ws_det.update(f"A{obtener_primera_fila_vacia(ws_det)}", filas_det)
                                st.session_state.pdf_actual = pdf_blob
                                st.session_state.nombre_pdf = nombre_pdf
                                st.session_state.remision_actual = generar_remision_blob(cab, df_analisis, st.session_state.dict_fotos, dict_links_drive)
                                st.session_state.registro_exitoso = True
                                st.balloons()
                                st.rerun()
                        except Exception as e: st.error(f"Error: {e}")

                if st.session_state.get('registro_exitoso'):
                    st.success("¡Guardado con éxito!")
                    st.download_button("Descargar Cotización", data=st.session_state.pdf_actual, file_name=st.session_state.nombre_pdf, use_container_width=True, type="primary")

