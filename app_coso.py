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
    return datetime.now(timezone(timedelta(hours=-6)))

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
            st.error(f"Error técnico en la nube: {str(e)}")
            return None

    # 2. Intentar con archivo local
    if os.path.exists(FILE_JSON_SERVICE):
        try:
            creds = Credentials.from_service_account_file(FILE_JSON_SERVICE, scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            st.error(f"Error con archivo local: {e}")
            return None
            
    return None

# --- CARGA SÓLO USUARIOS PARA LOGIN ---
def cargar_usuarios_login():
    if 'usuarios_db' not in st.session_state:
        try:
            gc = conectar_google_sheets()
            if gc is None:
                st.warning("⚠️ Esperando configuración de base de datos...")
                st.stop()
            st.session_state.usuarios_db = gc.open("CONTROL_USUARIOS").sheet1.get_all_records()
        except Exception as e:
            st.error(f"Acceso denegado a la lista de usuarios: {e}")
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
                    
                    # --- GUARDADO PERSISTENTE EN GOOGLE SHEETS ---
                    gc = conectar_google_sheets()
                    ws_users = gc.open("CONTROL_USUARIOS").sheet1
                    usuarios_list = ws_users.col_values(1) # Asumiendo columna 1 es USUARIO
                    
                    if usuario_regreso in usuarios_list:
                        fila_idx = usuarios_list.index(usuario_regreso) + 1
                        # Buscar o crear columna TOKEN
                        headers = ws_users.row_values(1)
                        if "TOKEN_DRIVE" not in headers:
                            ws_users.update_cell(1, len(headers) + 1, "TOKEN_DRIVE")
                            col_idx = len(headers) + 1
                        else:
                            col_idx = headers.index("TOKEN_DRIVE") + 1
                        
                        ws_users.update_cell(fila_idx, col_idx, json.dumps(token_data))
                    
                    st.success(f"¡Drive de {usuario_regreso} conectado con éxito!")
                    st.query_params.clear()
                    time.sleep(2)
                    st.rerun()
        except Exception as e:
            st.error(f"Error técnico en vinculación: {e}")

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
                    # Opcional: Actualizar el token en el sheet si se refrescó (para la próxima vez)
                except: pass
            
            return build('drive', 'v3', credentials=creds)
    except Exception as e:
        # st.write(f"Debug Drive: {e}") # Descomentar para ver errores de conexión
        return None
    return None

# --- GENERADOR DE FOLIO AUTOMÁTICO ---
def generar_folio_automatico(cliente_rs, ejecutivo_id):
    """Genera un folio con sintaxis: CLIENTE-EJECUTIVO-FECHA-SUCURSAL-CONSECUTIVO"""
    try:
        # 1. Siglas Cliente
        info_c = next((c for c in st.session_state.directorio if c['RAZON_SOCIAL'] == cliente_rs), {})
        siglas_c = str(info_c.get('SIGLAS', 'SCL')).upper()[:3]
        
        # 2. Siglas Ejecutivo y Sucursal
        info_e = next((u for u in st.session_state.usuarios_db if u['USUARIO'] == ejecutivo_id), {})
        siglas_e = str(info_e.get('SIGLAS', 'SEJ')).upper()[:3]
        sucursal = str(info_e.get('SUCURSAL', 'MX')).upper()[:2]
        
        # 3. Fecha (YYMMDD)
        fecha_str = date.today().strftime("%y%m%d")
        
        # 4. Consecutivo (Basado en el historial)
        prefijo_busqueda = f"{siglas_c}-{siglas_e}-{fecha_str}-{sucursal}"
        
        ws_res = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
        folios_historial = ws_res.col_values(1) # Columna FOLIO
        
        # Contar cuántos folios existen hoy para este ejecutivo/cliente
        count = 1
        for f in folios_historial:
            if str(f).startswith(prefijo_busqueda):
                count += 1
        
        consecutivo = str(count).zfill(3)
        
        return f"{prefijo_busqueda}-{consecutivo}"
    except Exception as e:
        return ""

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

def generar_pedido_tecnico_blob_v2(cab, df_partidas, datos_fisc, datos_log, datos_oper):
    """Genera el PDF técnico avanzado para administración con los 3 rubros detallados."""
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
    pdf.rect(15, y_bloques, 90, 38, "F") # Facturación
    pdf.rect(110, y_bloques, 90, 38, "F") # Logística
    
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
    pdf.multi_cell(80, 4, f"Dirección: {limpiar_texto(datos_log['dir_entrega'])}\nRecibe: {limpiar_texto(datos_log['persona_recibe'])}\nTel: {limpiar_texto(datos_log['tel_contacto'])}\nPO: {limpiar_texto(datos_log['num_po'])}")

    pdf.ln(5)
    
    # --- BLOQUE 2: DATOS DE PROVEEDOR Y OPERACIÓN ---
    y_oper = pdf.get_y() + 5
    pdf.set_fill_color(*gris_suave)
    pdf.rect(15, y_oper, 185, 30, "F")
    
    pdf.set_xy(18, y_oper + 3)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(*azul_corp)
    pdf.cell(180, 5, "DATOS DE PROVEEDOR Y OPERACIÓN", ln=True)
    
    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(*gris_texto)
    pdf.set_x(18)
    c1, c2, c3 = 60, 60, 60
    # Fila 1 Operación
    pdf.cell(c1, 4, f"Proveedor: {limpiar_texto(cab['proveedor'])}")
    pdf.cell(c2, 4, f"Vendedor: {limpiar_texto(datos_oper['vendedor'])}")
    pdf.cell(c3, 4, f"PM: {limpiar_texto(datos_oper['pm'])}", ln=True)
    # Fila 2 Operación
    pdf.set_x(18)
    pdf.cell(c1, 4, f"Folio Prov: {limpiar_texto(datos_oper['folio_prov'])}")
    pdf.cell(c2, 4, f"Vigencia Precio: {limpiar_texto(datos_oper['vigencia_prov'])}")
    pdf.cell(c3, 4, f"Registro Op: {limpiar_texto(datos_oper['registro_op'])}", ln=True)
    # Fila 3 Operación
    pdf.set_x(18)
    pdf.cell(c1, 4, f"Arrendamiento: {cab['arrendamiento']}")
    pdf.cell(c2, 4, f"Financiera: {limpiar_texto(cab.get('financiera', 'N/A'))}")
    pdf.cell(c3, 4, f"Ejecutivo: {limpiar_texto(cab['ejecutivo'])}", ln=True)

    # --- TABLA DE PRODUCTOS (COSTOS) ---
    pdf.set_xy(15, y_oper + 35)
    pdf.set_fill_color(*azul_corp)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 8)
    
    w_cant, w_desc, w_sku, w_unit, w_total = 10, 100, 25, 25, 25
    pdf.cell(w_cant, 8, "CANT", 0, 0, "C", True)
    pdf.cell(w_desc, 8, " CONCEPTO / DESCRIPCIÓN TÉCNICA", 0, 0, "L", True)
    pdf.cell(w_sku, 8, "SKU", 0, 0, "L", True)
    pdf.cell(w_unit, 8, "COSTO U.", 0, 0, "R", True)
    pdf.cell(w_total, 8, "TOTAL C.", 0, 1, "R", True)

    pdf.set_text_color(*gris_texto)
    for i, (idx, row) in enumerate(df_partidas.iterrows()):
        pdf.set_font("helvetica", "", 8)
        desc_limpia = f"{limpiar_texto(row['Concepto'])}\n{limpiar_texto(row['Descripción'])}"
        lineas = len(pdf.multi_cell(w_desc - 4, 4, desc_limpia, split_only=True))
        h_fila = max((lineas * 4) + 4, 10)

        if pdf.get_y() + h_fila > 250:
            pdf.add_page()
            pdf.set_y(35)
            # Re-encabezado
            pdf.set_fill_color(*azul_corp)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("helvetica", "B", 8)
            pdf.cell(w_cant, 8, "CANT", 0, 0, "C", True)
            pdf.cell(w_desc, 8, " CONCEPTO / DESCRIPCIÓN TÉCNICA", 0, 0, "L", True)
            pdf.cell(w_sku, 8, "SKU", 0, 0, "L", True)
            pdf.cell(w_unit, 8, "COSTO U.", 0, 0, "R", True)
            pdf.cell(w_total, 8, "TOTAL C.", 0, 1, "R", True)
            pdf.set_text_color(*gris_texto)

        y_antes = pdf.get_y()
        pdf.set_fill_color(252, 252, 252) if i % 2 == 0 else pdf.set_fill_color(*gris_suave)
        pdf.rect(15, y_antes, 185, h_fila, "F")

        pdf.set_font("helvetica", "B", 9)
        pdf.cell(w_cant, h_fila, str(int(row['Pzas'])), 0, 0, "C")
        
        x_desc = pdf.get_x()
        pdf.set_xy(x_desc + 2, y_antes + 2)
        pdf.multi_cell(w_desc - 4, 3.5, desc_limpia)
        
        pdf.set_xy(x_desc + w_desc, y_antes)
        pdf.cell(w_sku, h_fila, limpiar_texto(row.get('SKU', '')))
        
        costo_u = row.get('Costo (Sub)', 0)
        costo_t = costo_u * row['Pzas']
        pdf.cell(w_unit, h_fila, f"$ {costo_u:,.2f}", 0, 0, "R")
        pdf.set_font("helvetica", "B", 9)
        pdf.cell(w_total, h_fila, f"$ {costo_t:,.2f}", 0, 1, "R")
        
        pdf.set_y(y_antes + h_fila)

    # --- TOTALES Y FIRMAS ---
    pdf.ln(5)
    if pdf.get_y() > 220: pdf.add_page(); pdf.set_y(35)
    
    total_inversion = (df_partidas['Costo (Sub)'] * df_partidas['Pzas']).sum()
    pdf.set_x(135)
    pdf.set_fill_color(*azul_corp)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(65, 10, f"TOTAL INVERSIÓN: $ {total_inversion:,.2f}  ", 0, 1, "C", True)

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
                st.error("No se encontró configuración para este usuario.")
                st.stop()
            
            # Abrir su hoja personal (ID_SHEET es el estándar en este proyecto)
            sheet_id = datos_user.get('ID_SHEET') or datos_user.get('SPREADSHEET_ID')
            
            if sheet_id:
                st.session_state.sh_personal = gc.open_by_key(sheet_id)
            else:
                # Intento por nombre si no hay ID configurado
                try:
                    st.session_state.sh_personal = gc.open(f"COTIZACIONES_{st.session_state.usuario}")
                except:
                    # Segundo intento: Solo el nombre del usuario (sin prefijo)
                    try:
                        st.session_state.sh_personal = gc.open(st.session_state.usuario)
                    except Exception as e:
                        # Si falla, mostramos un error más claro
                        st.error(f"No se encontró la hoja personal para el usuario: {st.session_state.usuario}")
                        st.info("Asegúrese de que existe una hoja con su nombre y que esté compartida con la cuenta de servicio.")
                        st.stop()
            
            # --- CARGA DE DATOS MAESTROS (PROVEEDORES, TERMINOS, DIRECTORIO) ---
            def cargar_maestro(nombre_ws):
                # 1. Intentar en hoja personal
                try:
                    return st.session_state.sh_personal.worksheet(nombre_ws).get_all_records()
                except:
                    # 2. Intentar en Archivo Central (variaciones de nombre)
                    for file_name in ["TERMINOS_Y_CONDICIONES", "TERMINOS Y CONDICIONES"]:
                        try:
                            sh_m = gc.open(file_name)
                            # Intentar por nombre solicitado, luego Hoja1, luego el primer sheet disponible
                            for ws_name in [nombre_ws, "Hoja1", "HOJA1", "Sheet1"]:
                                try:
                                    recs = sh_m.worksheet(ws_name).get_all_records()
                                    if recs: return recs
                                except: continue
                            return sh_m.get_worksheet(0).get_all_records()
                        except: continue
                    
                    # 3. Intentar como archivo independiente
                    try:
                        return gc.open(nombre_ws).sheet1.get_all_records()
                    except:
                        return []

            st.session_state.directorio = cargar_maestro("DIRECTORIO")
            st.session_state.terminos_db = cargar_maestro("TERMINOS")
            st.session_state.proveedores_db = cargar_maestro("PROVEEDORES")
                
            st.session_state.datos_usuario_listos = True
    except Exception as e:
        st.error(f"Error al conectar con la base de datos personal: {e}")
        st.stop()

# --- 1. CARGA SÓLO USUARIOS PARA LOGIN ---
def cargar_usuarios_login():
    if 'usuarios_db' not in st.session_state:
        try:
            gc = conectar_google_sheets()
            # Cargamos solo la hoja de usuarios al inicio para validar el acceso
            st.session_state.usuarios_db = gc.open("CONTROL_USUARIOS").sheet1.get_all_records()
        except Exception as e:
            st.error(f"Error cargando lista de acceso: {e}")

def cargar_cotizacion_para_editar(row, df_resumen):
    """Extrae toda la lógica de carga de una cotización de forma robusta."""
    # 1. Normalizar nombres de columnas del DataFrame de entrada
    row_norm = {str(k).upper().replace(" ", "_"): v for k, v in row.items()}
    
    # 2. Identificar columnas clave
    f_id = str(row_norm.get('FOLIO', next(iter(row.values()), ""))).strip()
    
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
    st.session_state.ejecutivo_nom = str(row_norm.get('EJECUTIVO', st.session_state.usuario))
    st.session_state.cliente_sel = str(row_norm.get('CLIENTE', row_norm.get('RAZON_SOCIAL', 'Seleccionar...')))
    st.session_state.contacto_sel = str(row_norm.get('CONTACTO', 'Seleccionar...'))
    st.session_state.entrega_val = str(row_norm.get('TIEMPO_DE_ENTREGA', row_norm.get('ENTREGA', 'Seleccionar...')))
    st.session_state.pago_val = str(row_norm.get('FORMA_DE_PAGO', row_norm.get('PAGO', 'Seleccionar...')))
    st.session_state.condic_val = str(row_norm.get('CONDICIONES', row_norm.get('CONDICIONES_ESPECIALES', 'Seleccionar...')))
    st.session_state.coment_val = str(row_norm.get('COMENTARIOS', row_norm.get('RESUMEN', '')))
    st.session_state.moneda_val = str(row_norm.get('MONEDA', 'MXN'))
    st.session_state.tc_val = float(row_norm.get('TC', row_norm.get('TIPO_DE_CAMBIO', 1.0)))

    try:
        val_v = str(row_norm.get('VIGENCIA', ''))
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
    gc = conectar_google_sheets()
    resumen_escaneo = []
    
    with st.spinner("Escaneando ecosistema de proyectos..."):
        for u in st.session_state.usuarios_db:
            nombre_ej = u.get('NOMBRE', 'Ejecutivo')
            sheet_id = str(u.get('ID_SHEET') or u.get('SPREADSHEET_ID') or "").strip()
            
            try:
                # 1. Apertura del Sheet
                sh_target = None
                try:
                    if sheet_id:
                        sh_target = gc.open_by_key(sheet_id)
                    else:
                        sh_target = gc.open(f"COTIZACIONES_{u['USUARIO']}")
                except:
                    try:
                        sh_target = gc.open(u['USUARIO'])
                    except:
                        pass
                
                if not sh_target:
                    resumen_escaneo.append(f"❌ {nombre_ej}: Sin acceso")
                    continue

                # 2. Localización de la pestaña
                ws_dir = None
                for name in ["DIRECTORIO", "Directorio", "PROSPECTOS", "CLIENTES", "HOJA1", "SHEET1"]:
                    try:
                        ws_dir = sh_target.worksheet(name)
                        break
                    except:
                        continue
                
                if not ws_dir:
                    resumen_escaneo.append(f"❓ {nombre_ej}: Sin pestaña")
                    continue

                # 3. Búsqueda de datos
                datos_dir = ws_dir.get_all_records()
                resumen_escaneo.append(f"✅ {nombre_ej}: {len(datos_dir)} reg.")
                
                for d in datos_dir:
                    # Unir toda la fila para búsqueda profunda
                    toda_la_fila = " ".join([str(v) for v in d.values()])
                    if q_norm in normalizar(toda_la_fila):
                        # Mapeo de campos normalizado
                        d_norm = {str(k).upper().replace(" ", "_"): v for k, v in d.items()}
                        res_final = {
                            'RAZON_SOCIAL': d_norm.get('RAZON_SOCIAL', d_norm.get('CLIENTE', 'N/A')),
                            'CONTACTO': d_norm.get('CONTACTO', d_norm.get('ATENCION', 'N/A')),
                            'EMAIL': d_norm.get('EMAIL', 'N/A'),
                            'TELEFONO': d_norm.get('TELEFONO', 'N/A'),
                            'EJECUTIVO_DUEÑO': nombre_ej
                        }
                        
                        # Intentar obtener la última actividad (Opcional)
                        try:
                            ws_res = sh_target.worksheet("COTIZACIONES_RESUMEN")
                            recs_res = ws_res.get_all_records()
                            res_final['ULTIMA_ACTIVIDAD'] = recs_res[-1].get('FECHA_ELABORACION', 'N/A') if recs_res else "Sin cotizaciones"
                        except:
                            res_final['ULTIMA_ACTIVIDAD'] = "N/A"
                        
                        resultados.append(res_final)

            except Exception as e:
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

# --- ACTIVACIÓN DE RECEPCIÓN DE DRIVE (OAUTH) ---
# Esta función debe correr antes de que Streamlit detenga el flujo por el Login
procesar_callback_oauth()

# --- LÓGICA DE LOGIN ---
if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    cargar_usuarios_login()
    
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

        # Botón de vinculación de Drive si falla el token
        if not obtener_drive_service():
            st.warning("⚠️ CONEXIÓN DE DRIVE PENDIENTE")
            autenticar_usuario_oauth()
            st.divider()

        # --- NAVEGACIÓN DE VISTAS ---
        if st.session_state.menu_actual == 'ovo':
            renderizar_buscador_ovo()

        elif st.session_state.menu_actual == 'menu':
            st.title(f"Panel de Control - {st.session_state.usuario}")
            # 1. BOTONES DE ACCIÓN
            col_acc1, col_acc2, _ = st.columns([1, 1, 1])
            with col_acc1:
                if st.button("Crear Cotización Nueva", use_container_width=True, type="primary"):
                    keys_to_reset = ['folio_val', 'vigencia_val', 'entrega_val', 'pago_val', 'condic_val', 'coment_val', 'df_partidas', 'dict_fotos', 'dict_fotos_links', 'registro_exitoso']
                    for k in keys_to_reset:
                        if k in st.session_state: del st.session_state[k]
                    st.session_state.menu_actual = 'nuevo'
                    st.rerun()
            
            with col_acc2:
                if st.button("Buscador de Vínculos y Operaciones", use_container_width=True):
                    st.session_state.menu_actual = 'ovo'
                    st.rerun()
            
            st.divider()

            # 2. CÁLCULO DE MÉTRICAS (KPIs) e HISTORIAL
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
                    col_monto_src = next((c for c in df_det_norm.columns if "VENTA_TOTAL_CON_IVA" in c or "PFACTURA_TOTAL_IVA_INC" in c), df_det_norm.columns[-3])
                    col_util_src = next((c for c in df_det_norm.columns if "UTILIDAD_TOTAL" in c), df_det_norm.columns[-1])

                    df_montos_util = df_det_norm.groupby(col_folio_det).agg({
                        col_monto_src: lambda x: pd.to_numeric(x, errors='coerce').sum(),
                        col_util_src: lambda x: pd.to_numeric(x, errors='coerce').sum()
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
                            
                            conceptos = ", ".join(det_f['CONCEPTO'].unique()) if 'CONCEPTO' in det_f.columns else "N/A"
                            proveedores = ", ".join(det_f['PROVEEDOR'].unique()) if 'PROVEEDOR' in det_f.columns else "N/A"
                            estatus = row.get('ESTATUS', 'N/A')
                            if estatus == 'N/A' and len(row) > 13: estatus = row.iloc[13]
                            
                            col_venta = 'PFACTURA_TOTAL_IVA_INC' if 'PFACTURA_TOTAL_IVA_INC' in det_f.columns else det_f.columns[-3]
                            try: monto_total = pd.to_numeric(det_f[col_venta], errors='coerce').sum()
                            except: monto_total = 0.0

                            label = f"Folio: {f_id} | {row[col_cliente]} | ${monto_total:,.2f} | {estatus}"
                            with st.expander(label):
                                c1, c2, c3 = st.columns(3)
                                c1.write(f"**Fecha:** {row.get('FECHA_ELABORACION', 'N/A')}")
                                c1.write(f"**Estatus:** {estatus}")
                                c2.write(f"**Atención:** {row.get('CONTACTO', 'N/A')}")
                                c2.write(f"**Monto Total:** ${monto_total:,.2f}")
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
            st.title(f"Formalizar Pedido: {st.session_state.folio_val}")
            
            # Obtener datos del primer proveedor de la cotización para pre-llenar (si existe)
            df_p_actual = st.session_state.df_partidas
            prov_principal = df_p_actual["Proveedor"].iloc[0] if not df_p_actual.empty else ""
            
            # Buscar contactos en la DB de proveedores
            info_prov = next((p for p in st.session_state.get('proveedores_db', []) if p['PROVEEDOR'] == prov_principal), {})
            vendedor_prov = info_prov.get('VENDEDOR', '')
            pm_prov = info_prov.get('PRODUCT_MANAGER', '')

            with st.form("form_pedido_completo"):
                # --- SECCIÓN 1: DATOS DE PROVEEDOR ---
                st.markdown("### 🏢 Datos de Proveedor / Operación")
                p1, p2, p3 = st.columns(3)
                with p1:
                    ejecutivo_ped = st.text_input("Ejecutivo Solicitante:", value=st.session_state.get('ejecutivo_nom', ''), disabled=True)
                    proveedor_ped = st.text_input("Proveedor:", value=prov_principal, disabled=True)
                    vendedor_ped = st.text_input("Vendedor Proveedor:", value=vendedor_prov)
                with p2:
                    fecha_ped = st.date_input("Fecha de Solicitud:", value=date.today(), disabled=True)
                    prioridad_ped = st.selectbox("Prioridad:", ["Normal", "Urgente"])
                    pm_ped = st.text_input("Product Manager (PM):", value=pm_prov)
                with p3:
                    vigencia_prov_ped = st.text_input("Vigencia Precio Prov:", placeholder="Ej. 15 días / 30 Abr")
                    registro_op_ped = st.text_input("Núm. Registro Oportunidad:")
                    folio_prov_ped = st.text_input("Folio Cotización Proveedor:")

                pa1, pa2 = st.columns(2)
                with pa1:
                    es_arrendamiento = st.radio("¿Es Arrendamiento?", ["No", "Sí"], horizontal=True)
                with pa2:
                    financiera = st.text_input("Financiera involucrada:", disabled=(es_arrendamiento == "No"))

                st.divider()

                # --- SECCIÓN 2: DATOS DE FACTURACIÓN ---
                st.markdown("### 📄 Datos de Facturación (Cliente)")
                f1, f2 = st.columns(2)
                with f1:
                    razon_social_ped = st.text_input("Razón Social:", value=st.session_state.get('cliente_sel', ''), disabled=True)
                    rfc_ped = st.text_input("RFC del Cliente:")
                with f2:
                    usos_cfdi = [
                        "G03 - Gastos en general", "P01 - Por definir", "I01 - Construcciones", 
                        "I02 - Mobiliario y equipo de oficina", "I04 - Equipo de cómputo", 
                        "I08 - Otros", "G01 - Adquisición de mercancías", "D01 - Honorarios médicos"
                    ]
                    uso_cfdi_ped = st.selectbox("Uso de CFDI:", usos_cfdi)
                    metodo_pago_ped = st.selectbox("Método de Pago:", ["PUE - Pago en una sola exhibición", "PPD - Pago en parcialidades o diferido"])

                st.divider()

                # --- SECCIÓN 3: DATOS DE LOGÍSTICA ---
                st.markdown("### 🚚 Datos de Logística / Entrega")
                l1, l2, l3 = st.columns([2, 1, 1])
                with l1:
                    dir_entrega_ped = st.text_area("Dirección de Entrega:", value=st.session_state.get('entrega_val', ''))
                with l2:
                    persona_recibe_ped = st.text_input("Persona que recibe:")
                with l3:
                    tel_contacto_ped = st.text_input("Teléfono en sitio:")
                
                num_po_ped = st.text_input("Número de Orden de Compra (PO):")

                st.divider()
                if st.form_submit_button("VALIDAR Y FORMALIZAR PEDIDO", use_container_width=True, type="primary"):
                    if not rfc_ped or not persona_recibe_ped or not tel_contacto_ped:
                        st.error("Por favor completa los campos obligatorios de Facturación y Logística.")
                    else:
                        try:
                            with st.spinner("Registrando Pedido Maestro..."):
                                sh = st.session_state.sh_personal
                                try: ws_ped = sh.worksheet("PEDIDOS")
                                except: ws_ped = sh.add_worksheet(title="PEDIDOS", rows="100", cols="25")
                                
                                # Definir encabezados extendidos si es nueva
                                headers = [
                                    "FECHA", "FOLIO_INTERNO", "CLIENTE", "RFC", "USO_CFDI", "METODO_PAGO", 
                                    "PROVEEDOR", "VENDEDOR_PROV", "PM_PROV", "PRIORIDAD", "VIGENCIA_PROV", 
                                    "REGISTRO_OP", "FOLIO_PROV", "ARRENDAMIENTO", "FINANCIERA", "PO", 
                                    "DIRECCION_ENTREGA", "RECIBE", "TEL_SITIO", "EJECUTIVO", "ESTATUS", "LINK_PDF_TECNICO"
                                ]
                                if ws_ped.row_count == 1 and not ws_ped.cell(1,1).value:
                                    ws_ped.update("A1", [headers])

                                # Generar PDF Técnico con los nuevos rubros
                                cab_ped = {
                                    "folio": st.session_state.folio_val,
                                    "ejecutivo": st.session_state.ejecutivo_nom,
                                    "cliente": st.session_state.cliente_sel,
                                    "proveedor": prov_principal,
                                    "prioridad": prioridad_ped,
                                    "arrendamiento": es_arrendamiento,
                                    "financiera": financiera
                                }
                                
                                datos_fisc = {"rfc": rfc_ped, "razon_fiscal": razon_social_ped, "uso_cfdi": uso_cfdi_ped, "metodo_pago": metodo_pago_ped}
                                datos_log = {"dir_entrega": dir_entrega_ped, "persona_recibe": persona_recibe_ped, "tel_contacto": tel_contacto_ped, "num_po": num_po_ped}
                                datos_oper = {"vendedor": vendedor_ped, "pm": pm_ped, "vigencia_prov": vigencia_prov_ped, "registro_op": registro_op_ped, "folio_prov": folio_prov_ped}
                                
                                # Combinar datos para el PDF
                                pdf_tecnico_blob = generar_pedido_tecnico_blob_v2(cab_ped, df_p_actual, datos_fisc, datos_log, datos_oper)
                                link_pdf_t = subir_archivo_a_drive(pdf_tecnico_blob, f"PEDIDO_TECNICO_{st.session_state.folio_val}.pdf")

                                # Registro en Sheet
                                row_ped = [
                                    str(date.today()), st.session_state.folio_val, st.session_state.cliente_sel, rfc_ped, uso_cfdi_ped, metodo_pago_ped,
                                    prov_principal, vendedor_ped, pm_ped, prioridad_ped, vigencia_prov_ped,
                                    registro_op_ped, folio_prov_ped, es_arrendamiento, financiera, num_po_ped,
                                    dir_entrega_ped, persona_recibe_ped, tel_contacto_ped, st.session_state.usuario, "FORMALIZADO", link_pdf_t
                                ]
                                ws_ped.append_row(row_ped)

                                # Actualizar estatus
                                ws_res = sh.worksheet("COTIZACIONES_RESUMEN")
                                folios = ws_res.col_values(1)
                                if st.session_state.folio_val in folios:
                                    idx = folios.index(st.session_state.folio_val) + 1
                                    ws_res.update_cell(idx, 14, "100% Ganada")

                                # Remisión
                                st.session_state.remision_actual = generar_remision_blob(cab_ped, df_p_actual, st.session_state.get('dict_fotos', {}), st.session_state.get('dict_fotos_links', {}))
                                
                                st.session_state.pedido_exitoso = True
                                st.session_state.pdf_tecnico_actual = pdf_tecnico_blob
                                st.balloons()
                                st.rerun()
                        except Exception as e:
                            st.error(f"Error al formalizar: {e}")

            if st.session_state.get('pedido_exitoso'):
                st.success("✅ Pedido registrado y formalizado con éxito.")
                c_ped1, c_ped2 = st.columns(2)
                with c_ped1:
                    st.download_button("Descargar Pedido Técnico (Admin)", data=st.session_state.pdf_tecnico_actual, file_name=f"PEDIDO_TECNICO_{st.session_state.folio_val}.pdf", use_container_width=True, type="primary")
                with c_ped2:
                    st.download_button("Descargar Remisión (Entrega)", data=st.session_state.remision_actual, file_name=f"Remision_{st.session_state.folio_val}.pdf", use_container_width=True)
                
                if st.button("Volver al Dashboard", use_container_width=True):
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
                
                def buscar_index(lista, valor):
                    if not valor or valor not in lista: return 0
                    try: return lista.index(valor)
                    except: return 0

                with col_e1:
                    ejecutivos_lista = [u['NOMBRE'] for u in st.session_state.usuarios_db]
                    idx_ej = buscar_index(ejecutivos_lista, st.session_state.get('ejecutivo_nom'))
                    ejecutivo_nom = st.selectbox("Ejecutivo que firma:", ["Seleccionar..."] + ejecutivos_lista, index=idx_ej, key="ejecutivo_sel")
                    st.session_state.ejecutivo_nom = ejecutivo_nom
                
                v_tel, v_mail = ("", "")
                if ejecutivo_nom != "Seleccionar...":
                    d = next(u for u in st.session_state.usuarios_db if u['NOMBRE'] == ejecutivo_nom)
                    v_tel, v_mail = d['TELEFONO'], d['EMAIL']

                with col_e2:
                    tel_e = st.text_input("Teléfono:", value=v_tel)
                with col_e3:
                    mail_e = st.text_input("Email:", value=v_mail)

                st.divider()
                col_c1, col_c2 = st.columns(2)
                with col_c1:
                    lista_rs = sorted(list(set([c['RAZON_SOCIAL'] for c in st.session_state.directorio if c['RAZON_SOCIAL']])))
                    idx_rs = buscar_index(lista_rs, st.session_state.get('cliente_sel'))
                    cliente_sel = st.selectbox("Razón Social:", ["Seleccionar..."] + lista_rs, index=idx_rs, key="cliente_sel_widget")
                    
                    # DISPARADOR DE FOLIO AUTOMÁTICO
                    if cliente_sel != "Seleccionar..." and cliente_sel != st.session_state.get('cliente_sel_ant'):
                        with st.spinner("Calculando nuevo folio..."):
                            nuevo_folio = generar_folio_automatico(cliente_sel, st.session_state.usuario)
                            if nuevo_folio:
                                st.session_state.folio_val = nuevo_folio
                                st.session_state.cliente_sel_ant = cliente_sel
                                st.rerun()
                    
                    st.session_state.cliente_sel = cliente_sel
                
                with col_c2:
                    opciones_c = ["Seleccionar..."]
                    if cliente_sel != "Seleccionar...":
                        contactos = [c['CONTACTO'] for c in st.session_state.directorio if c['RAZON_SOCIAL'] == cliente_sel]
                        opciones_c += contactos
                        idx_cont = buscar_index(contactos, st.session_state.get('contacto_sel'))
                        contacto_sel = st.selectbox("Atención a:", opciones_c, index=idx_cont, key="contacto_sel_widget")
                        st.session_state.contacto_sel = contacto_sel
                    else:
                        st.selectbox("Atención a:", ["Seleccionar..."], disabled=True)
                        contacto_sel = "Seleccionar..."

                st.divider()
                col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 1, 1, 1])
                with col_f1:
                    # Usar directamente st.session_state.folio_val en el widget
                    if 'folio_val' not in st.session_state: st.session_state.folio_val = ""
                    folio = st.text_input("Folio de Cotización:", key="folio_val")
                
                with col_f2:
                    moneda_opciones = ["MXN", "USD"]
                    idx_moneda = buscar_index(moneda_opciones, st.session_state.get('moneda_val', 'MXN'))
                    moneda = st.selectbox("Moneda Cotización:", moneda_opciones, index=idx_moneda, key="moneda_val_sel")
                    st.session_state.moneda_val = moneda

                with col_f3:
                    # Mostrar TC solo si la cotización es MXN o si hay costos en USD (siempre visible es más seguro)
                    val_tc = st.session_state.get('tc_val', 18.00)
                    tc = st.number_input("Tipo de Cambio:", value=val_tc, format="%.2f", key="tc_val_input")
                    st.session_state.tc_val = tc

                with col_f4:
                    val_vigencia = st.session_state.get('vigencia_val', date.today())
                    vigencia = st.date_input("Vigencia:", value=val_vigencia, key="vigencia_val")

                st.divider()
                col_t1, col_t2, col_t3 = st.columns(3)
                
                # Función para extraer columna de forma robusta
                def ext_col(db, key_pref):
                    if not db: return []
                    keys = list(db[0].keys())
                    # Buscar coincidencia exacta o parcial ignorando mayúsculas y espacios
                    match = next((k for k in keys if key_pref.upper() in k.upper().replace(" ", "_")), None)
                    if match: 
                        # Extraer valores, quitar vacíos y duplicados, y ordenar
                        vals = [str(t[match]) for t in db if t.get(match)]
                        return sorted(list(set(vals)))
                    return []

                with col_t1:
                    opciones_entrega = ext_col(st.session_state.terminos_db, "ENTREGA")
                    idx_e = buscar_index(opciones_entrega, st.session_state.get('entrega_val'))
                    entrega = st.selectbox("Entrega:", ["Seleccionar..."] + opciones_entrega, index=idx_e, key="entrega_val_sel")
                    st.session_state.entrega_val = entrega
                with col_t2:
                    opciones_pago = ext_col(st.session_state.terminos_db, "PAGO")
                    idx_p = buscar_index(opciones_pago, st.session_state.get('pago_val'))
                    pago = st.selectbox("Pago:", ["Seleccionar..."] + opciones_pago, index=idx_p, key="pago_val_sel")
                    st.session_state.pago_val = pago
                with col_t3:
                    opciones_cond = ext_col(st.session_state.terminos_db, "CONDICIONES")
                    idx_c = buscar_index(opciones_cond, st.session_state.get('condic_val'))
                    condic = st.selectbox("Condiciones Especiales:", ["Seleccionar..."] + opciones_cond, index=idx_c, key="condic_val_sel")
                    st.session_state.condic_val = condic
                
                # Comentarios eliminados de aquí (se movieron a Finalizar)

            with tab2:
                st.subheader("Análisis de Partidas")
                # Lógica del editor (se mantiene igual, solo dentro del tab)
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

                    # --- LÓGICA DE CONVERSIÓN MULTIMONEDA ---
                    tc = st.session_state.get('tc_val', 1.0)
                    moneda_cot = st.session_state.get('moneda_val', 'MXN')

                    def normalizar_a_cotizacion(precio, moneda_item):
                        if moneda_cot == "MXN" and moneda_item == "USD": return precio * tc
                        if moneda_cot == "USD" and moneda_item == "MXN": return precio / tc
                        return precio

                    # Aplicar conversión a PM y Envíos antes del cálculo
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
                        # Envío (Gris oscuro para separar)
                        styler.set_properties(subset=["Envío Local (Unit)", "Envío Local (Total)"], 
                                            **{'background-color': '#424242', 'color': 'white'})
                        # Ventas (Amarillo)
                        styler.set_properties(subset=["venta unitaria sin iva", "venta unitaria con iva", "venta total sin iva", "venta total con iva"], 
                                            **{'background-color': '#FBC02D', 'color': 'black'})
                        # Utilidad (Morado)
                        styler.set_properties(subset=["utilidad total"], 
                                            **{'background-color': '#4A148C', 'color': 'white'})
                        return styler

                    st.write("Análisis Detallado de Partidas")
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
                    st.markdown(f"**Vigencia:** {vigencia}")
                with c_r2:
                    st.markdown(f"**Pago:** {st.session_state.get('pago_val', 'No seleccionado')}")
                    st.markdown(f"**Entrega:** {st.session_state.get('entrega_val', 'No seleccionado')}")
                    st.markdown(f"**Condiciones:** {st.session_state.get('condic_val', 'No seleccionado')}")
                    st.markdown(f"**Partidas:** {len(df_analisis)} conceptos")

                st.write("**Conceptos a registrar:**", ", ".join(df_analisis["Concepto"].tolist()))
                
                st.divider()
                val_coment = st.session_state.get('coment_val', "")
                comentarios = st.text_area("✍️ Comentarios / Resumen Ejecutivo:", value=val_coment, key="coment_val_input", help="Estos comentarios se guardarán en el historial y son útiles para el seguimiento.")
                st.session_state.coment_val = comentarios

                if st.button("CONFIRMAR Y GUARDAR TODO", use_container_width=True, type="primary"):
                    if "Seleccionar..." in [ejecutivo_nom, cliente_sel, contacto_sel, entrega, pago, condic] or not st.session_state.folio_val:
                        st.error("Revisa que todos los campos obligatorios en 'Generales' estén llenos.")
                    else:
                        try:
                            with st.spinner("Guardando en Sheets y Drive..."):
                                folio_actual = st.session_state.folio_val
                                
                                dict_links_drive = st.session_state.get('dict_fotos_links', {}).copy()
                                if st.session_state.dict_fotos:
                                    for idx_f, bytes_f in st.session_state.dict_fotos.items():
                                        bytes_f.seek(0)
                                        link_d = subir_archivo_a_drive(bytes_f.read(), f"Partida_{folio_actual}_{idx_f}.png", 'image/png')
                                        if link_d: dict_links_drive[idx_f] = link_d
                                
                                dict_evidencias_drive = st.session_state.get('dict_evidencias_links', {}).copy()
                                if st.session_state.get('dict_evidencias'):
                                    for idx_ev, file_ev in st.session_state.dict_evidencias.items():
                                        file_ev.seek(0)
                                        link_ev = subir_archivo_a_drive(file_ev.read(), f"Evidencia_{folio_actual}_{idx_ev}_{file_ev.name}", file_ev.type)
                                        if link_ev: dict_evidencias_drive[idx_ev] = link_ev

                                ws_res = st.session_state.sh_personal.worksheet("COTIZACIONES_RESUMEN")
                                folios_res = ws_res.col_values(1)
                                estatus_final = "60% Propuesta"
                                
                                if str(folio_actual) in [str(f) for f in folios_res]:
                                    idx_fila = [str(f) for f in folios_res].index(str(folio_actual)) + 1
                                    try:
                                        headers = ws_res.row_values(1)
                                        col_est_idx = headers.index("ESTATUS") + 1
                                        estatus_actual = ws_res.cell(idx_fila, col_est_idx).value
                                        if estatus_actual: estatus_final = estatus_actual
                                    except: pass
                                    ws_res.delete_rows(idx_fila)

                                ws_det = st.session_state.sh_personal.worksheet("COTIZACIONES_DETALLE")
                                folios_det = ws_det.col_values(1)
                                filas_a_borrar = [i + 1 for i, val in enumerate(folios_det) if str(val) == str(folio_actual)]
                                if filas_a_borrar:
                                    for fila in reversed(filas_a_borrar): ws_det.delete_rows(fila)

                                cab = {
                                    "folio": folio_actual, "ejecutivo": ejecutivo_nom, "email": mail_e, "tel": tel_e, 
                                    "cliente": cliente_sel, "contacto": contacto_sel, "vigencia": str(vigencia), 
                                    "entrega": entrega, "pago": pago, "condiciones": condic,
                                    "moneda": st.session_state.get('moneda_val', 'MXN'),
                                    "tc": st.session_state.get('tc_val', 1.0)
                                }
                                pdf_blob = generar_pdf_blob(cab, df_analisis, st.session_state.dict_fotos, dict_links_drive)

                                nombre_pdf = f"{folio_actual}.pdf"
                                link_pdf_drive = subir_archivo_a_drive(pdf_blob, nombre_pdf, 'application/pdf')
                                
                                # Registro en Resumen (Persistencia de Moneda y TC en O y P)
                                datos_res = [
                                    folio_actual, ejecutivo_nom, mail_e, tel_e, str(date.today()), 
                                    link_pdf_drive, cliente_sel, contacto_sel, str(vigencia), 
                                    entrega, pago, condic, comentarios, estatus_final,
                                    st.session_state.get('moneda_val', 'MXN'),
                                    st.session_state.get('tc_val', 1.0)
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

