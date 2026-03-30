# Estado del Proyecto - APP CO5O

Este archivo sirve de memoria para que el agente Gemini CLI retome el trabajo sin perder contexto.

## 🛠️ Funcionalidades Implementadas
- **Autenticación Híbrida:** 
  - Google Sheets usa Cuenta de Servicio (JSON) para mantener datos en la "NUBE CIP".
  - Google Drive usa OAuth (Cuentas personales) para que fotos y PDFs usen el espacio de cada ejecutivo.
- **Dashboard de Inicio:** 
  - Vista unificada que muestra métricas de desempeño (Cliente más cotizado, total de cotizaciones).
  - Historial de las últimas 10 cotizaciones con buscador por folio/cliente.
  - Botón de edición directa que carga partidas, imágenes y datos de contacto del emisor.
- **Módulo "Meter Pedido" (Formalización):**
  - Formulario completo para capturar datos de Facturación (RFC, Razón Social, Uso CFDI, Método Pago).
  - Captura de datos de Logística (Dirección, Contacto, Teléfono, PO).
  - **Generación de PDF Técnico:** Crea automáticamente un documento para Administración/Compras con desglose de costos, proveedores y datos de entrega.
  - **Integración con Nube:** Registro automático en la pestaña "PEDIDOS" con link directo al PDF en Drive.
  - **Actualización de Estatus:** Cambia automáticamente el estatus de la cotización a "100% Pedido".
- **Interfaz y Diseño:**
  - Fondo: `#030303` (Negro absoluto).
  - Pestañas: Centradas, anchas (220px) y sin emojis.
  - Estilo: Corporativo sobrio, sin emojis en botones ni mensajes.
  - Enfoque de inputs: Azul (`#3498DB`).
  - Tabla Financiera: Desglose total con colores (Verde para Costos, Amarillo para Ventas, Morado para Utilidad).

## 📁 Archivos de Configuración Listos
- `requirements.txt`: Lista de librerías para la nube.
- `.gitignore`: Configurado para proteger archivos JSON y tokens.

## 🚀 Próximos Pasos (El Plan)
1. **Despliegue en la Nube:** Pendiente hasta que los ejecutivos tengan sus cuentas de Google listas.

## 📌 Notas de Estilo
- Máxima seriedad: Cero emojis.
- Alta definición: CSS aplicado para evitar letras borrosas en fondos oscuros.
