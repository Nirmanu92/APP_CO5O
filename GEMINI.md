# Estado del Proyecto - APP CO5O (PRODUCCIÓN)

## 🚀 Despliegue en la Nube (Completado)
- **Plataforma:** Streamlit Community Cloud.
- **URL:** https://appco5o-gunixkfb5hakxc6r5ufshk.streamlit.app/
- **Repositorio:** Nirmanu92/APP_CO5O (Sincronizado vía GitHub).

## 🛠️ Arquitectura de Autenticación
- **Google Sheets:** Usa Cuenta de Servicio (`app-coso@manuel-hernandez.iam.gserviceaccount.com`).
- **Google Drive:** Sistema de OAuth Híbrido.
  - Los tokens de los ejecutivos se guardan de forma persistente en la hoja `CONTROL_USUARIOS` (columna `TOKEN_DRIVE`).
  - Esto evita que la conexión se pierda cuando la app se reinicia en la nube.
- **Seguridad:** Los secretos sensibles se manejan exclusivamente a través de los *Secrets* de Streamlit.

## ✅ Funcionalidades Activas
- Dashboard de Desempeño con gráficas de Plotly.
- Generación de PDFs (Cotización, Remisión, Pedido Técnico).
- Carga de imágenes directa a Drive desde el navegador.
- Persistencia de datos en Sheets (Nube CIP).

## 📌 Próximos Pasos
1. **Monitoreo:** Validar que los PDFs se guarden correctamente en las carpetas de los nuevos ejecutivos.
2. **Refinamiento:** Ajustar detalles estéticos si los usuarios lo solicitan.

## ⚠️ Notas Importantes
- Siempre compartir carpetas de Drive con el correo de la cuenta de servicio antes de vincular.
- No subir archivos JSON al repositorio de GitHub.
