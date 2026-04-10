#!/bin/bash
# start.sh — Lanza FastAPI + Streamlit + nginx

# Iniciar FastAPI en background (puerto 8000)
uvicorn api_mapa:app --host 127.0.0.1 --port 8000 --workers 2 --log-level warning &

# Iniciar Streamlit en background (puerto 8502, interno)
streamlit run app.py \
    --server.port=8502 \
    --server.address=127.0.0.1 \
    --server.headless=true \
    --browser.gatherUsageStats=false &

# Esperar a que ambos arranquen
sleep 3

# Iniciar nginx en foreground (puerto 8501 — CapRover lo expone)
nginx -g 'daemon off;'
