FROM python:3.11-slim

WORKDIR /app

# ─── تثبيت المتطلبات فقط (لا يحتاج secrets) ───
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ─── نسخ كود التطبيق ───
COPY . .

# ─── المنفذ من Railway ───
EXPOSE ${PORT:-8501}

# ─── التشغيل: secrets تُقرأ فقط هنا (runtime) ───
CMD streamlit run app.py \
    --server.port=${PORT:-8501} \
    --server.address=0.0.0.0 \
    --server.headless=true
