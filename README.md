# Quant-Analytics-App



This project consists of:
- **FastAPI backend** (Uvicorn) running from the project root
- **Streamlit dashboard** running from `frontend/`

---

## Prerequisites

- Python 3.10+
- Virtual environment (recommended)

---

## Setup

Create and activate a virtual environment:

```bash
python -m venv venv
```


**Windows**

```bash
venv\Scripts\activate
```

**Linux / macOS**

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Run Backend (FastAPI)

From the **project root**:

```bash
python -m uvicorn app:app --reload
```

Backend will be available at:

```
http://127.0.0.1:8000
```

---

## Run Frontend (Streamlit)

From the **project root**:

```bash
cd frontend
streamlit run dashboard.py
```

Streamlit app will be available at:

```
http://localhost:8501
```

