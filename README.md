# Voia Vector Service

Este proyecto es un microservicio basado en **FastAPI** que permite extraer, vectorizar e indexar documentos en una base de datos de vectores (Qdrant). Este servicio es parte del ecosistema de **Voia**, una plataforma que permite crear y entrenar bots personalizados con IA, capaces de responder en tiempo real con datos embebidos.

---

## 🚀 Funcionalidad principal

* 📄 Procesamiento de documentos PDF.
* 🔍 Extracción de texto con OCR y parser.
* 🧠 Embedding de texto usando modelos como OpenAI o HuggingFace.
* 🗂 Indexación en Qdrant para búsquedas semánticas.
* 🔁 Evita indexaciones duplicadas (checksum).

---

## 🧪 Endpoints disponibles

### `POST /process-documents/`

Procesa y vectoriza documentos PDF existentes en el directorio `/Uploads/Documents/`.

#### Ejemplo de llamada desde otro microservicio:

```bash
POST http://localhost:8000/process-documents/
```

---

## 🛠 Requisitos

* Python 3.10+
* Qdrant en local o en la nube
* Archivo `.env` con configuración del embedder y Qdrant

---

## 📦 Instalación

```bash
# Clona el repositorio
git clone https://github.com/ivancho1212/voia_vector_services.git
cd voia_vector_services

# Crea un entorno virtual
python3 -m venv venv
source venv/bin/activate  # o venv\Scripts\activate en Windows

# Instala dependencias
pip install -r requirements.txt
```

---

## ⚙️ Archivo `.env` esperado

```env
EMBEDDER_PROVIDER=openai             # o huggingface
OPENAI_API_KEY=tu_clave
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=voia_vectors
```

---

## ▶️ Ejecutar servidor

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

---

## 🧠 Arquitectura general (resumen de flujo)

1. Se crea una plantilla de bot y se entrena con roles `system`, `user`, `assistant`.
2. Se cargan documentos, URLs o texto plano que alimentan la base de conocimiento del bot.
3. Este servicio los procesa y vectoriza.
4. El bot se crea con estilos personalizados.
5. Desde un widget embebido, los usuarios pueden interactuar con el bot en una web pública.
6. La IA (o eventualmente un humano) responde usando la base vectorizada y el historial de entrenamiento.

---

## 🧑‍💻 Autor

**Iván Daniel Herrera Surmay**
[LinkedIn](https://www.linkedin.com/in/ivan-herrera-surmay)

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT.
