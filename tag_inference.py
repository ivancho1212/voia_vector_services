import re
from typing import Dict, List

def infer_tags_from_payload(payload: Dict, extracted_text: str = "") -> Dict:
    tags = {}

    file_name = payload.get("file_name", "").lower()
    text = extracted_text.lower()

    def match_keywords(text: str, keywords: List[str]) -> bool:
        return any(re.search(rf"\b{re.escape(kw)}\b", text) for kw in keywords)

    # -----------------------------
    # Tipos de documento
    # -----------------------------
    tipos_documento = {
        "autorización": ["autorizacion", "autorización", "autorisacion", "autorizacón", "autorizaciòn"],
        "contrato": ["contrato", "contarto", "contratp", "contratto"],
        "certificado": ["certificado", "certificdo", "cert", "constancia"],
        "factura": ["factura", "recibo", "cuenta de cobro"],
        "información empresarial": ["informacion", "quienes somos", "perfil empresarial", "presentación", "empresa"]
    }

    for tipo, keywords in tipos_documento.items():
        if match_keywords(file_name, keywords) or match_keywords(text, keywords):
            tags["tipo"] = tipo
            break

    # -----------------------------
    # Temas
    # -----------------------------
    temas = {
        "nómina": ["nómina", "nomina", "descuento por nómina", "liquidación de nómina"],
        "salud": ["salud", "eps", "historia clínica", "centro médico", "procedimiento médico"],
        "financiero": ["préstamo", "cuota", "descuento", "interés", "deuda", "pago", "cartera"],
        "legal": ["demandas", "proceso judicial", "abogado", "juez", "código penal", "sentencia"],
        "educación": ["colegio", "universidad", "certificado de estudio", "boletín", "notas"],
        "laboral": ["trabajo", "empleo", "contratación", "vacaciones", "licencia"],
        "inmobiliario": ["arriendo", "inmueble", "propiedad", "contrato de arrendamiento"],
        "tecnología": ["software", "sistema", "plataforma", "aplicación", "soporte técnico"],
        "vehículos": ["vehículo", "soat", "licencia de conducción", "revisión técnico-mecánica", "matrícula vehicular"],
        "tributario": ["renta", "DIAN", "impuesto", "retención", "declaración"],
    }

    for tema, palabras in temas.items():
        if match_keywords(text, palabras):
            tags["tema"] = tema
            break

    # -----------------------------
    # Sectores económicos
    # -----------------------------
    sectores = {
        "tasación": ["tasación", "avaluo", "avalúo", "valor comercial", "peritaje", "inspección vehicular"],
        "automotriz": ["vehículos", "taller", "automotor", "siniestro", "accidente de tránsito"],
        "salud": ["eps", "clínica", "médico", "psicología", "odontología"],
        "educativo": ["universidad", "colegio", "institución educativa", "certificado académico"],
        "financiero": ["entidad financiera", "banco", "pago", "deuda", "cuenta"],
        "legal": ["tribunal", "juez", "proceso", "firma de abogados", "sentencia"],
        "tecnología": ["startup", "aplicación", "plataforma digital", "software"],
        "logística": ["transporte", "entrega", "mensajería", "camión"],
    }

    for sector, keywords in sectores.items():
        if match_keywords(text, keywords):
            if "sectores" not in tags:
                tags["sectores"] = []
            tags["sectores"].append(sector)

    # -----------------------------
    # Firma electrónica o física
    # -----------------------------
    firma_keywords = [
        "firma", "firmado", "firma electrónica", "firmado electrónicamente", "firma digital"
    ]
    if match_keywords(text, firma_keywords):
        tags["requiere_firma"] = True

    # -----------------------------
    # Especialidades médicas (si aplica)
    # -----------------------------
    especialidades = {
        "psicología": ["psicología", "psicoterapia", "consulta psicológica"],
        "odontología": ["odontología", "dentista", "odontograma"],
        "medicina general": ["consulta médica", "médico general", "valoración médica"],
        "oftalmología": ["oftalmología", "visión", "examen visual", "optometría"],
    }

    for esp, keywords in especialidades.items():
        if match_keywords(text, keywords):
            tags["especialidad"] = esp
            break

    return tags
