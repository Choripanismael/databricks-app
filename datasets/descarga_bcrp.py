# Descarga de series regionales del BCRP
# Credito directo y depositos del sistema financiero por departamento
# Fuente BCRPData con datos de la SBS, en millones de soles a fin de periodo
# Genera dos archivos que luego se suben a la capa raw del Data Lake

import csv
import json
import time
import urllib.request

URL_API = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{codigos}/json/{inicio}/{fin}"
PERIODO_INICIO = "2015-1"
PERIODO_FIN = "2025-12"
TAMANO_LOTE = 10

DEPARTAMENTOS = [
    "Amazonas", "Ancash", "Apurimac", "Arequipa", "Ayacucho", "Cajamarca",
    "Callao", "Cusco", "Huancavelica", "Huanuco", "Ica", "Junin",
    "La Libertad", "Lambayeque", "Lima", "Loreto", "Madre de Dios",
    "Moquegua", "Pasco", "Piura", "Puno", "San Martin", "Tacna",
    "Tumbes", "Ucayali", "Total",
]
MONEDAS = ["MN", "ME", "TOTAL"]

# El codigo de cada serie sigue un orden fijo por departamento y moneda
# creditos empieza en RD13560DM y depositos en RD13638DM
DATASETS = {
    "creditos_departamento.csv": 13560,
    "depositos_departamento.csv": 13638,
}


def armar_catalogo(codigo_base):
    catalogo = []
    for i, departamento in enumerate(DEPARTAMENTOS):
        for j, moneda in enumerate(MONEDAS):
            codigo = f"RD{codigo_base + 3 * i + j}DM"
            catalogo.append((codigo, departamento, moneda))
    return catalogo


def consultar_api(lote):
    codigos = "-".join(serie[0] for serie in lote)
    url = URL_API.format(codigos=codigos, inicio=PERIODO_INICIO, fin=PERIODO_FIN)
    pedido = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(pedido, timeout=90) as respuesta:
        texto = respuesta.read().decode("utf-8", errors="replace")
    return json.loads(texto)


def descargar(nombre_archivo, codigo_base):
    catalogo = armar_catalogo(codigo_base)
    filas = []
    for k in range(0, len(catalogo), TAMANO_LOTE):
        lote = catalogo[k:k + TAMANO_LOTE]
        datos = consultar_api(lote)
        for periodo in datos["periods"]:
            for (codigo, departamento, moneda), valor in zip(lote, periodo["values"]):
                filas.append([codigo, departamento, moneda, periodo["name"], valor])
        print(f"{nombre_archivo} lote {k // TAMANO_LOTE + 1} listo")
        time.sleep(1)

    with open(nombre_archivo, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["codigo_serie", "departamento", "moneda", "periodo", "valor"])
        escritor.writerows(filas)
    print(f"{nombre_archivo} guardado con {len(filas)} filas")


if __name__ == "__main__":
    for nombre, base in DATASETS.items():
        descargar(nombre, base)
