import pandas as pd
from sodapy import Socrata

print("Descargando datos de contrataciones públicas desde el portal de datos abiertos de Colombia...")
client = Socrata("www.datos.gov.co", None)

results = client.get("jbjy-vk9h", limit=2000)

print("Datos descargados exitosamente. Guardando en CSV...")
data = pd.DataFrame.from_records(results)

data.to_csv("", index=False)

print("Datos guardados")