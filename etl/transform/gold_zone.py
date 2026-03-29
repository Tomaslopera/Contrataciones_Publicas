import pandas as pd
import uuid

def geografia(data):
    dim_geografia = data[['departamento', 'ciudad']].drop_duplicates().reset_index(drop=True)
    dim_geografia["id_geografia"] = dim_geografia.apply(lambda _: str(uuid.uuid4()), axis=1)
    dim_geografia = dim_geografia[["id_geografia", "departamento", "ciudad"]]
    return dim_geografia

def fechas(data):
    dim_fecha_firma = data[['fecha_de_firma']].drop_duplicates().sort_values(by="fecha_de_firma").reset_index(drop=True)
    dim_fecha_firma["año"] = pd.to_datetime(dim_fecha_firma['fecha_de_firma'], errors='coerce').dt.year
    dim_fecha_firma["mes"] = pd.to_datetime(dim_fecha_firma['fecha_de_firma'], errors='coerce').dt.month
    dim_fecha_firma["día"] = pd.to_datetime(dim_fecha_firma['fecha_de_firma'], errors='coerce').dt.day
    dim_fecha_firma = dim_fecha_firma.rename(columns={'fecha_de_firma': 'fecha'})

    dim_fecha_inicio = data[['fecha_de_inicio_del_contrato']].drop_duplicates().sort_values(by="fecha_de_inicio_del_contrato").reset_index(drop=True)
    dim_fecha_inicio["año"] = pd.to_datetime(dim_fecha_inicio['fecha_de_inicio_del_contrato'], errors='coerce').dt.year
    dim_fecha_inicio["mes"] = pd.to_datetime(dim_fecha_inicio['fecha_de_inicio_del_contrato'], errors='coerce').dt.month
    dim_fecha_inicio["día"] = pd.to_datetime(dim_fecha_inicio['fecha_de_inicio_del_contrato'], errors='coerce').dt.day
    dim_fecha_inicio = dim_fecha_inicio.rename(columns={'fecha_de_inicio_del_contrato': 'fecha'})

    dim_fecha_fin = data[['fecha_de_fin_del_contrato']].drop_duplicates().sort_values(by="fecha_de_fin_del_contrato").reset_index(drop=True)
    dim_fecha_fin["año"] = pd.to_datetime(dim_fecha_fin['fecha_de_fin_del_contrato'], errors='coerce').dt.year
    dim_fecha_fin["mes"] = pd.to_datetime(dim_fecha_fin['fecha_de_fin_del_contrato'], errors='coerce').dt.month
    dim_fecha_fin["día"] = pd.to_datetime(dim_fecha_fin['fecha_de_fin_del_contrato'], errors='coerce').dt.day
    dim_fecha_fin = dim_fecha_fin.rename(columns={'fecha_de_fin_del_contrato': 'fecha'})
    
    return dim_fecha_firma, dim_fecha_inicio, dim_fecha_fin

def detalles(data):
    dim_detalles = data[['id_contrato', 'referencia_del_contrato', 'estado_contrato', 'codigo_de_categoria_principal', 'descripcion_del_proceso', 'tipo_de_contrato', 'modalidad_de_contratacion', 'justificacion_modalidad_de']].drop_duplicates().reset_index(drop=True)
    dim_detalles["id_detalles"] = dim_detalles.apply(lambda _: str(uuid.uuid4()), axis=1)
    dim_detalles = dim_detalles[["id_detalles", "id_contrato", "referencia_del_contrato", "estado_contrato", "codigo_de_categoria_principal", "descripcion_del_proceso", "tipo_de_contrato", "modalidad_de_contratacion", "justificacion_modalidad_de"]]
    return dim_detalles

def cuenta(data):
    dim_cuenta = data[['id_contrato', 'nombre_del_banco', 'tipo_de_cuenta', 'numero_de_cuenta']]
    dim_cuenta["id_cuenta"] = dim_cuenta.apply(lambda _: str(uuid.uuid4()), axis=1)
    dim_cuenta = dim_cuenta[["id_cuenta", "id_contrato", "nombre_del_banco", "tipo_de_cuenta", "numero_de_cuenta"]]
    return dim_cuenta

def representante(data):
    dim_representante = data[['id_contrato', 'identificacion_representante_legal', 'nombre_representante_legal', 'nacionalidad_representante_legal', 'domicilio_representante_legal', 'tipo_identificacion_representante_legal','genero_representante_legal']].drop_duplicates().reset_index(drop=True)
    dim_representante["id_representante"] = dim_representante.apply(lambda _: str(uuid.uuid4()), axis=1)
    dim_representante = dim_representante[["id_representante", "id_contrato", "identificacion_representante_legal", "nombre_representante_legal", "nacionalidad_representante_legal", "domicilio_representante_legal", "tipo_identificacion_representante_legal", "genero_representante_legal"]]
    return dim_representante

def caracteristicas_generales(data):
    dim_caracteristicas_generales = data[['es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental', 'espostconflicto', 'origen_de_los_recursos', 'destino_gasto']].drop_duplicates().reset_index(drop=True)
    dim_caracteristicas_generales["id_caracteristicas_generales"] = dim_caracteristicas_generales.index + 1
    dim_caracteristicas_generales = dim_caracteristicas_generales[["id_caracteristicas_generales", "es_grupo", "es_pyme", "liquidacion", "oblicacion_ambiental", "espostconflicto", "origen_de_los_recursos", "destino_gasto"]]
    return dim_caracteristicas_generales

def proveedor(data):
    dim_proveedor = data[['id_contrato', 'codigo_proveedor', 'tipodocproveedor', 'documento_proveedor', 'proveedor_adjudicado']].drop_duplicates().reset_index(drop=True)
    return dim_proveedor

def contrato(data, dim_geografia, dim_cuenta, dim_detalles, dim_representante, dim_caracteristicas_generales):
    fact_contrato = data[['id_contrato', 'codigo_proveedor', 'fecha_de_firma', 'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato', 'valor_del_contrato', 'valor_de_pago_adelantado', 'valor_facturado', 'valor_pendiente_de_pago', 'valor_pagado', 'valor_amortizado', 'valor_pendiente_de', 'valor_pendiente_de_ejecucion', 'saldo_cdp', 'saldo_vigencia', 'departamento', 'ciudad', 'es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental', 'espostconflicto', 'origen_de_los_recursos', 'destino_gasto']]

    lookup_geografia = dim_geografia.set_index(['departamento', 'ciudad'])['id_geografia'].to_dict()
    fact_contrato['id_geografia'] = fact_contrato.apply(lambda row: lookup_geografia.get((row['departamento'], row['ciudad'])), axis=1)

    fact_contrato = fact_contrato.drop(columns=['departamento', 'ciudad'])

    lookup_cuenta = dim_cuenta.set_index('id_contrato')['id_cuenta'].to_dict()
    fact_contrato['id_cuenta'] = fact_contrato['id_contrato'].map(lookup_cuenta)

    lookup_detalles = dim_detalles.set_index('id_contrato')['id_detalles'].to_dict()
    fact_contrato['id_detalles'] = fact_contrato['id_contrato'].map(lookup_detalles)

    lookup_representante = dim_representante.set_index('id_contrato')['id_representante'].to_dict()
    fact_contrato['id_representante'] = fact_contrato['id_contrato'].map(lookup_representante)

    lookup_caracteristicas_generales = dim_caracteristicas_generales.set_index(['es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental', 'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'])['id_caracteristicas_generales'].to_dict()
    fact_contrato['id_caracteristicas_generales'] = fact_contrato.apply(lambda row: lookup_caracteristicas_generales.get((row['es_grupo'], row['es_pyme'], row['liquidacion'], row['oblicacion_ambiental'], row['espostconflicto'], row['origen_de_los_recursos'], row['destino_gasto'])), axis=1)
    fact_contrato = fact_contrato.drop(columns=['es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental', 'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'])

    fact_contrato = fact_contrato[["id_contrato", "id_geografia", "id_cuenta", "id_detalles", "id_representante", "id_caracteristicas_generales", "codigo_proveedor", "fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato", "valor_del_contrato", "valor_de_pago_adelantado", "valor_facturado", "valor_pendiente_de_pago", "valor_pagado", "valor_amortizado", "valor_pendiente_de", "valor_pendiente_de_ejecucion", "saldo_cdp", "saldo_vigencia"]]

    fact_contrato = fact_contrato.rename(columns={
        'codigo_proveedor': 'id_proveedor',
        'fecha_de_firma': 'id_fecha_firma',
        'fecha_de_inicio_del_contrato': 'id_fecha_inicio',
        'fecha_de_fin_del_contrato': 'id_fecha_fin',
        'valor_del_contrato': 'valor_contrato',
        'valor_de_pago_adelantado': 'valor_pago_adelantado',
    })

    return fact_contrato

def correcion_dim(dim_cuenta, dim_detalles, dim_representante, dim_proveedor):
    dim_cuenta = dim_cuenta.drop(columns=['id_contrato'])
    dim_cuenta = dim_cuenta.rename(columns={'nombre_del_banco': 'banco', 'tipo_de_cuenta': 'tipo_cuenta', 'numero_de_cuenta': 'numero_cuenta'})

    dim_detalles = dim_detalles.drop(columns=['id_contrato'])
    dim_detalles = dim_detalles.rename(columns={'referencia_del_contrato': 'referencia', 'estado_contrato': 'estado', 'codigo_de_categoria_principal': 'codigo_categoria_principal', 'descripcion_del_proceso': 'descripcion_proceso', 'tipo_de_contrato': 'tipo_contrato', 'modalidad_de_contratacion': 'modalidad_contratacion', 'justificacion_modalidad_de': 'justificacion_modalidad'})

    dim_representante = dim_representante.drop(columns=['id_contrato'])
    dim_representante = dim_representante.rename(columns={'identificacion_representante_legal': 'identificacion', 'nombre_representante_legal': 'nombre', 'nacionalidad_representante_legal': 'nacionalidad', 'domicilio_representante_legal': 'domicilio', 'tipo_identificacion_representante_legal': 'tipo_identificacion', 'genero_representante_legal': 'genero'})

    dim_proveedor = dim_proveedor.drop(columns=['id_contrato'])
    dim_proveedor = dim_proveedor.rename(columns={'codigo_proveedor': 'id_proveedor', 'tipodocproveedor': 'tipo_documento', 'documento_proveedor': 'documento', 'proveedor_adjudicado': 'nombre'})

    return dim_cuenta, dim_detalles, dim_representante, dim_proveedor

def to_csv(fact_contrato, dim_geografia, dim_fecha_firma, dim_fecha_inicio, dim_fecha_fin, dim_detalles, dim_cuenta, dim_representante, dim_caracteristicas_generales, dim_proveedor):
    BASE = '/Users/loperatomas410/Documents/WorkSpace/Proyectos/contrataciones_publicas/'
    fact_contrato.to_csv(BASE + 'data/processed/fact_contrato.csv', index=False)
    dim_geografia.to_csv(BASE + 'data/processed/dim_geografia.csv', index=False)
    dim_fecha_firma.to_csv(BASE + 'data/processed/dim_fecha_firma.csv', index=False)
    dim_fecha_inicio.to_csv(BASE + 'data/processed/dim_fecha_inicio.csv', index=False)
    dim_fecha_fin.to_csv(BASE + 'data/processed/dim_fecha_fin.csv', index=False)
    dim_detalles.to_csv(BASE + 'data/processed/dim_detalles.csv', index=False)
    dim_cuenta.to_csv(BASE + 'data/processed/dim_cuenta.csv', index=False)
    dim_representante.to_csv(BASE + 'data/processed/dim_representante.csv', index=False)
    dim_caracteristicas_generales.to_csv(BASE + 'data/processed/dim_caracteristicas_generales.csv', index=False)
    dim_proveedor.to_csv(BASE + 'data/processed/dim_proveedor.csv', index=False)

def main():
    data = pd.read_csv('/Users/loperatomas410/Documents/WorkSpace/Proyectos/contrataciones_publicas/data/staging/contrataciones_publicas_staging.csv')
    
    print("Iniciando proceso de transformación de datos...")
    print("Creando dimension de geografía...")
    dim_geografia = geografia(data)   
    print("Dimensión de geografía creada exitosamente.")
    
    print("Creando dimension de fechas...")
    dim_fecha_firma, dim_fecha_inicio, dim_fecha_fin = fechas(data)
    print("Dimensión de fechas creada exitosamente.")

    print("Creando dimension de detalles...")
    dim_detalles = detalles(data)
    print("Dimensión de detalles creada exitosamente.")
    
    print("Creando dimension de cuenta...")
    dim_cuenta = cuenta(data)
    print("Dimensión de cuenta creada exitosamente.")
    
    print("Creando dimension de representante...")
    dim_representante = representante(data)
    print("Dimensión de representante creada exitosamente.")
    
    print("Creando dimension de características generales...")
    dim_caracteristicas_generales = caracteristicas_generales(data)
    print("Dimensión de características generales creada exitosamente.")

    print("Creando dimension de proveedor...")
    dim_proveedor = proveedor(data)
    print("Dimensión de proveedor creada exitosamente.")
    
    print("Creando tabla de hechos de contrato...")
    fact_contrato = contrato(data, dim_geografia, dim_cuenta, dim_detalles, dim_representante, dim_caracteristicas_generales)
    print("Tabla de hechos de contrato creada exitosamente.")

    print("Realizando correcciones en las dimensiones...")
    dim_cuenta, dim_detalles, dim_representante, dim_proveedor = correcion_dim(dim_cuenta, dim_detalles, dim_representante, dim_proveedor)
    print("Correcciones realizadas exitosamente.")

    print("Guardando datos transformados en archivos CSV...")
    to_csv(fact_contrato, dim_geografia, dim_fecha_firma, dim_fecha_inicio, dim_fecha_fin, dim_detalles, dim_cuenta, dim_representante, dim_caracteristicas_generales, dim_proveedor)
    
    print("Datos transformados y guardados exitosamente.")
    
if __name__ == "__main__":
    main()