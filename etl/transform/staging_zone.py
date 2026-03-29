import pandas as pd

def read_data(url):
    data = pd.read_csv(url)
    return data

def delete_columns(data):
    data = data.drop(columns=['ultima_actualizacion', 'fecha_inicio_liquidacion', 'fecha_fin_liquidacion', 'fecha_de_notificaci_n_de_prorrogaci_n', 'localizaci_n'])
    data = data.dropna()
    return data

def rename_columns(data):
    data = data.rename(columns={
                            'liquidaci_n' : 'liquidacion',
                            'obligaci_n_ambiental' : 'oblicacion_ambiental', 
                            'tipo_de_identificaci_n_representante_legal' : 'tipo_identificacion_representante_legal',
                            'identificaci_n_representante_legal' : 'identificacion_representante_legal',
                            'g_nero_representante_legal' : 'genero_representante_legal',
                            'sistema_general_de_regal_as' : 'sistema_general_de_regalias',
                            'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_' : 'recursos_propios_alcaldias_gobernaciones_y_resguardos_indigenas', 
                            'duraci_n_del_contrato' : 'duracion_del_contrato',
                            'n_mero_de_cuenta' : 'numero_de_cuenta',
                            'n_mero_de_documento_ordenador_del_gasto' : 'numero_de_documento_ordenador_del_gasto',
                            'n_mero_de_documento_supervisor' : 'numero_de_documento_supervisor',
                            'n_mero_de_documento_ordenador_de_pago' : 'numero_de_documento_ordenador_de_pago',})
    return data

def date_transformations(data):
    data["fecha_de_firma"] = pd.to_datetime(data["fecha_de_firma"], errors='coerce')
    data["fecha_de_inicio_del_contrato"] = pd.to_datetime(data["fecha_de_inicio_del_contrato"], errors='coerce')
    data["fecha_de_fin_del_contrato"] = pd.to_datetime(data["fecha_de_fin_del_contrato"], errors='coerce')
    return data
  
def numeric_transformations(data):
    numeric_columns = ["valor_del_contrato", "valor_de_pago_adelantado", "valor_facturado", "valor_pendiente_de_pago", "valor_pagado", "valor_amortizado", "valor_pendiente_de", "valor_pendiente_de_ejecucion", "saldo_cdp", "saldo_vigencia", "dias_adicionados", "sistema_general_de_participaciones", "sistema_general_de_regalias", "recursos_propios_alcaldias_gobernaciones_y_resguardos_indigenas", "recursos_de_credito", "recursos_propios"]
    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce')
    return data

def boolean_transformations(data):
    boolean_columns = ["es_grupo", "es_pyme", "liquidacion", "oblicacion_ambiental", "obligaciones_postconsumo", "reversion", "espostconflicto", "el_contrato_puede_ser_prorrogado"]
    for column in boolean_columns:
        data[column] = data[column].astype(str).str.strip().str.lower().map({'si': True, 'no': False})
    return data

def uppercase_transformations(data):
    data["nombre_entidad"] = data["nombre_entidad"].str.upper()
    data["nombre_entidad"] = data["nombre_entidad"].str.replace("*", "", regex=False).str.replace("**", "", regex=False).str.replace("****", "", regex=False)
    data["descripcion_del_proceso"] = data["descripcion_del_proceso"].str.upper()
    data["proveedor_adjudicado"] = data["proveedor_adjudicado"].str.upper()
    data["nombre_representante_legal"] = data["nombre_representante_legal"].str.upper()
    data["objeto_del_contrato"] = data["objeto_del_contrato"].str.upper()
    data["nombre_ordenador_del_gasto"] = data["nombre_ordenador_del_gasto"].str.upper()
    data["nombre_supervisor"] = data["nombre_supervisor"].str.upper()
    data["nombre_ordenador_de_pago"] = data["nombre_ordenador_de_pago"].str.upper()
    # data["justificacion_modalidad_de"] = data["justificacion_modalidad_de"].str.upper()
    return data

def url_transformations(data):
    data["urlproceso"] = data["urlproceso"].str.extract(r"url':\s*'([^']+)'")
    return data

def duration_transformations(data):       
    data[["duracion_valor", "duracion_unidad"]] = (data["duracion_del_contrato"].str.extract(r"(\d+)\s*(Mes\(es\)|Dia\(s\))"))
    data = data.drop(columns=['duracion_del_contrato'])
    return data

def main():
    print("Cargando datos...")
    data = read_data("/Users/loperatomas410/Documents/WorkSpace/Proyectos/contrataciones_publicas/data/raw/contrataciones_publicas.csv")
    
    print("Datos cargados exitosamente. Iniciando limpieza de datos...")
    data = delete_columns(data)
    
    print("Datos cargados exitosamente. Iniciando transformaciones...")
    data = rename_columns(data)
    
    print("Columnas renombradas exitosamente. Aplicando transformaciones...")
    data = date_transformations(data)
    
    print("Transformaciones de fecha aplicadas exitosamente. Aplicando transformaciones numéricas...")
    data = numeric_transformations(data)
    
    print("Transformaciones numéricas aplicadas exitosamente. Aplicando transformaciones booleanas...")
    data = boolean_transformations(data)
    
    print("Transformaciones booleanas aplicadas exitosamente. Aplicando transformaciones de mayúsculas...")
    data = uppercase_transformations(data)
    
    print("Transformaciones de mayúsculas aplicadas exitosamente. Aplicando transformaciones de URL...")
    data = url_transformations(data)
    
    print("Transformaciones de URL aplicadas exitosamente. Aplicando transformaciones de duración...")
    data = duration_transformations(data)
    
    print("Transformaciones de duración aplicadas exitosamente. Guardando datos transformados...")
    data.to_csv("/Users/loperatomas410/Documents/WorkSpace/Proyectos/contrataciones_publicas/data/staging/contrataciones_publicas_staging.csv", index=False)
    
    print("Datos transformados y guardados")
    
if __name__ == "__main__":
    main()