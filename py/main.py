import pandas as pd
import os
from datetime import datetime
from datosvald import obtener_token_vald, obtener_datos_vald, procesar_datos, guardar_csv
from PERFIL import obtener_perfiles,procesar_perfiles  # Asumiendo que tienes un módulo para obtener perfiles
from merge import merge_csv_files  # Asumiendo que tienes un módulo para combinar datos

# El resto del código se mantiene igual
# Configuración de VALD
CLIENT_ID = "Nr37673W7ncT4qBQ=="
CLIENT_SECRET = "EMiYKddb7wKrwgxxLqopECDpkNLiS1XOEaw="
TENANT_ID = "f1185650-fb79-44a0-8b4b-b2bf82d28c83"
FECHA_DESDE = "2023-01-01T00:00:00.000Z"

# Función para extraer datos de validación (de datosvald.py)
def extraer_datos_vald():
    print("Extrayendo datos de validación...")
    
    # Obtener token
    token = obtener_token_vald(CLIENT_ID, CLIENT_SECRET)
    
    if token:
        # Obtener datos
        datos = obtener_datos_vald(token, TENANT_ID, FECHA_DESDE)
        
        if datos:
            # Procesar datos
            df = procesar_datos(datos)
            
            # Generar nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ruta_archivo = f"datos_vald_{timestamp}.csv"
            
            # Guardar como CSV
            guardar_csv(df, ruta_archivo)
            
            print(f"✅ Datos de validación extraídos y guardados en {ruta_archivo}")
            return df
        else:
            print("❌ No hay datos para procesar.")
            return None
    else:
        print("❌ No se pudo continuar sin un token válido.")
        return None
    
    
    
    
# Función para extraer perfiles de jugadores (de PERFIL.py)
def extraer_perfiles():
    print("Extrayendo perfiles de jugadores...")
    
    # Obtener token
    token = obtener_token_vald(CLIENT_ID, CLIENT_SECRET)
    
    if token:
        # Obtener los perfiles usando el token y TENANT_ID
        perfiles_json = obtener_perfiles(token, TENANT_ID)
        
        if perfiles_json:
            # Procesar los perfiles
            df_perfiles = procesar_perfiles(perfiles_json)
            
            if df_perfiles is not None:
                # Generar nombre de archivo con timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                ruta_archivo = f"perfiles_vald_{timestamp}.csv"
                df_perfiles.to_csv(ruta_archivo, index=False)
                
                print(f"✅ Perfiles extraídos y guardados en {ruta_archivo}")
                return df_perfiles
        
        print("❌ No se pudieron obtener los perfiles.")
        return None
    else:
        print("❌ No se pudo continuar sin un token válido.")
        return None
    
    

# Función para combinar los datos (de merge.py)
def combinar_datos(df_vald, df_perfiles):
    print("Combinando datos de validación y perfiles...")
    # Aquí coloca el código de merge.py para combinar los dataframes
    df_combinado = merge_csv_files(df_vald, df_perfiles)  # Asumiendo que esta función existe
    
    return df_combinado

# Función principal que ejecuta todo el proceso
def ejecutar_proceso_completo():
    print("Iniciando proceso completo de extracción y combinación de datos...")
    
    # Paso 1: Extraer datos de validación
    df_vald = extraer_datos_vald()
    if df_vald is None:
        print("❌ No se pudieron obtener los datos de validación. Proceso cancelado.")
        return None
        
    print(f"✅ Datos de validación extraídos: {df_vald.shape[0]} filas, {df_vald.shape[1]} columnas")
    
    # Paso 2: Extraer perfiles de jugadores
    df_perfiles = extraer_perfiles()
    if df_perfiles is None:
        print("❌ No se pudieron obtener los perfiles. Proceso cancelado.")
        return None
        
    print(f"✅ Perfiles extraídos: {df_perfiles.shape[0]} filas, {df_perfiles.shape[1]} columnas")
    
    # Paso 3: Combinar datos
    df_combinado = combinar_datos(df_vald, df_perfiles)
    print(f"✅ Datos combinados: {df_combinado.shape[0]} filas, {df_combinado.shape[1]} columnas")
    
    # Paso 4: Guardar resultado
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"datos_completos_{timestamp}.csv"
    df_combinado.to_csv(nombre_archivo, index=False)
    print(f"✅ Datos guardados en {nombre_archivo}")
    
    return df_combinado

# Ejecución principal
if __name__ == "__main__":
    df_final = ejecutar_proceso_completo()
    
    if df_final is not None:
        print("\n✅ Proceso completado exitosamente")
        print("Primeras filas del dataset final:")
        print(df_final.head())
    else:
        print("\n❌ El proceso no pudo completarse correctamente")