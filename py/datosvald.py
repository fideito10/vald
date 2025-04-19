import requests
import pandas as pd
import json
import base64

def obtener_token_vald(client_id, client_secret):
    """Obtiene un token de autenticación de la API de VALD"""
    
    # URL para solicitar el token
    token_url = "https://security.valdperformance.com/connect/token"
    
    # Datos para la solicitud del token
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    try:
        # Realizar la solicitud
        response = requests.post(token_url, data=payload)
        
        # Verificar respuesta
        if response.status_code == 200:
            token_data = response.json()
            print("✅ Autenticación exitosa")
            return token_data.get('access_token')
        else:
            print(f"❌ Error en la autenticación: {response.status_code}")
            print(response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return None

def obtener_datos_vald(token, tenant_id, fecha_desde):
    """Obtiene los datos de pruebas desde la API de VALD"""
    
    # URL base según región
    base_url = "https://prd-aue-api-externalnordbord.valdperformance.com"
    
    # Endpoint y parámetros
    endpoint = "/tests/v2"
    params = {
        "tenantId": tenant_id,
        "modifiedFromUtc": fecha_desde
    }
    
    # Headers con token
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Realizar la solicitud
    print("🔄 Solicitando datos a la API...")
    response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
    
    # Verificar respuesta
    if response.status_code == 200:
        print(f"✅ Datos obtenidos correctamente")
        return response.json()
    elif response.status_code == 204:
        print("⚠️ No hay más registros para obtener.")
        return []
    else:
        print(f"❌ Error al obtener datos: {response.status_code}")
        print(response.text)
        return None

def procesar_datos(datos):
    """Convierte los datos JSON a un DataFrame de pandas y normaliza columnas anidadas"""
    
    # Verificar tipo de datos y normalizarlos
    if isinstance(datos, dict) and 'items' in datos:
        df = pd.DataFrame(datos['items'])
    elif isinstance(datos, list):
        df = pd.DataFrame(datos)
    else:
        df = pd.DataFrame([datos])
    
    # Si existe la columna 'tests' y contiene datos, normalizarla
    if 'tests' in df.columns and len(df) > 0 and isinstance(df['tests'].iloc[0], (list, dict)):
        try:
            # Normalizar la columna tests
            expanded_df = pd.json_normalize(df['tests'].iloc[0])
            
            # Convertir columnas de fecha a datetime
            date_columns = [col for col in expanded_df.columns 
                            if 'date' in col.lower() or 'time' in col.lower()]
            for col in date_columns:
                expanded_df[col] = pd.to_datetime(expanded_df[col], errors='ignore')
            
            # Reemplazar el DataFrame original con el normalizado
            df = expanded_df
            print("✅ Normalización aplicada a la columna 'tests'")
        except Exception as e:
            print(f"⚠️ No se pudo normalizar la columna 'tests': {e}")
    
    print(f"✅ DataFrame creado con {df.shape[0]} filas y {df.shape[1]} columnas")
    return df

def guardar_csv(df, ruta_archivo):
    """Guardar el DataFrame como CSV"""
    
    df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
    print(f"✅ Archivo guardado como: {ruta_archivo}")
    
    # Verificar el archivo guardado
    df_verificacion = pd.read_csv(ruta_archivo)
    print(f"✅ Verificación: {df_verificacion.shape[0]} filas, {df_verificacion.shape[1]} columnas")

