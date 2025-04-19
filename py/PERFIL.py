import requests
import pandas as pd
import json
from datetime import datetime

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

def obtener_perfiles(token, tenant_id):
    """Obtiene perfiles desde la API de VALD"""
    
    # URL base y endpoint
    url = "https://prd-aue-api-externalprofile.valdperformance.com/profiles"
    
    # Headers con token
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    # Parámetros
    params = {
        "tenantId": tenant_id
    }
    
    # Realizar la solicitud
    print("🔄 Solicitando perfiles a la API...")
    response = requests.get(url, headers=headers, params=params)
    
    # Verificar respuesta
    if response.status_code == 200:
        print(f"✅ Perfiles obtenidos correctamente")
        return response.json()
    elif response.status_code == 401:
        print("🔒 Token inválido o vencido.")
        return None
    elif response.status_code == 403:
        print("⛔ No tienes permisos suficientes.")
        return None
    elif response.status_code == 204:
        print("⚠️ No hay perfiles disponibles.")
        return []
    else:
        print(f"❌ Error al obtener perfiles: {response.status_code}")
        print(response.text)
        return None

def procesar_perfiles(perfiles):
    """Convierte los datos de perfiles a un DataFrame estructurado"""
    
    print("🔄 Procesando datos de perfiles...")
    
    # Mostramos el tipo de datos para diagnóstico
    print(f"📋 Tipo de datos recibidos: {type(perfiles)}")
    
    # Verificar formato de los datos recibidos con más detalle
    if isinstance(perfiles, list):
        # Si es una lista directamente, usarla
        profiles_list = perfiles
        print(f"✅ Formato lista detectado con {len(profiles_list)} elementos")
    elif isinstance(perfiles, dict):
        # Imprimir claves disponibles para diagnóstico
        print(f"📑 Claves disponibles en el diccionario: {list(perfiles.keys())}")
        
        # Intentar diferentes estructuras conocidas
        if 'items' in perfiles:
            profiles_list = perfiles['items']
        elif 'profiles' in perfiles:
            profiles_list = perfiles['profiles']
        elif 'data' in perfiles:
            profiles_list = perfiles['data']
        else:
            # Si no encontramos una clave conocida, asumimos que el objeto completo es un perfil
            profiles_list = [perfiles]
            
        print(f"✅ Se extrajeron {len(profiles_list)} perfiles del diccionario")
    else:
        print(f"❌ Formato de datos no reconocido: {type(perfiles)}")
        # Intentar convertir a string y mostrar para diagnóstico
        try:
            print(f"Contenido de muestra: {str(perfiles)[:200]}...")
        except:
            pass
        return None
    
    
    
    # Crear DataFrame normalizado
    try:
        df = pd.DataFrame(profiles_list)
        
        # Seleccionar y renombrar columnas relevantes
        columnas_deseadas = {
            'profileId': 'profileId',
            'id': 'ID',
            'externalId': 'DNI',
            'givenName': 'Nombre',
            'familyName': 'Apellido',
            'email': 'Email',
            'sex': 'Sexo',
            'dateOfBirth': 'Fecha_Nacimiento'
        }
        
        # Filtrar solo columnas disponibles
        columnas_disponibles = {k: v for k, v in columnas_deseadas.items() if k in df.columns}
        
        # Crear nuevo DataFrame con columnas seleccionadas
        df_final = df[[*columnas_disponibles.keys()]].copy()
        
        # Renombrar columnas
        df_final = df_final.rename(columns=columnas_disponibles)
        
        # Crear columna de nombre completo
        if 'Nombre' in df_final.columns and 'Apellido' in df_final.columns:
            df_final['Nombre_Completo'] = df_final['Apellido'] + ', ' + df_final['Nombre']
        
        # Formatear fechas
        if 'Fecha_Nacimiento' in df_final.columns:
            df_final['Fecha_Nacimiento'] = pd.to_datetime(df_final['Fecha_Nacimiento']).dt.strftime('%Y-%m-%d')
        
        print(f"✅ DataFrame creado con {df_final.shape[0]} filas y {df_final.shape[1]} columnas")
        return df_final
    
    except Exception as e:
        print(f"❌ Error procesando perfiles: {str(e)}")
        return None

def guardar_csv(df, prefijo="perfiles_vald"):
    """Guarda el DataFrame como CSV con timestamp"""
    
    if df is None or df.empty:
        print("⚠️ No hay datos para guardar")
        return False
    
    # Crear nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefijo}_{timestamp}.csv"
    
    # Guardar archivo
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ Archivo guardado como: {filename}")
        
        # Verificar archivo guardado
        df_verificacion = pd.read_csv(filename)
        print(f"✅ Verificación: {df_verificacion.shape[0]} filas, {df_verificacion.shape[1]} columnas")
        return True
    
    except Exception as e:
        print(f"❌ Error guardando archivo: {str(e)}")
        return False
