def merge_csv_files(file1_path, file2_path, output_path):
    """
    Realiza un merge entre dos archivos CSV por la columna 'profileId' y ordena las columnas
    
    Args:
        file1_path: Ruta al primer archivo CSV
        file2_path: Ruta al segundo archivo CSV
        output_path: Ruta donde guardar el archivo combinado
    
    Returns:
        DataFrame con el resultado del merge
    """
    import pandas as pd
    
    # Cargar los archivos CSV
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    
    # Realizar el merge de los dataframes por la columna 'profileId'
    df_merged = pd.merge(df1, df2, on='profileId', how='inner')
    
    # Orden específico de columnas (según solicitado)
    column_order = ['Fecha_Nacimiento', 'Nombre_Completo', 'DNI', 'testTypeName', 'profileId']
    
    # Añadir el resto de las columnas que no están en el orden especificado
    remaining_columns = [col for col in df_merged.columns if col not in column_order]
    column_order.extend(remaining_columns)
    
    # Reorganizar las columnas (solo incluye las que realmente existen en el DataFrame)
    existing_columns = [col for col in column_order if col in df_merged.columns]
    df_merged = df_merged[existing_columns]
    
    # Guardar el resultado en un nuevo archivo CSV
    df_merged.to_csv(output_path, index=False)
    
    return df_merged