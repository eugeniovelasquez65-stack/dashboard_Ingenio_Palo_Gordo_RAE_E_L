import pandas as pd
import json
import re
from datetime import datetime
import os
import sys

# ===================================================================
# CONFIGURACIÓN DE RUTAS SEGURAS (EVITA ERRORES EN VS CODE)
# ===================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

JSON_PRESUPUESTO = os.path.join(BASE_DIR, 'data', 'datos_presupuesto_erp.json')
JSON_ACCESOS     = os.path.join(BASE_DIR, 'data', 'accesos_usuarios_erp.json')
EXCEL_USUARIOS   = os.path.join(BASE_DIR, 'data', 'COMPLEMENTO_DIRECTLY_1.xlsx')

print("Directorio actual:", os.getcwd())


# ===================================================================
# FUNCIÓN LIMPIEZA JSON ROBUSTA
# ===================================================================

def limpiar_json_problematico(texto):
    """
    Limpia JSON provenientes de ERP:
    - Elimina TODOS los caracteres de control
    - Elimina saltos de línea reales dentro de strings
    - Reemplaza caracteres invisibles problemáticos
    """
    # Eliminar caracteres de control ASCII
    texto = re.sub(r'[\x00-\x1F]', ' ', texto)

    # Reemplazar saltos de línea reales
    texto = texto.replace('\r', ' ').replace('\n', ' ')

    return texto


# ===================================================================
# LECTURA Y PARSEO JSON
# ===================================================================

def limpiar_y_parsear_json(ruta, nombre):

    if not os.path.exists(ruta):
        print(f"\nERROR: No se encontró '{ruta}'")
        return pd.DataFrame()

    print(f"\nLeyendo {nombre} ({round(os.path.getsize(ruta)/1024/1024, 1)} MB)...")

    # Leer bytes
    with open(ruta, 'rb') as f:
        raw = f.read()

    # Detectar encoding
    encoding = 'utf-8'
    try:
        raw.decode('utf-8')
        encoding = 'utf-8'
    except UnicodeDecodeError:
        encoding = 'latin-1'

    print(f"  Encoding detectado : {encoding}")

    try:
        texto = raw.decode(encoding)
    except Exception as e:
        print(f"  ERROR decodificando: {e}")
        return pd.DataFrame()

    # Limpiar JSON problemático
    texto_limpio = limpiar_json_problematico(texto)

    # Intentar parsear
    try:
        contenido = json.loads(texto_limpio, strict=False)
        print("  JSON parseado OK")
    except json.JSONDecodeError as e:
        print(f"  ERROR parseando JSON: {e}")
        return pd.DataFrame()

    # Extraer registros
    try:
        if isinstance(contenido, dict) and 'recordset' in contenido:
            registros = contenido['recordset']
        elif isinstance(contenido, list):
            registros = contenido
        else:
            print("  ERROR: Estructura JSON desconocida.")
            return pd.DataFrame()

        df = pd.DataFrame(registros)
        df.columns = df.columns.str.strip().str.upper()

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()

        print(f"  Registros cargados : {len(df)}")
        print(f"  Columnas           : {df.columns.tolist()}")

        return df

    except Exception as e:
        print(f"  ERROR construyendo DataFrame: {e}")
        return pd.DataFrame()


# ===================================================================
# LECTURA EXCEL USUARIOS
# ===================================================================

def leer_excel_usuarios(ruta):

    print("\nLeyendo Excel de usuarios...")

    if not os.path.exists(ruta):
        print(f"  ERROR: No se encontró '{ruta}'")
        return pd.DataFrame()

    try:
        df_test = pd.read_excel(ruta, engine='openpyxl', header=0)

        primera_fila = df_test.iloc[0].astype(str).tolist() if len(df_test) > 0 else []

        if any('@' in v or 'USUARIO' in v.upper() for v in primera_fila):
            df = pd.read_excel(ruta, engine='openpyxl', header=1)
            print("  (Encabezados en fila 2)")
        else:
            df = df_test
            print("  (Encabezados en fila 1)")

        df.columns = df.columns.str.strip()

        print("  Columnas detectadas:", df.columns.tolist())

        if len(df.columns) >= 4:
            cols = list(df.columns)
            cols[0] = 'NAME_USUARIO'
            cols[1] = 'USUARIO_NOMBRE'
            cols[2] = 'USUARIO'
            cols[3] = 'PERMISO_ACTUAL'
            df.columns = cols

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()

        df = df[
            df['USUARIO'].notna() &
            (df['USUARIO'] != '') &
            (df['USUARIO'].str.lower() != 'nan')
        ]

        print(f"  Usuarios cargados : {len(df)}")

        return df

    except Exception as e:
        print(f"  ERROR leyendo Excel: {e}")
        return pd.DataFrame()


# ===================================================================
# VERIFICACIÓN RLS
# ===================================================================

def verificar_rls(df_datos, df_accesos, df_usuarios):

    print("\n" + "=" * 70)
    print("VERIFICANDO CADENA RLS")
    print("=" * 70)

    if df_usuarios.empty or df_accesos.empty or df_datos.empty:
        print("  Faltan archivos.")
        return

    correo_prueba   = df_usuarios.iloc[0]['USUARIO'].strip().lower()
    usr_sistema     = df_usuarios.iloc[0]['USUARIO_NOMBRE'].strip().lower()
    nombre_completo = df_usuarios.iloc[0]['NAME_USUARIO']

    print(f"\n  Usuario          : {nombre_completo}")
    print(f"  Correo           : {correo_prueba}")
    print(f"  Usuario sistema  : {usr_sistema}")

    df_accesos['USUARIO'] = df_accesos['USUARIO'].astype(str).str.strip().str.lower()
    accesos_usr = df_accesos[df_accesos['USUARIO'] == usr_sistema]

    print(f"\n  Accesos encontrados: {len(accesos_usr)}")

    if accesos_usr.empty:
        print("  Usuario no encontrado en accesos.")
        return

    ccus = accesos_usr['CCU'].astype(str).str.strip().tolist()

    df_datos['CENTRO_COSTO'] = df_datos['CENTRO_COSTO'].astype(str).str.strip()
    df_filtrado = df_datos[df_datos['CENTRO_COSTO'].isin(ccus)]

    print(f"\n  Registros filtrados: {len(df_filtrado)}")

    if len(df_filtrado) > 0:
        cr = pd.to_numeric(df_filtrado.get('CREDITOS', 0), errors='coerce').sum()
        db = pd.to_numeric(df_filtrado.get('DEBITOS', 0), errors='coerce').sum()
        print(f"  Créditos: ${cr:,.2f}")
        print(f"  Débitos : ${db:,.2f}")
    else:
        print("  No hubo coincidencias CCU.")


# ===================================================================
# PROCESAMIENTO PRINCIPAL
# ===================================================================

def procesar_datos():

    print("=" * 70)
    print("PROCESANDO DATOS DESDE JSON DEL ERP")
    print("=" * 70)

    df_datos    = limpiar_y_parsear_json(JSON_PRESUPUESTO, 'Presupuesto (datos)')
    df_accesos  = limpiar_y_parsear_json(JSON_ACCESOS, 'Accesos por usuario')
    df_usuarios = leer_excel_usuarios(EXCEL_USUARIOS)

    if df_datos.empty or df_accesos.empty or df_usuarios.empty:
        print("\nNo se puede continuar por archivos faltantes.")
        return False

    verificar_rls(df_datos, df_accesos, df_usuarios)

    print("\nGuardando archivos procesados...")

    try:
        os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)

        df_datos.to_json(os.path.join(BASE_DIR, 'data', 'datos_presupuesto.json'),
                         orient='records', force_ascii=False)

        df_accesos.to_json(os.path.join(BASE_DIR, 'data', 'accesos_usuarios.json'),
                           orient='records', force_ascii=False)

        df_usuarios.to_json(os.path.join(BASE_DIR, 'data', 'usuarios_correos.json'),
                            orient='records', force_ascii=False)

        metadata = {
            'fecha': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'datos': len(df_datos),
            'accesos': len(df_accesos),
            'usuarios': len(df_usuarios)
        }

        with open(os.path.join(BASE_DIR, 'data', 'metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print("\nPROCESAMIENTO COMPLETADO EXITOSAMENTE")
        print(metadata)

        return True

    except Exception as e:
        print(f"ERROR guardando archivos: {e}")
        return False


if __name__ == '__main__':
    procesar_datos()
