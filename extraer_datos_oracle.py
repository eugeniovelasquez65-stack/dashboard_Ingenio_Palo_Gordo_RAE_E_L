import oracledb
import pandas as pd
import json
from datetime import datetime
import os
import sys

ORACLE_USER = 'Eulopez'
ORACLE_PASSWORD = 'AUD1T0R142025#'
ORACLE_HOST = 'srvdbpi.ipg.com.gt'
ORACLE_PORT = 1521
ORACLE_SID = 'dbprodpi'
EXCEL_LOCAL = 'data/COMPLEMENTO_DIRECTLY_1.xlsx'

QUERY_DATOS = """
SELECT
    COTD_EC_PRESUPUESTO.NO_CIA,
    COTD_EC_PRESUPUESTO.PERIODO,
    COTD_EC_PRESUPUESTO.CENTRO_COSTO,
    SUBSTR(CENTRO_COSTO, 1, 3) AS ADMINISTRACION,
    CC_N1.DESC_CC1 AS DES_ADMINSTRACION,
    SUBSTR(CENTRO_COSTO, 4, 3) AS GERENCIA,
    CC_N2.DESC_CC2 AS DES_GERENCIA,
    SUBSTR(CENTRO_COSTO, 7, 3) AS RESPONSABLE,
    CC_N3.DESC_CC3 AS DES_RESPONSABLE,
    CC_N3.ESTADO,
    NVL(CC_N3.RESPONSABLE, 'NO_EXISTE') AS COD_RESPONSABLE,
    NVL(UPPER(A.NOM_USUARIO) || ' ' || A.APE_USUARIO, 'NO_EXISTE') AS NAME_RESPONSABLE,
    NVL(CC_N3.GERENTE, 'NO_EXISTE') AS COD_GERENTE,
    NVL(UPPER(G.NOM_USUARIO) || ' ' || G.APE_USUARIO, 'NO_EXISTE') AS NAME_GERENTE,
    COTD_EC_PRESUPUESTO.TIPO_PR AS NUM_CHEQUERA,
    COTC_TIPO_PRESUPUESTO.DESCRIPCION AS NOMBRE_CHEQUERA,
    COTC_TIPO_PRESUPUESTO.GRUPO_P,
    CASE
        WHEN COTC_TIPO_PRESUPUESTO.GRUPO_P = 'D' THEN 'DISPONIBLES'
        WHEN COTC_TIPO_PRESUPUESTO.GRUPO_P = 'G' THEN 'GASTOS'
        WHEN COTC_TIPO_PRESUPUESTO.GRUPO_P = 'I' THEN 'INVERSIONES'
        WHEN COTC_TIPO_PRESUPUESTO.GRUPO_P = 'F' THEN 'FINANCIAMIENTOS'
        WHEN COTC_TIPO_PRESUPUESTO.GRUPO_P = 'V' THEN 'VENTAS'
        ELSE 'GRUPO_NO_IDENTIFICADO'
    END AS NAME_GRUPOS,
    COTD_EC_PRESUPUESTO.TIPO_TR,
    COTC_TRANSACCION.DESCRIPCION,
    COTD_EC_PRESUPUESTO.NO_DOCU,
    COTD_EC_PRESUPUESTO.COR_ID,
    COTD_EC_PRESUPUESTO.FECHA_DOCU,
    COTD_EC_PRESUPUESTO.MES_PR AS NUM_MESES,
    CASE
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 1 THEN 'ENERO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 2 THEN 'FEBRERO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 3 THEN 'MARZO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 4 THEN 'ABRIL'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 5 THEN 'MAYO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 6 THEN 'JUNIO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 7 THEN 'JULIO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 8 THEN 'AGOSTO'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 9 THEN 'SEPTIEMBRE'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 10 THEN 'OCTUBRE'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 11 THEN 'NOVIEMBRE'
        WHEN COTD_EC_PRESUPUESTO.MES_PR = 12 THEN 'DICIEMBRE'
        ELSE 'MES_DESCONOCIDO'
    END AS nombre_mes,
    COTD_EC_PRESUPUESTO.FECHA_TR,
    COTD_EC_PRESUPUESTO.OBSERVACIONES,
    NVL(DECODE(SIGN(COTD_EC_PRESUPUESTO.monto),  1, COTD_EC_PRESUPUESTO.monto, NULL), 0) AS CREDITOS,
    NVL(DECODE(SIGN(COTD_EC_PRESUPUESTO.monto), -1, COTD_EC_PRESUPUESTO.monto * -1, NULL), 0) AS DEBITOS,
    COTD_EC_PRESUPUESTO.USUARIO,
    COTD_EC_PRESUPUESTO.FECHA_OPER,
    CC_N3.PUESTO_GER,
    H.DESCRI AS DESCRI_GER,
    CC_N3.PUESTO_RES,
    I.DESCRI AS DESCRI_RES  
FROM COTD_EC_PRESUPUESTO
JOIN COTC_TIPO_PRESUPUESTO ON COTC_TIPO_PRESUPUESTO.TIPO_P = COTD_EC_PRESUPUESTO.TIPO_PR
JOIN COTC_TRANSACCION ON COTC_TRANSACCION.TIPO_TRANSACCION = COTD_EC_PRESUPUESTO.TIPO_TR
JOIN CC_N1 ON CC_N1.CC1 = SUBSTR(COTD_EC_PRESUPUESTO.CENTRO_COSTO, 1, 3)
JOIN CC_N2 ON CC_N2.CC1 = CC_N1.CC1 AND CC_N2.CC2 = SUBSTR(COTD_EC_PRESUPUESTO.CENTRO_COSTO, 4, 3)
JOIN CC_N3 ON CC_N3.CC1 = CC_N1.CC1 AND CC_N3.CC2 = CC_N2.CC2 AND CC_N3.CC3 = SUBSTR(COTD_EC_PRESUPUESTO.CENTRO_COSTO, 7, 3)
LEFT JOIN SEG47.TASGUSUARIO G ON G.NO_EMPLE = CC_N3.GERENTE
LEFT JOIN SEG47.TASGUSUARIO A ON A.NO_EMPLE = CC_N3.RESPONSABLE
LEFT JOIN ARPLMPEQUI H ON CC_N3.PUESTO_GER=H.PUESTO
LEFT JOIN ARPLMPEQUI I ON CC_N3.PUESTO_RES=I.PUESTO
WHERE CC_N1.ESTADO = 'A'
"""

QUERY_ACCESOS = """
SELECT
    LOWER(A.USUARIO) AS USUARIO,
    B.NOM_USUARIO||' '||B.APE_USUARIO AS NOMBRE_USUARIO,
    A.CCU,
    CASE WHEN A.ADMON='S' THEN 'YES' ELSE 'NO' END AS CUENTA_ACESOS,
    C.PUESTO_RES AS COD_RESPONSABLE,
    D.DESCRI AS DES_RESPONSABLE,
    C.PUESTO_GER AS COD_GERENTE,
    E.DESCRI AS DES_GERENTE
FROM PR_ACCESO_CCU A
JOIN TASGUSUARIO B ON B.USUARIO=A.USUARIO
JOIN CC_N3 C ON C.CENTRO_C=A.CCU
JOIN ARPLMPEQUI D ON D.PUESTO=C.PUESTO_RES
JOIN ARPLMPEQUI E ON E.PUESTO=C.PUESTO_GER
WHERE A.ADMON='S' AND C.ESTADO='A'
"""

def conectar_oracle():
    oracle_path = r"C:\Users\eulopez\Documents\EUGENIO_LÓPEZ\CONTROLES IPG_REA.E.L\CLIENTE_PROPIO\instantclient-basic-windows.x64-21.20.0.0.0dbru\instantclient_21_20"
    
    if oracle_path not in sys.path:
        sys.path.insert(0, oracle_path)
    os.environ["PATH"] = oracle_path + ";" + os.environ.get("PATH", "")
    
    try:
        oracledb.init_oracle_client(lib_dir=oracle_path)
    except:
        pass
    
    try:
        conn_str = f"{ORACLE_USER}/{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SID}"
        connection = oracledb.connect(conn_str)
        print("Conexion exitosa")
        return connection
    except Exception as e:
        print(f"Error metodo 1: {e}")
        try:
            dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
            connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
            print("Conexion exitosa con DSN")
            return connection
        except Exception as e2:
            print(f"Error metodo 2: {e2}")
            return None

def ejecutar_query(query, nombre):
    print(f"\nEjecutando: {nombre}...")
    conn = conectar_oracle()
    if not conn:
        return None
    try:
        df = pd.read_sql(query, conn)
        print(f"OK: {len(df)} registros")
        conn.close()
        return df
    except Exception as e:
        print(f"Error query: {e}")
        conn.close()
        return None

def cargar_excel():
    print(f"\nCargando Excel...")
    try:
        if os.path.exists(EXCEL_LOCAL):
            df = pd.read_excel(EXCEL_LOCAL, engine='openpyxl')
            print(f"OK: {len(df)} usuarios")
            return df
        else:
            print("Excel no encontrado")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def extraer_todos_los_datos():
    print("="*70)
    print("EXTRACCION DE DATOS")
    print("="*70)
    
    if not os.path.exists('data'):
        os.makedirs('data')
    
    df_datos = ejecutar_query(QUERY_DATOS, "Datos Presupuesto")
    if df_datos is None:
        return False
    
    df_accesos = ejecutar_query(QUERY_ACCESOS, "Accesos")
    if df_accesos is None:
        return False
    
    df_usuarios = cargar_excel()
    
    print("\nGuardando JSON...")
    try:
        for col in df_datos.columns:
            if pd.api.types.is_datetime64_any_dtype(df_datos[col]):
                df_datos[col] = df_datos[col].astype(str)
        
        for col in df_accesos.columns:
            if pd.api.types.is_datetime64_any_dtype(df_accesos[col]):
                df_accesos[col] = df_accesos[col].astype(str)
        
        df_datos.to_json('data/datos_presupuesto.json', orient='records', date_format='iso')
        df_accesos.to_json('data/accesos_usuarios.json', orient='records')
        
        if not df_usuarios.empty:
            df_usuarios.to_json('data/usuarios_correos.json', orient='records')
        
        metadata = {
            'fecha': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'datos': len(df_datos),
            'accesos': len(df_accesos),
            'usuarios': len(df_usuarios) if not df_usuarios.empty else 0
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print("\n"+"="*70)
        print("COMPLETADO")
        print("="*70)
        print(f"Datos: {len(df_datos)}")
        print(f"Accesos: {len(df_accesos)}")
        print(f"Usuarios: {len(df_usuarios) if not df_usuarios.empty else 0}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == '__main__':
    extraer_todos_los_datos()