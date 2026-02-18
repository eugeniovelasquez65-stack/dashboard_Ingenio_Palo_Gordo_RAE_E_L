import os
import sys

# CONFIGURAR PATH ANTES DE IMPORTAR ORACLEDB
oracle_path = r"C:\Users\eulopez\Documents\EUGENIO_LÓPEZ\CONTROLES IPG_REA.E.L\CLIENTE_PROPIO\instantclient-basic-windows.x64-21.20.0.0.0dbru\instantclient_21_20"

# Agregar al PATH del sistema
os.add_dll_directory(oracle_path)
os.environ["PATH"] = oracle_path + ";" + os.environ.get("PATH", "")

# AHORA sí importar oracledb
import oracledb

print(f"1. Modo antes: thin={oracledb.is_thin_mode()}")

# Intentar inicializar sin config_dir
try:
    oracledb.init_oracle_client(lib_dir=oracle_path)
    print("2. init OK")
except Exception as e:
    print(f"2. Error init: {str(e)[:100]}")

print(f"3. Modo despues: thin={oracledb.is_thin_mode()}")

# Si SIGUE en thin mode, no hay nada que hacer con esta versión de Oracle
if oracledb.is_thin_mode():
    print("\n⚠️ NO SE PUDO ACTIVAR THICK MODE")
    print("Tu Oracle es muy viejo para python-oracledb")
    print("\nOPCIONES:")
    print("1. Usar cx_Oracle (requiere compilación)")
    print("2. Actualizar Oracle en el servidor (imposible)")
    print("3. Usar un servidor intermedio con Oracle más nuevo")
else:
    print("\n✅ THICK MODE ACTIVADO")
    try:
        dsn = oracledb.makedsn("srvdbpi.ipg.com.gt", 1521, sid="dbprodpi")
        conn = oracledb.connect(user="Eulopez", password="AUD1T0R142025#", dsn=dsn)
        print("✅ CONEXION EXITOSA!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT SYSDATE FROM DUAL")
        print(f"✅ Query test: {cursor.fetchone()}")
        
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")