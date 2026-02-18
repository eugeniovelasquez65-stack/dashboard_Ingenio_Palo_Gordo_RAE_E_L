import os
import sys

# Configurar Oracle Client ANTES de importar oracledb
oracle_path = r"C:\Users\eulopez\Documents\EUGENIO_LÓPEZ\CONTROLES IPG_REA.E.L\CLIENTE_PROPIO\instantclient-basic-windows.x64-19.29.0.0.0dbru\instantclient_19_29"

# Agregar al PATH
os.environ["PATH"] = oracle_path + ";" + os.environ.get("PATH", "")

# Importar oracledb DESPUÉS de configurar PATH
import oracledb

# Inicializar en modo thick
try:
    oracledb.init_oracle_client(lib_dir=oracle_path)
    print("Oracle Client inicializado en modo thick")
except Exception as e:
    print(f"Advertencia init: {e}")

# Verificar modo
print(f"Modo thin: {oracledb.is_thin_mode()}")