import os

ruta = 'data/datos_presupuesto_erp.json'
tamano = os.path.getsize(ruta)
print(f'Tamaño del archivo: {tamano} bytes')

# Leer los primeros 500 bytes en modo binario para ver qué hay
with open(ruta, 'rb') as f:
    primeros = f.read(500)

print(f'\nPrimeros bytes (raw):')
print(primeros)

print(f'\nIntentando detectar encoding...')
# Buscar el byte problemático
for enc in ['utf-8', 'latin-1', 'cp1252', 'utf-16', 'utf-8-sig']:
    try:
        with open(ruta, 'r', encoding=enc) as f:
            contenido = f.read(1000)
        print(f'  OK con encoding: {enc}')
        print(f'  Primeros 200 caracteres: {contenido[:200]}')
        break
    except Exception as e:
        print(f'  FALLA con {enc}: {e}')