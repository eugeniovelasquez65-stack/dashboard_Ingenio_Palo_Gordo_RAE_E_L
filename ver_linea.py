ruta = 'data/datos_presupuesto_erp.json'

with open(ruta, 'rb') as f:
    contenido = f.read()

# Encontrar el byte problemático en posición 231278
pos = 231278
print('=== CONTEXTO ALREDEDOR DEL ERROR (posicion 231278) ===')
fragmento = contenido[pos-200 : pos+200]
print('Raw bytes:')
print(fragmento)
print()
print('Intentando decodificar con latin-1:')
try:
    print(fragmento.decode('latin-1'))
except Exception as e:
    print(f'Error: {e}')

# Ver el byte exacto problemático
print(f'\nByte en posicion {pos}: {contenido[pos]:02x} (hex) = {contenido[pos]} (decimal)')
print(f'Bytes alrededor: {contenido[pos-5:pos+5].hex()}')