with open('data/datos_presupuesto.txt', encoding='latin-1') as f:
    lines = f.readlines()

print('=== TOTAL LINEAS:', len(lines))
print()
print('=== LINEA 1 (encabezado) ===')
print(lines[0])
print('=== LINEA 2 ===')
print(lines[1])
print('=== LINEA 3 ===')
print(lines[2])
print('=== LINEA 4 ===')
print(lines[3])