from flask import Flask, render_template, request, redirect, url_for, session, send_file, jsonify
import pandas as pd
import json
from datetime import datetime
import io
import os

app = Flask(__name__)
app.secret_key = 'clave_secreta_ipg_2026'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MESES_ORDEN = ['NOVIEMBRE','DICIEMBRE','ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
               'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE']


def cargar_datos():
    try:
        with open(os.path.join(BASE_DIR, 'data', 'datos_presupuesto.json'), 'r', encoding='utf-8') as f:
            datos = json.load(f)
        with open(os.path.join(BASE_DIR, 'data', 'accesos_usuarios.json'), 'r', encoding='utf-8') as f:
            accesos = json.load(f)
        with open(os.path.join(BASE_DIR, 'data', 'usuarios_correos.json'), 'r', encoding='utf-8') as f:
            usuarios = json.load(f)
        df_datos    = pd.DataFrame(datos)
        df_accesos  = pd.DataFrame(accesos)
        df_usuarios = pd.DataFrame(usuarios)
        df_datos.columns    = df_datos.columns.str.strip().str.upper()
        df_accesos.columns  = df_accesos.columns.str.strip().str.upper()
        df_usuarios.columns = df_usuarios.columns.str.strip().str.upper()
        return df_datos, df_accesos, df_usuarios
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def cargar_usuarios_login():
    try:
        ruta = os.path.join(BASE_DIR, 'data', 'COMPLEMENTO_DIRECTLY_1.xlsx')
        df = pd.read_excel(ruta, header=0, engine='openpyxl')
        df.columns = [str(c).strip().upper().replace(' ', '_') for c in df.columns]
        df['USUARIO'] = df['USUARIO'].astype(str).str.strip().str.lower()
        df = df[df['USUARIO'] != 'nan']
        df = df[df['USUARIO'] != '']
        print(">>> Excel login OK | Columnas:", df.columns.tolist(), "| Usuarios:", len(df))
        return df
    except Exception as e:
        print(f">>> ERROR Excel login: {e}")
        return pd.DataFrame()


def obtener_datos_usuario(correo):
    df_datos, df_accesos, df_usuarios = cargar_datos()
    if df_datos.empty or df_accesos.empty or df_usuarios.empty:
        return pd.DataFrame(), ""
    df_usuarios['USUARIO'] = df_usuarios['USUARIO'].astype(str).str.strip().str.lower()
    correo_lower = correo.strip().lower()
    fila_usuario = df_usuarios[df_usuarios['USUARIO'] == correo_lower]
    if fila_usuario.empty:
        return pd.DataFrame(), ""
    nombre_completo = fila_usuario.iloc[0]['NAME_USUARIO']
    usuario_sistema = str(fila_usuario.iloc[0]['USUARIO_NOMBRE']).strip().lower()
    df_accesos['USUARIO'] = df_accesos['USUARIO'].astype(str).str.strip().str.lower()
    accesos_usuario = df_accesos[df_accesos['USUARIO'] == usuario_sistema]
    if accesos_usuario.empty:
        return pd.DataFrame(), nombre_completo
    ccus_permitidos = accesos_usuario['CCU'].astype(str).str.strip().tolist()
    df_datos['CENTRO_COSTO'] = df_datos['CENTRO_COSTO'].astype(str).str.strip()
    df_filtrado = df_datos[df_datos['CENTRO_COSTO'].isin(ccus_permitidos)]
    return df_filtrado, nombre_completo


def calcular_chequeras_por_mes(df):
    resultado = {mes: {} for mes in MESES_ORDEN}
    if df.empty:
        return resultado
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    col_mes      = None
    col_chequera = 'NOMBRE_CHEQUERA'
    col_debito   = 'DEBITOS'
    if 'NOMBRE_MES' in df.columns:
        col_mes = 'NOMBRE_MES'
    elif 'NUM_MESES' in df.columns:
        col_mes = 'NUM_MESES'
    if col_mes is None or col_chequera not in df.columns or col_debito not in df.columns:
        return resultado
    df[col_debito] = pd.to_numeric(df[col_debito], errors='coerce').fillna(0)
    df_debitos = df[df[col_debito] > 0].copy()
    if df_debitos.empty:
        return resultado
    if col_mes == 'NOMBRE_MES':
        agrupado = df_debitos.groupby([col_mes, col_chequera])[col_debito].sum().reset_index()
        for _, row in agrupado.iterrows():
            mes_upper = str(row[col_mes]).strip().upper()
            chequera  = str(row[col_chequera]).strip()
            monto     = round(float(row[col_debito]), 2)
            if mes_upper in resultado and chequera.lower() not in ('nan', ''):
                resultado[mes_upper][chequera] = monto
    elif col_mes == 'NUM_MESES':
        mapa_num_mes = {1:'ENERO',2:'FEBRERO',3:'MARZO',4:'ABRIL',5:'MAYO',6:'JUNIO',
                        7:'JULIO',8:'AGOSTO',9:'SEPTIEMBRE',10:'OCTUBRE',11:'NOVIEMBRE',12:'DICIEMBRE'}
        df_debitos['_MES_NOMBRE'] = pd.to_numeric(df_debitos[col_mes], errors='coerce').map(mapa_num_mes)
        agrupado = df_debitos.groupby(['_MES_NOMBRE', col_chequera])[col_debito].sum().reset_index()
        for _, row in agrupado.iterrows():
            mes_upper = str(row['_MES_NOMBRE']).strip().upper()
            chequera  = str(row[col_chequera]).strip()
            monto     = round(float(row[col_debito]), 2)
            if mes_upper in resultado and chequera.lower() not in ('nan', ''):
                resultado[mes_upper][chequera] = monto
    for mes in resultado:
        resultado[mes] = dict(sorted(resultado[mes].items(), key=lambda x: x[1], reverse=True))
    return resultado


def calcular_resumen_saldos(df):
    if df.empty:
        return []
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    if 'CENTRO_COSTO' not in df.columns or 'NOMBRE_CHEQUERA' not in df.columns:
        return []
    df['CREDITOS'] = pd.to_numeric(df['CREDITOS'], errors='coerce').fillna(0)
    df['DEBITOS']  = pd.to_numeric(df['DEBITOS'],  errors='coerce').fillna(0)
    cols_grupo = ['CENTRO_COSTO', 'NOMBRE_CHEQUERA']
    if 'DES_RESPONSABLE' in df.columns:
        cols_grupo.insert(1, 'DES_RESPONSABLE')
    df_resumen = df.groupby(cols_grupo).agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index()
    df_resumen = df_resumen.sort_values(cols_grupo)
    resultado = []
    for _, row in df_resumen.iterrows():
        resultado.append({
            'centro_costo': str(row['CENTRO_COSTO']).strip(),
            'chequera':     str(row['NOMBRE_CHEQUERA']).strip(),
            'creditos':     round(float(row['CREDITOS']), 2),
            'debitos':      round(float(row['DEBITOS']),  2),
            'desc_centro':  str(row.get('DES_RESPONSABLE', '')).strip()
        })
    return resultado


def calcular_resumen(df):
    if df.empty:
        return {'total_creditos':'Q0.00','total_debitos':'Q0.00','saldo_neto':'Q0.00'}, \
               {m:{'creditos':0,'debitos':0} for m in MESES_ORDEN}, \
               '<p style="color:#e8f5e9;padding:20px;">No hay datos disponibles.</p>'
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    df['CREDITOS'] = pd.to_numeric(df['CREDITOS'], errors='coerce').fillna(0)
    df['DEBITOS']  = pd.to_numeric(df['DEBITOS'],  errors='coerce').fillna(0)
    if 'MONTO' not in df.columns:
        df['MONTO'] = df['CREDITOS'] - df['DEBITOS']
    total_creditos = df['CREDITOS'].sum()
    total_debitos  = df['DEBITOS'].sum()
    saldo_neto     = total_creditos - total_debitos
    kpis = {
        'total_creditos': f'Q{total_creditos:,.2f}',
        'total_debitos':  f'Q{total_debitos:,.2f}',
        'saldo_neto':     f'Q{saldo_neto:,.2f}'
    }
    datos_meses = {m:{'creditos':0,'debitos':0} for m in MESES_ORDEN}
    if 'NOMBRE_MES' in df.columns:
        df_mes = df.groupby('NOMBRE_MES').agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index()
        for _, row in df_mes.iterrows():
            mes_upper = str(row['NOMBRE_MES']).strip().upper()
            if mes_upper in datos_meses:
                datos_meses[mes_upper] = {'creditos':round(float(row['CREDITOS']),2),'debitos':round(float(row['DEBITOS']),2)}
    elif 'NUM_MESES' in df.columns:
        df['_MES'] = pd.to_numeric(df['NUM_MESES'], errors='coerce')
        df_mes = df.groupby('_MES').agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index()
        for _, row in df_mes.iterrows():
            try:
                idx = int(row['_MES']) - 1
                if 0 <= idx < 12:
                    datos_meses[MESES_ORDEN[idx]] = {'creditos':round(float(row['CREDITOS']),2),'debitos':round(float(row['DEBITOS']),2)}
            except:
                pass
    columnas_tabla = ['DES_RESPONSABLE','TIPO_TR','NOMBRE_CHEQUERA','FECHA_DOCU','NO_DOCU','OBSERVACIONES','CREDITOS','DEBITOS','MONTO']
    cols_exist = [c for c in columnas_tabla if c in df.columns]
    tabla_html = _build_table_html(df[cols_exist].head(1000).copy())
    return kpis, datos_meses, tabla_html


def _build_table_html(df):
    if df.empty:
        return '<p style="color:#e8f5e9;padding:20px;">Sin datos</p>'
    cols_num = ['CREDITOS','DEBITOS','MONTO']
    html = '<table class="detail-table"><thead><tr>'
    for col in df.columns:
        html += f'<th>{col}</th>'
    html += '</tr></thead><tbody>'
    for _, row in df.iterrows():
        html += '<tr>'
        for col in df.columns:
            val = row[col]
            extra = ''
            if col in cols_num:
                try:
                    num = float(str(val).replace(',','')) if str(val) not in ['','nan','None'] else 0
                    if col == 'CREDITOS':
                        extra = ' class="num credito"'; val = f'{num:,.2f}'
                    elif col == 'DEBITOS':
                        extra = ' class="num debito"'; val = f'{num:,.2f}'
                    elif col == 'MONTO':
                        extra = f' class="num {"monto-neg" if num < 0 else "monto-pos"}"'; val = f'{num:,.2f}'
                except:
                    extra = ' class="num"'
            html += f'<td{extra}>{val}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html


def obtener_segmentadores(df):
    empty = {k:[] for k in ['administraciones','gerencias','centros_costo','responsables','chequeras','cod_administraciones','cod_responsables','cod_centros']}
    if df.empty:
        return empty
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    def uniq(col):
        if col in df.columns:
            vals = df[col].dropna().astype(str).str.strip()
            vals = vals[vals.str.lower() != 'nan']
            return sorted(vals.unique().tolist())
        return []
    return {
        'administraciones':     uniq('DES_ADMINSTRACION'),
        'gerencias':            uniq('DES_GERENCIA'),
        'centros_costo':        uniq('CENTRO_COSTO'),
        'responsables':         uniq('DES_RESPONSABLE'),
        'chequeras':            uniq('NOMBRE_CHEQUERA'),
        'cod_administraciones': uniq('ADMINISTRACION'),
        'cod_responsables':     uniq('GERENCIA'),
        'cod_centros':          uniq('RESPONSABLE'),
    }


@app.route('/')
def index():
    if 'usuario' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        email    = request.form['email'].lower().strip()
        password = request.form['password'].strip()

        df_usuarios = cargar_usuarios_login()

        if df_usuarios.empty:
            error = 'Error interno. Contacte al administrador.'
            return render_template('login.html', error=error)

        fila = df_usuarios[df_usuarios['USUARIO'] == email]

        if fila.empty:
            error = 'Correo no encontrado'
            return render_template('login.html', error=error)

        password_correcto = str(fila.iloc[0]['PASSWORD']).strip()
        if password != password_correcto:
            error = 'Contrasena incorrecta'
            return render_template('login.html', error=error)

        session['usuario'] = email
        session['nombre']  = str(fila.iloc[0]['NAME_USUARIO']).strip()
        return redirect(url_for('dashboard'))

    return render_template('login.html', error=error)


@app.route('/dashboard')
def dashboard():
    if 'usuario' not in session:
        return redirect(url_for('login'))
    usuario = session['usuario']
    nombre  = session['nombre']
    df, _ = obtener_datos_usuario(usuario)
    kpis, datos_meses, tabla_html = calcular_resumen(df)
    segs  = obtener_segmentadores(df)
    datos_chequeras_mes = calcular_chequeras_por_mes(df)
    datos_resumen       = calcular_resumen_saldos(df)
    try:
        with open(os.path.join(BASE_DIR, 'data', 'metadata.json'), 'r', encoding='utf-8') as f:
            meta = json.load(f)
        ultima_actualizacion = meta.get('fecha', 'N/A')
    except:
        ultima_actualizacion = 'N/A'
    return render_template('dashboard.html',
        nombre=nombre, usuario=usuario, kpis=kpis,
        datos_meses=datos_meses, datos_chequeras_mes=datos_chequeras_mes,
        datos_resumen=datos_resumen, tabla_html=tabla_html,
        ultima_actualizacion=ultima_actualizacion, **segs)


@app.route('/api/filtrar', methods=['POST'])
def api_filtrar():
    if 'usuario' not in session:
        return jsonify({'error': 'no autenticado'}), 401
    filtros = request.get_json()
    df, _   = obtener_datos_usuario(session['usuario'])
    if df.empty:
        return jsonify({'kpis':{'total_creditos':'Q0.00','total_debitos':'Q0.00','saldo_neto':'Q0.00'},
                        'datos_meses':{m:{'creditos':0,'debitos':0} for m in MESES_ORDEN},
                        'datos_chequeras_mes':{m:{} for m in MESES_ORDEN},
                        'datos_resumen':[],'tabla_html':''})
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    mapa = {'administracion':'DES_ADMINSTRACION','gerencia':'DES_GERENCIA','responsable':'DES_RESPONSABLE',
            'centro_costo':'CENTRO_COSTO','chequera':'NOMBRE_CHEQUERA',
            'cod_responsable':'GERENCIA','cod_admin':'ADMINISTRACION','cod_centro':'RESPONSABLE'}
    for key, col in mapa.items():
        valores = filtros.get(key, [])
        if valores and col in df.columns:
            df = df[df[col].astype(str).str.strip().isin(valores)]
    kpis, datos_meses, tabla_html = calcular_resumen(df)
    return jsonify({'kpis':kpis,'datos_meses':datos_meses,
                    'datos_chequeras_mes':calcular_chequeras_por_mes(df),
                    'datos_resumen':calcular_resumen_saldos(df),'tabla_html':tabla_html})


@app.route('/exportar-excel')
def exportar_excel():
    if 'usuario' not in session:
        return redirect(url_for('login'))
    df, _ = obtener_datos_usuario(session['usuario'])
    if df.empty:
        return "No hay datos para exportar", 404
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Datos', index=False)
    output.seek(0)
    return send_file(output,
        download_name=f'reporte_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)