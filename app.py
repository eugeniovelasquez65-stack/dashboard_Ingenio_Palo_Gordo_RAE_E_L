from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import pandas as pd
import json
import os

app = Flask(__name__)
app.secret_key = 'clave_secreta_ipg_2026'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MESES_ORDEN = ['NOVIEMBRE','DICIEMBRE','ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
               'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE']


def cargar_datos():
    try:
        with open(os.path.join(BASE_DIR,'data','datos_presupuesto.json'),'r',encoding='utf-8') as f:
            datos=json.load(f)
        with open(os.path.join(BASE_DIR,'data','accesos_usuarios.json'),'r',encoding='utf-8') as f:
            accesos=json.load(f)
        with open(os.path.join(BASE_DIR,'data','usuarios_correos.json'),'r',encoding='utf-8') as f:
            usuarios=json.load(f)
        df_d=pd.DataFrame(datos); df_a=pd.DataFrame(accesos); df_u=pd.DataFrame(usuarios)
        df_d.columns=df_d.columns.str.strip().str.upper()
        df_a.columns=df_a.columns.str.strip().str.upper()
        df_u.columns=df_u.columns.str.strip().str.upper()
        return df_d,df_a,df_u
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()


def cargar_datos_erp():
    try:
        ruta=os.path.join(BASE_DIR,'data','datos_presupuesto_erp.json')
        with open(ruta,'rb') as f:
            raw_bytes=f.read()
        clean=bytes(b if b>=32 or b in(9,10,13) else 32 for b in raw_bytes)
        raw=json.loads(clean.decode('latin-1'))
        registros=raw if isinstance(raw,list) else next((v for v in raw.values() if isinstance(v,list)),[])
        df=pd.DataFrame(registros)
        if not df.empty:
            df.columns=df.columns.str.strip().str.upper()
            if 'USUARIO' in df.columns:
                df=df.drop(columns=['USUARIO'])
        return df
    except Exception as e:
        print(f"Error cargando datos ERP: {e}")
        return pd.DataFrame()


def cargar_usuarios_login():
    try:
        ruta=os.path.join(BASE_DIR,'data','COMPLEMENTO_DIRECTLY_1.xlsx')
        df=pd.read_excel(ruta,header=0,engine='openpyxl')
        df.columns=[str(c).strip().upper().replace(' ','_') for c in df.columns]
        df['USUARIO']=df['USUARIO'].astype(str).str.strip().str.lower()
        df=df[df['USUARIO'].notna() & (df['USUARIO']!='nan') & (df['USUARIO']!='')]
        print(">>> Excel login OK | Usuarios:",len(df))
        return df
    except Exception as e:
        print(f">>> ERROR Excel login: {e}")
        return pd.DataFrame()


def obtener_datos_usuario(correo):
    df_d,df_a,df_u=cargar_datos()
    if df_d.empty or df_a.empty or df_u.empty:
        return pd.DataFrame(),""
    df_u['USUARIO']=df_u['USUARIO'].astype(str).str.strip().str.lower()
    fila=df_u[df_u['USUARIO']==correo.strip().lower()]
    if fila.empty:
        return pd.DataFrame(),""
    nombre=fila.iloc[0]['NAME_USUARIO']
    usis=str(fila.iloc[0]['USUARIO_NOMBRE']).strip().lower()
    df_a['USUARIO']=df_a['USUARIO'].astype(str).str.strip().str.lower()
    accesos=df_a[df_a['USUARIO']==usis]
    if accesos.empty:
        return pd.DataFrame(),nombre
    ccus=accesos['CCU'].astype(str).str.strip().tolist()
    df_d['CENTRO_COSTO']=df_d['CENTRO_COSTO'].astype(str).str.strip()
    return df_d[df_d['CENTRO_COSTO'].isin(ccus)].copy(),nombre


def obtener_datos_erp_usuario(correo):
    df_base,_=obtener_datos_usuario(correo)
    if df_base.empty:
        return pd.DataFrame()
    df_erp=cargar_datos_erp()
    if df_erp.empty:
        r=df_base.copy()
        if 'USUARIO' in r.columns: r=r.drop(columns=['USUARIO'])
        return r
    if 'CENTRO_COSTO' in df_erp.columns:
        ccus=df_base['CENTRO_COSTO'].unique().tolist()
        df_erp['CENTRO_COSTO']=df_erp['CENTRO_COSTO'].astype(str).str.strip()
        df_erp=df_erp[df_erp['CENTRO_COSTO'].isin(ccus)]
    return df_erp.copy()


def aplicar_filtros_df(df, filtros):
    df=df.copy()
    df.columns=df.columns.str.strip().str.upper()
    # ADMINISTRACION → DESCRI_GER  |  GERENCIA → DESCRI_RES
    mapa={
        'administracion': 'DESCRI_GER',
        'gerencia':       'DESCRI_RES',
        'responsable':    'DES_RESPONSABLE',
        'centro_costo':   'CENTRO_COSTO',
        'chequera':       'NOMBRE_CHEQUERA',
        'cod_responsable':'GERENCIA',
        'cod_admin':      'ADMINISTRACION',
        'cod_centro':     'RESPONSABLE',
    }
    for key,col in mapa.items():
        valores=filtros.get(key,[])
        if valores and col in df.columns:
            df=df[df[col].astype(str).str.strip().isin(valores)]
    return df


def calcular_chequeras_por_mes(df):
    resultado={mes:{} for mes in MESES_ORDEN}
    if df.empty: return resultado
    df=df.copy(); df.columns=df.columns.str.strip().str.upper()
    col_mes=None
    if 'NOMBRE_MES' in df.columns: col_mes='NOMBRE_MES'
    elif 'NUM_MESES' in df.columns: col_mes='NUM_MESES'
    if not col_mes or 'NOMBRE_CHEQUERA' not in df.columns or 'DEBITOS' not in df.columns:
        return resultado
    df['DEBITOS']=pd.to_numeric(df['DEBITOS'],errors='coerce').fillna(0)
    df_deb=df[df['DEBITOS']>0].copy()
    if df_deb.empty: return resultado
    mapa_num={1:'ENERO',2:'FEBRERO',3:'MARZO',4:'ABRIL',5:'MAYO',6:'JUNIO',
              7:'JULIO',8:'AGOSTO',9:'SEPTIEMBRE',10:'OCTUBRE',11:'NOVIEMBRE',12:'DICIEMBRE'}
    if col_mes=='NUM_MESES':
        df_deb['_MN']=pd.to_numeric(df_deb[col_mes],errors='coerce').map(mapa_num)
        col_mes='_MN'
    ag=df_deb.groupby([col_mes,'NOMBRE_CHEQUERA'])['DEBITOS'].sum().reset_index()
    for _,r in ag.iterrows():
        m=str(r[col_mes]).strip().upper()
        ch=str(r['NOMBRE_CHEQUERA']).strip()
        if m in resultado and ch.lower() not in('nan',''):
            resultado[m][ch]=round(float(r['DEBITOS']),2)
    for m in resultado:
        resultado[m]=dict(sorted(resultado[m].items(),key=lambda x:x[1],reverse=True))
    return resultado


def calcular_resumen_saldos(df):
    if df.empty: return []
    df=df.copy(); df.columns=df.columns.str.strip().str.upper()
    if 'CENTRO_COSTO' not in df.columns or 'NOMBRE_CHEQUERA' not in df.columns: return []
    df['CREDITOS']=pd.to_numeric(df['CREDITOS'],errors='coerce').fillna(0)
    df['DEBITOS']=pd.to_numeric(df['DEBITOS'],errors='coerce').fillna(0)
    cols=['CENTRO_COSTO','NOMBRE_CHEQUERA']
    if 'DES_RESPONSABLE' in df.columns: cols.insert(1,'DES_RESPONSABLE')
    ag=df.groupby(cols).agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index().sort_values(cols)
    return [{'centro_costo':str(r['CENTRO_COSTO']).strip(),'chequera':str(r['NOMBRE_CHEQUERA']).strip(),
             'creditos':round(float(r['CREDITOS']),2),'debitos':round(float(r['DEBITOS']),2),
             'desc_centro':str(r.get('DES_RESPONSABLE','')).strip()} for _,r in ag.iterrows()]


def calcular_resumen(df):
    vacio={'total_creditos':'Q0.00','total_debitos':'Q0.00','saldo_neto':'Q0.00'}
    if df.empty:
        return vacio,{m:{'creditos':0,'debitos':0} for m in MESES_ORDEN},'<p style="color:#e8f5e9;padding:20px;">No hay datos.</p>'
    df=df.copy(); df.columns=df.columns.str.strip().str.upper()
    df['CREDITOS']=pd.to_numeric(df['CREDITOS'],errors='coerce').fillna(0)
    df['DEBITOS']=pd.to_numeric(df['DEBITOS'],errors='coerce').fillna(0)
    if 'MONTO' not in df.columns: df['MONTO']=df['CREDITOS']-df['DEBITOS']
    tc=df['CREDITOS'].sum(); td=df['DEBITOS'].sum(); sn=tc-td
    kpis={'total_creditos':f'Q{tc:,.2f}','total_debitos':f'Q{td:,.2f}','saldo_neto':f'Q{sn:,.2f}'}
    dm={m:{'creditos':0,'debitos':0} for m in MESES_ORDEN}
    mapa_num={1:'ENERO',2:'FEBRERO',3:'MARZO',4:'ABRIL',5:'MAYO',6:'JUNIO',
              7:'JULIO',8:'AGOSTO',9:'SEPTIEMBRE',10:'OCTUBRE',11:'NOVIEMBRE',12:'DICIEMBRE'}
    if 'NOMBRE_MES' in df.columns:
        for _,r in df.groupby('NOMBRE_MES').agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index().iterrows():
            m=str(r['NOMBRE_MES']).strip().upper()
            if m in dm: dm[m]={'creditos':round(float(r['CREDITOS']),2),'debitos':round(float(r['DEBITOS']),2)}
    elif 'NUM_MESES' in df.columns:
        df['_M']=pd.to_numeric(df['NUM_MESES'],errors='coerce')
        for _,r in df.groupby('_M').agg({'CREDITOS':'sum','DEBITOS':'sum'}).reset_index().iterrows():
            try:
                m=mapa_num.get(int(r['_M']))
                if m and m in dm: dm[m]={'creditos':round(float(r['CREDITOS']),2),'debitos':round(float(r['DEBITOS']),2)}
            except: pass
    cols_t=['DES_RESPONSABLE','TIPO_TR','NOMBRE_CHEQUERA','FECHA_OPER','NO_DOCU','OBSERVACIONES','CREDITOS','DEBITOS','MONTO']
    ce=[c for c in cols_t if c in df.columns]
    return kpis,dm,_build_table_html(df[ce].head(1000).copy())


def _build_table_html(df):
    if df.empty: return '<p style="color:#e8f5e9;padding:20px;">Sin datos</p>'
    cols_num=['CREDITOS','DEBITOS','MONTO']
    h='<table class="detail-table"><thead><tr>'+''.join(f'<th>{c}</th>' for c in df.columns)+'</tr></thead><tbody>'
    for _,row in df.iterrows():
        h+='<tr>'
        for col in df.columns:
            val=row[col]; extra=''
            if col in cols_num:
                try:
                    num=float(str(val).replace(',','')) if str(val) not in['','nan','None'] else 0
                    if col=='CREDITOS': extra=' class="num credito"'; val=f'{num:,.2f}'
                    elif col=='DEBITOS': extra=' class="num debito"'; val=f'{num:,.2f}'
                    else: extra=f' class="num {"monto-neg" if num<0 else "monto-pos"}"'; val=f'{num:,.2f}'
                except: extra=' class="num"'
            h+=f'<td{extra}>{val}</td>'
        h+='</tr>'
    return h+'</tbody></table>'


def obtener_segmentadores(df):
    empty={k:[] for k in['administraciones','gerencias','centros_costo','responsables','chequeras','cod_administraciones','cod_responsables','cod_centros']}
    if df.empty: return empty
    df=df.copy(); df.columns=df.columns.str.strip().str.upper()
    def u(col):
        if col not in df.columns: return []
        v=df[col].dropna().astype(str).str.strip()
        return sorted(v[v.str.lower()!='nan'].unique().tolist())
    return {
        # ADMINISTRACION usa DESCRI_GER  |  GERENCIA usa DESCRI_RES
        'administraciones': u('DESCRI_GER'),
        'gerencias':        u('DESCRI_RES'),
        'centros_costo':    u('CENTRO_COSTO'),
        'responsables':     u('DES_RESPONSABLE'),
        'chequeras':        u('NOMBRE_CHEQUERA'),
        'cod_administraciones': u('ADMINISTRACION'),
        'cod_responsables': u('GERENCIA'),
        'cod_centros':      u('RESPONSABLE'),
    }


def obtener_segmentadores_cascada(df):
    if df.empty: return {}
    df=df.copy(); df.columns=df.columns.str.strip().str.upper()
    def u(col):
        if col not in df.columns: return []
        v=df[col].dropna().astype(str).str.strip()
        return sorted(v[v.str.lower()!='nan'].unique().tolist())
    return {
        'administracion':  u('DESCRI_GER'),
        'gerencia':        u('DESCRI_RES'),
        'centro_costo':    u('CENTRO_COSTO'),
        'responsable':     u('DES_RESPONSABLE'),
        'chequera':        u('NOMBRE_CHEQUERA'),
        'cod_admin':       u('ADMINISTRACION'),
        'cod_responsable': u('GERENCIA'),
        'cod_centro':      u('RESPONSABLE'),
    }


@app.route('/')
def index():
    return redirect(url_for('dashboard') if 'usuario' in session else url_for('login'))


@app.route('/login', methods=['GET','POST'])
def login():
    error=None
    if request.method=='POST':
        email=request.form['email'].lower().strip()
        password=request.form['password'].strip()
        df_u=cargar_usuarios_login()
        if df_u.empty:
            return render_template('login.html',error='Error interno. Contacte al administrador.')
        fila=df_u[df_u['USUARIO']==email]
        if fila.empty:
            return render_template('login.html',error='Correo no encontrado')
        if password!=str(fila.iloc[0]['PASSWORD']).strip():
            return render_template('login.html',error='Contrasena incorrecta')
        session['usuario']=email
        session['nombre']=str(fila.iloc[0]['NAME_USUARIO']).strip()
        return redirect(url_for('dashboard'))
    return render_template('login.html',error=error)


@app.route('/dashboard')
def dashboard():
    if 'usuario' not in session: return redirect(url_for('login'))
    df,_=obtener_datos_usuario(session['usuario'])
    kpis,dm,tabla=calcular_resumen(df)
    segs=obtener_segmentadores(df)
    try:
        with open(os.path.join(BASE_DIR,'data','metadata.json'),'r',encoding='utf-8') as f:
            ultima_actualizacion=json.load(f).get('fecha','N/A')
    except: ultima_actualizacion='N/A'
    return render_template('dashboard.html',
        nombre=session['nombre'],usuario=session['usuario'],kpis=kpis,
        datos_meses=dm,datos_chequeras_mes=calcular_chequeras_por_mes(df),
        datos_resumen=calcular_resumen_saldos(df),tabla_html=tabla,
        ultima_actualizacion=ultima_actualizacion,**segs)


@app.route('/api/filtrar', methods=['POST'])
def api_filtrar():
    if 'usuario' not in session: return jsonify({'error':'no autenticado'}),401
    filtros=request.get_json() or {}
    df,_=obtener_datos_usuario(session['usuario'])
    if df.empty:
        return jsonify({'kpis':{'total_creditos':'Q0.00','total_debitos':'Q0.00','saldo_neto':'Q0.00'},
                        'datos_meses':{m:{'creditos':0,'debitos':0} for m in MESES_ORDEN},
                        'datos_chequeras_mes':{m:{} for m in MESES_ORDEN},
                        'datos_resumen':[],'tabla_html':'','segmentadores':{}})
    df=aplicar_filtros_df(df,filtros)
    kpis,dm,tabla=calcular_resumen(df)
    return jsonify({'kpis':kpis,'datos_meses':dm,'tabla_html':tabla,
                    'datos_chequeras_mes':calcular_chequeras_por_mes(df),
                    'datos_resumen':calcular_resumen_saldos(df),
                    'segmentadores':obtener_segmentadores_cascada(df)})


@app.route('/api/exportar-datos', methods=['POST'])
def api_exportar_datos():
    if 'usuario' not in session: return jsonify({'error':'no autenticado'}),401
    filtros=request.get_json() or {}
    df=obtener_datos_erp_usuario(session['usuario'])
    if not df.empty and filtros: df=aplicar_filtros_df(df,filtros)
    if df.empty: return jsonify({'columnas':[],'filas':[]})
    df=df.where(pd.notnull(df),None)
    return jsonify({'columnas':df.columns.tolist(),'filas':df.values.tolist()})


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


if __name__=='__main__':
    port=int(os.environ.get('PORT',10000))
    app.run(debug=False,host='0.0.0.0',port=port)