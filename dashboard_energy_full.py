# dashboard_energy_full.py
import pandas as pd
import dash
from dash import html, dcc, dash_table
import plotly.express as px
from sqlalchemy import create_engine
import os

# ------------------------------
# Connessione al DB
# ------------------------------
DB_URL = os.getenv("DB_URL")  # impostata nelle env vars di Render
if not DB_URL:
    raise ValueError("DB_URL non trovato. Imposta l'Internal Database URL su Render nelle env vars.")

engine = create_engine(DB_URL)

def fetch_df(query):
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print("Errore fetch_df:", e)
        return pd.DataFrame()

# ------------------------------
# Caricamento dati
# ------------------------------
consumption = fetch_df("SELECT country_code, timestamp, consumption_mwh FROM consumption;")
production = fetch_df("""
SELECT p.country_code, e.source_name, p.timestamp, p.production_mwh
FROM production p
JOIN energy_sources e ON p.source_id = e.source_id;
""")
flows = fetch_df("SELECT from_country, to_country, timestamp, flow_mwh FROM crossborder_flows;")

# ------------------------------
# Preprocessing
# ------------------------------
for df in [consumption, production, flows]:
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        # Assicurarsi che i valori numerici siano float
        for col in df.select_dtypes(include='object').columns:
            if 'mwh' in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce')

# ------------------------------
# KPI per paese
# ------------------------------
kpi_list = []
countries = consumption['country_code'].unique() if not consumption.empty else []

for country in countries:
    cons_country = consumption[consumption['country_code'] == country].copy()
    prod_country = production[production['country_code'] == country].copy()
    net_country = flows[(flows['from_country'] == country) | (flows['to_country'] == country)].copy() if not flows.empty else pd.DataFrame()

    # Rimuovo timezone
    for df in [cons_country, prod_country, net_country]:
        if not df.empty:
            df['timestamp'] = df['timestamp'].dt.tz_convert(None)

    # Periodi
    cons_country['date'] = cons_country['timestamp'].dt.floor('D')
    cons_country['month_start'] = cons_country['timestamp'].dt.to_period('M').apply(lambda r: r.start_time)
    cons_country['year'] = cons_country['timestamp'].dt.year

    # Totali giornalieri, mensili e annuali
    daily_totals = cons_country.groupby('date')['consumption_mwh'].sum().reset_index(name='total')
    daily_totals['avg'] = daily_totals['total'].mean()

    monthly_totals = cons_country.groupby('month_start')['consumption_mwh'].sum().reset_index(name='total')
    monthly_totals['avg'] = monthly_totals['total'].mean()

    yearly_totals = cons_country.groupby('year')['consumption_mwh'].sum().reset_index(name='total')
    yearly_totals['avg'] = daily_totals['total'].mean()

    # KPI Produzione
    total_prod = prod_country['production_mwh'].sum()
    energy_mix = prod_country.groupby('source_name')['production_mwh'].sum()
    energy_mix_percent = (energy_mix / energy_mix.sum() * 100).round(1).to_dict() if not energy_mix.empty else {}

    # KPI Net Import/Export
    if not net_country.empty:
        total_in = net_country[net_country['to_country'] == country]['flow_mwh'].sum()
        total_out = net_country[net_country['from_country'] == country]['flow_mwh'].sum()
        net_import_export = total_in - total_out
    else:
        net_import_export = 0

    kpi_list.append({
        'country': country,
        'daily': daily_totals,
        'monthly': monthly_totals,
        'yearly': yearly_totals,
        'total_prod': total_prod,
        'energy_mix_percent': energy_mix_percent,
        'net_import_export': net_import_export
    })

# ------------------------------
# Dash App
# ------------------------------
app = dash.Dash(__name__)
app.title = "Energy Dashboard"

# Helper KPI box
def kpi_box(title, value, subtitle=None):
    display_value = f"{value:,.2f}" if isinstance(value, (int,float)) else str(value)
    return html.Div([
        html.Div(title, style={'fontSize':'14px','color':'#555'}),
        html.Div(display_value, style={'fontSize':'24px','fontWeight':'bold'}),
        html.Div(subtitle or '', style={'fontSize':'12px','color':'#888'})
    ], style={
        'border':'1px solid #ccc','borderRadius':'8px','padding':'15px','margin':'5px',
        'flex':'1','textAlign':'center','backgroundColor':'#f9f9f9','boxShadow':'2px 2px 5px rgba(0,0,0,0.1)'
    })

# ------------------------------
# Tabs layout
# ------------------------------
tabs_children = []

# --- KPI Tab ---
kpi_sections = []
for kpi in kpi_list:
    country = kpi['country']

    daily_dash_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in kpi['daily'].columns],
        data=kpi['daily'].to_dict('records'),
        page_size=10, style_table={'overflowX':'auto'}
    )
    monthly_dash_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in kpi['monthly'].columns],
        data=kpi['monthly'].to_dict('records'),
        page_size=10, style_table={'overflowX':'auto'}
    )
    yearly_dash_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in kpi['yearly'].columns],
        data=kpi['yearly'].to_dict('records'),
        page_size=10, style_table={'overflowX':'auto'}
    )
    consumption_tab = dcc.Tabs([
        dcc.Tab(label='Daily', children=html.Div([daily_dash_table], style={'padding':'10px'})),
        dcc.Tab(label='Monthly', children=html.Div([monthly_dash_table], style={'padding':'10px'})),
        dcc.Tab(label='Yearly', children=html.Div([yearly_dash_table], style={'padding':'10px'}))
    ])

    # Production & Mix
    prod_df = pd.DataFrame(list(kpi['energy_mix_percent'].items()), columns=['Source', 'Percentage'])
    prod_df['Total Production (MWh)'] = kpi['total_prod']
    prod_dash_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in prod_df.columns],
        data=prod_df.to_dict('records'),
        page_size=10, style_table={'overflowX':'auto'}
    )

    # Net Import/Export
    net_df = pd.DataFrame([{'Net Import/Export (MWh)': kpi['net_import_export']}])
    net_dash_table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in net_df.columns],
        data=net_df.to_dict('records'),
        style_table={'overflowX':'auto'}
    )

    kpi_sections.append(
        html.Div([
            html.H3(f"{country}", style={'textAlign':'center','marginBottom':'10px'}),
            dcc.Tabs([
                dcc.Tab(label='Consumption', children=consumption_tab),
                dcc.Tab(label='Production & Energy Mix', children=html.Div([prod_dash_table], style={'padding':'10px'})),
                dcc.Tab(label='Net Import/Export', children=html.Div([net_dash_table], style={'padding':'10px'}))
            ])
        ], style={'marginBottom':'30px'})
    )

tabs_children.append(dcc.Tab(label='KPIs', children=html.Div(kpi_sections, style={'padding':'20px'})))

# --- Visuals Tab ---
# Correggere tipi per Plotly
if not consumption.empty and not production.empty:
    daily_cons = consumption.groupby(['country_code', 'timestamp']).agg(total_mwh=('consumption_mwh','sum')).reset_index()
    daily_prod = production.groupby(['country_code', 'timestamp']).agg(total_mwh=('production_mwh','sum')).reset_index()
    daily_cons['timestamp'] = pd.to_datetime(daily_cons['timestamp'])
    daily_prod['timestamp'] = pd.to_datetime(daily_prod['timestamp'])
    daily_cons['total_mwh'] = daily_cons['total_mwh'].astype(float)
    daily_prod['total_mwh'] = daily_prod['total_mwh'].astype(float)
    time_df = pd.merge(daily_cons, daily_prod, on=['country_code','timestamp'], suffixes=('_cons','_prod'))
    fig_time = px.line(
        time_df,
        x='timestamp',
        y=['total_mwh_cons','total_mwh_prod'],
        color='country_code',
        labels={'value':'MWh','variable':'Serie','timestamp':'Data'},
        title='Time series: consumption vs. production'
    )
else:
    fig_time = px.line(title="Nessun dato disponibile")

tabs_children.append(dcc.Tab(label='Visuals', children=html.Div([dcc.Graph(figure=fig_time)], style={'padding':'20px'})))

# --- Tables Tab ---
daily_table = pd.DataFrame()  # puoi aggiungere aggregazioni qui
tabs_children.append(dcc.Tab(label='Tables', children=html.Div([html.H3("Tables section")], style={'padding':'20px'})))

# --- Layout finale ---
app.layout = html.Div([
    html.H1("Energy Dashboard", style={'textAlign':'center','marginBottom':'20px'}),
    dcc.Tabs(tabs_children)
], style={'maxWidth':'1200px','margin':'auto','fontFamily':'Arial, sans-serif'})

# --- Avvio server ---
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8050)))
