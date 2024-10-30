import os
import dash
import dash_bootstrap_components as dbc
from DatabricksChatbot import DatabricksChatbot

external_scripts = [
    {'src': 'https://cdn.tailwindcss.com'}
]

# Initialize the Dash app with a clean theme
app = dash.Dash(__name__,
    external_stylesheets=[dbc.themes.FLATLY, "https://cdn.jsdelivr.net/npm/daisyui@4.12.13/dist/full.min.css"],
    external_scripts=external_scripts,
    use_pages=True,

    # dev_tools_props_check=False
    )

# Define the app layout
app.layout = dbc.Container([
], fluid=True, className='h-screen max-h-screen')

if __name__ == '__main__':
    app.run_server(debug=False, dev_tools_ui=None, dev_tools_props_check=None)
