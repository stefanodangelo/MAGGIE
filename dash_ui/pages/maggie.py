import dash
from dash import html
import os
import dash
import dash_bootstrap_components as dbc
from DatabricksChatbot import DatabricksChatbot

serving_endpoint = os.getenv('SERVING_ENDPOINT')
assert serving_endpoint, 'SERVING_ENDPOINT must be set in app.yaml.'

dash.register_page(__name__)

chatbot = DatabricksChatbot(endpoint_name=serving_endpoint, height='600px')

layout = html.Div([
    chatbot.layout
], className='h-screen max-h-screen')
