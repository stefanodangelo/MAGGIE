import dash
from dash import html, Input, Output, State, dcc, callback, clientside_callback
import dash_bootstrap_components as dbc
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from datetime import datetime
import json
from PIL import Image
import base64
import io
import dash_bootstrap_components as dbc


from services import build_string_input
from services.databricks_service import DatabricksService


class DatabricksChatbot:
    def __init__(self, endpoint_name, height='600px'):
        self.endpoint_name = endpoint_name
        self.height = height
        self.service = DatabricksService()

        self.qr_code_options = self.service.get_qr_code_options()

        try:
            print('Initializing WorkspaceClient...')
            self.w = self.service.client
            print('WorkspaceClient initialized successfully')
        except Exception as e:
            print(f'Error initializing WorkspaceClient: {str(e)}')
            self.w = None

        self.layout = self._create_layout()
        self._create_callbacks()
        self._add_custom_css()

        self.agent_messages = []

    def _create_layout(self):
        return html.Div([
            html.H2('Chat with MAGGIE', className='chat-title mb-3'),
            html.Div([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Dropdown(
                            id='maintenance-type',
                            options=[{'label': 'install', 'value': 'install'},
                                     {'label': 'repair', 'value': 'repair'},
                                     {'label': 'remove', 'value': 'remove'}
                                     ],  # Initial value (empty)
                            value='install',
                            placeholder="install/repair/remove",
                        ),

                        dcc.Dropdown(
                            id='part-qr-code',
                            options=self.qr_code_options,
                            value=None,  # Initial value (empty)
                            placeholder="Select one assembly of parts",
                            optionHeight=100,
                            maxHeight=400
                        ),

                        dcc.Dropdown(
                            id='parts-list',
                            options=[
                                {'label': 'Option 1', 'value': 'option1'},
                                {'label': 'Option 2', 'value': 'option2'},
                                {'label': 'Option 3', 'value': 'option3'}
                            ],
                            value=None,  # Initial value (empty)
                            placeholder="Select one part",
                            multi=True,
                            optionHeight=100,
                            maxHeight=400
                        ),

                        html.Button('Start conversation', id='send-initial-button', n_clicks=0,
                                    className='btn btn-neutral text-white hover:text-gray-500'),

                    ], className="max-h-screen"),
                ], className="w-1/4"),
                html.Div([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div(id='chat-history', className='chat-history'),
                        ], className='d-flex flex-column chat-body overflow-auto')
                    ], className='chat-card grow mb-3'),
                    html.Div([
                        dbc.Textarea(id='user-input', placeholder='Type your message here...'),
                        dbc.Button('Send', id='send-button', n_clicks=0,
                                   className='btn btn-circle btn-neutral text-white'),
                    ], className='flex mb-3 gap-2 mx-10'),
                    dcc.Store(id='assistant-trigger'),
                    dcc.Store(id='chat-history-store'),
                    html.Div(id='dummy-output', style={'display': 'none'}),
                ], className="flex flex-col grow w-full overflow-auto max-h-screen")
            ], className="flex grow gap-2 whitespace-pre-wrap w-full w-3/4 max-h-screen")
        ], className="h-screen flex flex-col justify-center items-center p-4 ")

    def _create_callbacks(self):
        @callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Output('user-input', 'value'),
            Output('assistant-trigger', 'data', allow_duplicate=True),
            Input('send-button', 'n_clicks'),
            Input('user-input', 'n_submit'),
            State('user-input', 'value'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def update_chat(send_clicks, user_submit, user_input, chat_history):
            if not user_input:
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update

            chat_history = chat_history or []
            chat_history.append({'role': 'user', 'content': user_input, 'created_at': datetime.now().strftime('%H:%M')})
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_typing_indicator())

            return chat_history, chat_display, '', {'trigger': True}

        @callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Output('assistant-trigger', 'data', allow_duplicate=True),
            Input('send-initial-button', 'n_clicks'),
            State('chat-history-store', 'data'),
            State('part-qr-code', 'value'),
            State('parts-list', 'value'),
            State('maintenance-type', 'value'),
            prevent_initial_call=True
        )
        def send_initial_message(click, chat_history, qr_code_part, partlist_part, maintenance_type):
            if not qr_code_part or not partlist_part:
                return dash.no_update, dash.no_update, dash.no_update

            qr_code_part_name = list(filter(lambda x: x['value'] == qr_code_part, self.qr_code_options))[0]['label']
            print(qr_code_part_name)

            query = build_string_input(maintenance_type, qr_code_part_name, partlist_part)
            chat_history = []
            chat_history.append({'role': 'user', 'content': query, 'created_at': datetime.now().strftime('%H:%M')})
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_typing_indicator())

            return chat_history, chat_display, {'trigger': True}

        @callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('source-buttons', 'n_clicks'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def more_info_buttons_selected(click, chat_history):
            if not click:
                return dash.no_update, dash.no_update
            print('More info button clicked')

            chat_history.append(
                {'role': 'user', 'content': 'What are the references?', 'created_at': datetime.now().strftime('%H:%M')})
            
            # reference_images = []
            # reference_message = 'The following documents were used to answer your question:'
            # for reference in self.agent_messages[-1]['references']:
            #     reference_images.append(reference['img_base64'])
            #     reference_message += '\n\t- page {} from <a href="{}">this document</a>'.format(reference['page_number'], reference['doc_uri'])

            # chat_history.append({'role': 'assistant', 'content': reference_message, 'created_at': datetime.now().strftime('%H:%M')})

            print(chat_history)
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_images_reference(self.agent_messages[-1]['references']))

            return chat_history, chat_display
        
        @callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Output('assistant-trigger', 'data', allow_duplicate=True),
            Input('know-more-button', 'n_clicks'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def know_more_button(click, chat_history):
            if not click:
                return dash.no_update, dash.no_update
            chat_history.append(
                {'role': 'user', 'content': 'Can you give me more information?', 'created_at': datetime.now().strftime('%H:%M')})
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_typing_indicator())

            return chat_history, chat_display, {'trigger': True}


        @callback(
            Output("parts-list", "options"),
            Input("part-qr-code", "value")
        )
        def _update_partlist(qr_code_id):
            if (qr_code_id):
                print(qr_code_id)
                return self.service.get_partlist_options(qr_code_id)
            return []

        @callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('assistant-trigger', 'data'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def process_assistant_response(trigger, chat_history):
            if not trigger or not trigger.get('trigger'):
                return dash.no_update, dash.no_update

            chat_history = chat_history or []
            if (not chat_history or not isinstance(chat_history[-1], dict)
                    or 'role' not in chat_history[-1]
                    or chat_history[-1]['role'] != 'user'):
                return dash.no_update, dash.no_update

            try:
                assistant_response = self._call_model_endpoint(chat_history)
                chat_history.append({
                    'role': 'assistant',
                    'content': assistant_response,
                    'created_at': datetime.now().strftime('%H:%M')
                })
            except Exception as e:
                error_message = f'Error: {str(e)}'
                print(error_message)  # Log the error for debugging
                chat_history.append({
                    'role': 'assistant',
                    'content': error_message,
                    'created_at': datetime.now().strftime('%H:%M')
                })

            chat_display = self._format_chat_display(chat_history)
            return chat_history, chat_display

    def _call_model_endpoint(self, messages, max_tokens=128):
        if self.w is None:
            raise Exception('WorkspaceClient is not initialized')
        
        chat_messages = [
            ChatMessage(
                content=message['content'],
                role=ChatMessageRole[message['role'].upper()]
            ) for message in messages
        ]
        try:
            print('Calling model endpoint...')
            response = self.w.serving_endpoints.query(
                name=self.endpoint_name,
                messages=chat_messages,
                max_tokens=max_tokens
            )
            print('Got response from model endpoint', response)
            self.agent_messages.append(json.loads(response.choices[0].message.content))
            message = self.agent_messages[-1]['answer']
            print('Model endpoint called successfully')
            return message
        except Exception as e:
            print(f'Error calling model endpoint: {str(e)}')
            raise

    def _format_chat_display(self, chat_history):
        all_messages = [
            html.Div([
                html.Div([
                    html.Div([
                        msg['content'],
                    ], className='chat-bubble ' + ('bg-slate-500 text-white' if msg['role'] == 'user' else '')),
                    html.Div(msg['created_at'], className='chat-footer opacity-50')

                ], className='chat ' + 'chat-end user-message' if msg['role'] == 'user' else 'chat-start')
                for msg in chat_history if isinstance(msg, dict) and 'role' in msg])
        ]
        

        is_last_message_from_assistant = chat_history[-1]['role'] == 'assistant'

        all_messages.append(self._create_assistant_buttons(visible=is_last_message_from_assistant))
        return all_messages
    
    def _create_assistant_buttons(self, visible: bool = False):
        return html.Div([
            dbc.Button('I want to know more', n_clicks=0, id='know-more-button',
                       className='btn btn-neutral btn-sm text-white'),
            dbc.Button('What is the source', n_clicks=0, id='source-buttons',
                       className='btn btn-neutral btn-sm text-white'),
        ], className='flex gap-2 mt-2 ml-4' + ' visible' if visible else ' hidden')

    def _create_typing_indicator(self):
        return html.Div([
            html.Div(className='chat-bubble typing-message',
                     children=[
                         html.Span(className='loading loading-dots loading-md')
                     ])
        ], className='flex chat chat-start assistant-container')

    def _create_images_reference(self, references):

        # if reference_images:
        #     pil_images = []
        #     for image in reference_images:
        #         img_data = base64.b64decode(image)
        #         pil_img = Image.open(io.BytesIO(img_data))
        #         pil_images.append(pil_img)

        #     # Process images in groups of 3 for each row
        #     for i in range(0, len(pil_images), 3):
        #         row_div = html.Div([
        #             html.Div([
        #                 html.Img(src=image, style={'width': '30%', 'padding': '5px'})
        #                 for image in pil_images[i:i+3]
        #             ], style={'display': 'flex', 'justify-content': 'center'})
        #         ])
        #         all_messages.append(row_div)

        pil_images = []
        for ref in references:
            img_data = base64.b64decode(ref['img_base64'])
            pil_img = Image.open(io.BytesIO(img_data))
            pil_images.append(pil_img)

        # Process images in groups of 3 for each row
        image_div = html.Div([
            html.Div([
                html.Img(src=pil_images[image_number])
            ] )
        for image_number in range(len(pil_images))], className='grid grid-cols-3 gap-2')

        # image_switcher_div = html.Div([
        #     html.A([image_number], href="#"+self.image_number_gen(image_number), className="btn btn-xs") for image_number in range(len(pil_images))
        # ], className="flex w-full justify-center gap-2 py-2")

        return html.Div([
            html.Div([
                html.P('The following documents were used to answer your question:'),
                html.Div(
                    [ 
                    html.P(
                        ['\t - page {} from '.format(ref['page_number']), html.A(ref['doc_uri'].split('/')[-1], href = ref['doc_uri'], className="link")]
                        ) for ref in references
                    ]),
                image_div,
                # image_switcher_div
            ], className="chat-bubble"),
        ], className='chat chat-start assistant-container')

    def image_number_gen(self, no):
        return 'item{}'.format(no)

    def _add_custom_css(self):
        custom_css = '''

        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        body {
            font-family: 'DM Sans', sans-serif;
            background-color: #F9F7F4; /* Oat Light */
        }
        .chat-title {
            font-size: 24px;
            font-weight: 700;
            color: #1B3139; /* Navy 800 */
            text-align: center;
        }
        .chat-card {
            border: none;
            background-color: #EEEDE9; /* Oat Medium */
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        .chat-body {
            flex-grow: 1;
            display: flex;
            flex-direction: column;
        }
        .chat-history {
            flex-grow: 1;
            overflow-y: auto;
            padding: 15px;
        }
        .user-container {
            justify-content: flex-end;
        }
        .chat-message {
            max-width: 80%;
            padding: 10px 15px;
            border-radius: 20px;
            font-size: 16px;
            line-height: 1.4;
        }
        .user-message {
            background-color: #FF3621; /* Databricks Orange 600 */
            color: white;
        }
        .assistant-message {
            background-color: #1B3139; /* Databricks Navy 800 */
            color: white;
        }
        .typing-message {
            background-color: #2D4550; /* Lighter shade of Navy 800 */
            color: #EEEDE9; /* Oat Medium */
            display: flex;
            justify-content: center;
            align-items: center;
            min-width: 60px;
        }
        .typing-dot {
            width: 8px;
            height: 8px;
            background-color: #EEEDE9; /* Oat Medium */
            border-radius: 50%;
            margin: 0 3px;
            animation: typing-animation 1.4s infinite ease-in-out;
        }
        .typing-dot:nth-child(1) { animation-delay: 0s; }
        .typing-dot:nth-child(2) { animation-delay: 0.2s; }
        .typing-dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes typing-animation {
            0% { transform: translateY(0px); }
            50% { transform: translateY(-5px); }
            100% { transform: translateY(0px); }
        }
        #user-input {
            border-radius: 20px;
            border: 1px solid #DCE0E2; /* Databricks Gray - Lines */
        }
        '''

        clientside_callback(
            """
            function(children) {
                var chatHistory = document.getElementById('chat-history');
                if(chatHistory) {
                    chatHistory.scrollTop = chatHistory.scrollHeight;
                }
                return '';
            }
            """,
            Output('dummy-output', 'children'),
            Input('chat-history', 'children'),
            prevent_initial_call=True
        )
