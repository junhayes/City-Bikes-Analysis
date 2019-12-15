import time
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
import psycopg2

#connection to SQL
connection = psycopg2.connect(
    host='[host_name].eu-west-1.rds.amazonaws.com',
    port=5432,
    user='username',
    password='password',
    database='database'
)
sleeptime = 300

#Function to call live date per city
def live_dash(city):
    # Variable inputs per city
    if city == "dublin":
        height = 1000
        left = 275
    if city == "brussels":
        height = 7000
        left = 350
    if city == "goteborg":
        height = 1000
        left = 275
    app = dash.Dash()

#SQL query for most recently updated rows per station
    SQL_Query_LiveDash = pd.read_sql_query(
            "SELECT * FROM bikes.bikestations JOIN bikes."+ city +"_live ON bikes.bikestations.id::text = bikes."+ city +"_live.stand_number::text ORDER BY bikes.bikestations.name DESC;",
            connection)

    df = pd.DataFrame(SQL_Query_LiveDash)
#Dashboard layout and graph
    colors = {
        'background': '#111111',
        'text': '#7FDBFF'
    }
    app.layout = html.Div(style={'backgroundColor': colors['background'], 'display': 'inline-block', 'width': '60%' }, children=[
        html.H1(
            children='Bike Availability',
            style={
                'textAlign': 'center',
                'color': colors['text']
            }
        ),
        html.Div(children= city, style={
            'textAlign': 'center',
            'color': colors['text']
        }),
        dcc.Graph(
            id='Bike Availability',
            figure={
                'data': [go.Bar(
                        x = df['available_bikes'],
                        y = df['name'],
                        name = 'Available Bikes',
                        orientation = 'h'),
                        go.Bar(
                        x = df['available_bikes_stands'],
                        y = df['name'],
                        name = 'Available Stands',
                        orientation = 'h')
                ],
                'layout': {
                    'margin': {'l': left, 'b': 40, 't': 10, 'r': 10},
                    'plot_bgcolor': colors['background'],
                    'paper_bgcolor': colors['background'],
                    'height': height,
                    'font': {
                        'color': colors['text']
                    }
                }
            }
        )
        ])
    if __name__ == '__main__':
        app.run_server(debug=True)
        
    #if __name__ == '__main__':
    #application.run(debug=False, port=8080)    #Used in live version of dashboard
    
while 1:
    live_dash('brussels')
    time.sleep(sleeptime)