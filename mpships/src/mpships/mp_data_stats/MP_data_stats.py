from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import json
from ptable_plotly import ptable_heatmap_plotly
from ptable_info import elements_dict, empty_element_count

# from pymatviz import ptable_heatmap_plotly

# MP API endpoints that contain chemical element information
endpoint_list = [
    'absorption',
    'bonds',
    'dielectric',
    'elasticity',
    'electronic_structure_bandstructure', 
    'electronic_structure_dos',
    'electronic_structure',
    'insertion_electrodes',
    'magnetism',
    'oxidation_states',
    'piezoelectric',
    'summary', 
    'thermo',
]

app = Dash()
"""
app.layout = [
    html.H1(children='Materials Project Data Statistics', style={'textAlign':'center'}),
    dcc.Dropdown(
        options=endpoint_list, 
        value='absorption', 
        id='dropdown-selection'
    ),
    html.Div(
        children=[
            dcc.Graph(id='histogram', style={'width': '40%', 'display': 'inline-block', 'vertical-align': 'top'}),
            dcc.Graph(id='ptable_endpoint', style={'width': '55%', 'display': 'inline-block', 'vertical-align': 'top'}),
            dcc.Graph(id='ptable_endpoint_element', style={'width': '55%', 'display': 'inline-block', 'vertical-align': 'top'}),
        ])
    ]
"""
app.layout = [
    # title and dropdown
    html.Div([
        html.H1(
            children='Materials Project Data Statistics', style={'textAlign':'center'}
        ),
        dcc.Dropdown(
            options=endpoint_list, 
            value='absorption', 
            id='dropdown-selection'
        )
        ]
    ),  
    # figure
    html.Div([
        # Left figure: histogram
        html.Div([
            dcc.Graph(id='histogram')
            ], 
            style={'flex': '1', 'margin-right': '10px'}
        ),  # Left column, larger

        # Right column
        html.Div([
            # Top right figure
            html.Div([
                dcc.Graph(id='ptable_endpoint')
            ], 
            style={'flex': '1.5', 'width': '50%'}
            ),  # Top right

            # Bottom right figure
            html.Div([
                dcc.Graph(id='ptable_endpoint_element')
            ])  # Bottom right
        ], style={'flex': '1.5'})  # Right column, taller
    ], style={'display': 'flex'})] # Using flexbox for layout


@callback(
    Output('histogram', 'figure'),
    Output('ptable_endpoint', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_fig_enpoint(end_point):
    
    global api_end_point
    api_end_point = end_point
    
    # histogram
    with open(f"./data/count.json", 'r') as json_file:
        endpoint_material_count = json.load(json_file)
    endpoint_count_df = pd.DataFrame(endpoint_material_count, index=[0]).T.reset_index()
    endpoint_count_df.columns = ['endpoint', 'count']
    endpoint_count_df.sort_values('count', inplace=True)
    colors = ['blue' if endpoint != end_point else 'red' for endpoint in endpoint_count_df['endpoint']]
    fig_hist = px.bar(
        endpoint_count_df, 
        x='endpoint', 
        y='count', 
        color=colors
    )
    fig_hist.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=20, b=20),
        yaxis=dict(type='log')
    )

    # chemical elements distribution
    with open(f"./data/count_ele_{end_point}.json", 'r') as json_file:
        element_count = json.load(json_file)
    fig_enpoint = ptable_heatmap_plotly(
            element_count,
            # hover_props=["atomic_number", "type"],
            hover_props=["name"],
            scaling_factor=0.7
        )
    
    return(fig_hist, fig_enpoint)


@callback(
    Output('ptable_endpoint_element', 'figure'),
    Input('dropdown-selection', 'value'),
    Input('ptable_endpoint', 'clickData'),
)
def update_fig_endpoint_element(end_point, clickData):
    if clickData:
        # Access the hover text from clickData
        try:
            element_name = clickData['points'][0]['text'].split('<br>')[0]
        except:
            element_name = None
        if element_name:
            symbol = elements_dict[element_name]
            
            # get 
            with open(f"./data/elemental/count_ele_ele_{api_end_point}.json", 'r') as json_file:
                element_element_count = json.load(json_file)
            
            if symbol in element_element_count:
            
                # chemical elements distribution on specific element-based
                fig_endpoint_element = ptable_heatmap_plotly(
                        element_element_count[symbol],
                        hover_props=["name"],
                        scaling_factor=0.7
                    )
                return(fig_endpoint_element)

    fig_endpoint_element = ptable_heatmap_plotly(
        empty_element_count, 
        scaling_factor=0.7
    )
    return(fig_endpoint_element)
        

if __name__ == '__main__':
    app.run(debug=True)

