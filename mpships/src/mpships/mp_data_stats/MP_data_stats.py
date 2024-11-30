__description__ = "This module provides elemental distribution visualization of MP API endpoints."
__author__ = "Min-Hsueh Chiu"


from dash import Dash, html, dcc, callback, Output, Input, MATCH
import plotly.express as px
import pandas as pd
import json
from ptable_plotly import ptable_heatmap_plotly
from ptable_info import elements_dict, empty_element_count
import sys
import uuid


class MPDistAIO(html.Div):
    class ids:
        header = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "header"
        } 

        endpoint_dropdown = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "endpoint_dropdown"
        } 

        whole_graph = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "whole_graph"
        } 


        histogram = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "histogram"
        } 

        whole_ptable = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "whole_ptable"
        } 

        ptable_endpoint = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "ptable_endpoint"
        } 

        ptable_endpoint_title = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "ptable_endpoint_title"
        }

        ptable_endpoint_element = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "ptable_endpoint_element"
        } 

        ptable_endpoint_element_title = lambda aio:{
            "component": "MPDistAIO",
            "aio": aio,
            "subcomponents": "ptable_endpoint_element_title"
        } 

    ids = ids 

    def __init__(self, id=None, aio=None, **kwargs):

        if sys.version_info < (3, 10):
            # this is require for pymatgen
            raise RuntimeError("Python 3.10 or higher is required.")

        aio_id = aio
        if aio is None:
            # Otherwise use a uuid that has virtually no chance of collision.
            # Uuids are safe in dash deployments with processes
            # because this component's callbacks
            # use a stateless pattern-matching callback:
            # The actual ID does not matter as long as its unique and matches
            # the PMC `MATCH` pattern..
            aio_id = str(uuid.uuid4())
        self.aio = aio_id
        self.kwargs = kwargs

        # put your layout here 
        # endpoint_bar
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
        endpoint_bar = html.Div([
                            html.H1(
                                children='Materials Project Data Statistics', 
                                style={'textAlign':'center'},
                                id=self.ids.header(aio_id)
                            ),
                            dcc.Dropdown(
                                options=endpoint_list, 
                                value='absorption', 
                                id=self.ids.endpoint_dropdown(aio_id)
                            )
                            ]
                            )
        # histogram
        histogram = html.Div(
            [
            dcc.Graph(id=self.ids.histogram(aio_id))
            ], 
            style={
                'flex': '1',
                'width': '10%'
            }
        )

        # ptable_endpoint
        ptable_endpoint = html.Div([
                    html.H3(
                        children=f'Elemental distribution', 
                        style={'textAlign':'center'},
                        id=self.ids.ptable_endpoint_title(aio_id)
                    ),
                    dcc.Graph(
                            id=self.ids.ptable_endpoint(aio_id)
                        )
                ], 
                style={
                    'flex': '2'
                }
            )

        # ptable_endpoint_element
        ptable_endpoint_element = html.Div([
                    html.H3(
                        children=f'Elemental distribution', 
                        style={'textAlign':'center'},
                        id=self.ids.ptable_endpoint_element_title(aio_id)
                    ),
                    dcc.Graph(
                            id=self.ids.ptable_endpoint_element(aio_id)
                        )
                ]
            )

        whole_ptable = html.Div([
                ptable_endpoint,
                ptable_endpoint_element
            ], 
            style={
                # 'display': 'flex'
            },
            id=self.ids.whole_ptable(aio_id)
            )

        whole_graph = html.Div([
                histogram,
                whole_ptable
            ], 
            style={
                'display': 'flex'
            },
            id=self.ids.whole_graph(aio_id)
            )
        


        super().__init__(children=[
            endpoint_bar,
            whole_graph

        ], **kwargs)


    # put all your callbacks here   
    @callback(
        Output(ids.histogram(MATCH), 'figure'),
        Output(ids.ptable_endpoint(MATCH), 'figure'),
        Output(ids.ptable_endpoint_title(MATCH), 'children'),
        Input(ids.endpoint_dropdown(MATCH), 'value')
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
                scaling_factor=0.7,
            )

        if api_end_point:
            children_string = f'Elemental distribution at endpoint: {api_end_point}'
        else:
            children_string = 'Elemental distribution'  # Default text
        

        return(fig_hist, fig_enpoint, children_string)


    @callback(
        Output(ids.ptable_endpoint_element(MATCH), 'figure'),
        Output(ids.ptable_endpoint_element_title(MATCH), 'children'),
        Input(ids.endpoint_dropdown(MATCH), 'value'),
        Input(ids.ptable_endpoint(MATCH), 'clickData'),
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


                    children_string = f"""
                            Elemental distribution of ({symbol}-X) in the given endpoint: {api_end_point}
                        """

                    return(fig_endpoint_element, children_string)

        fig_endpoint_element = ptable_heatmap_plotly(
            empty_element_count, 
            scaling_factor=0.7
        )
        children_string = f'Elemental distribution at endpoint: {api_end_point}'
        return(fig_endpoint_element, children_string)

if __name__ == "__main__":
    app = Dash(__name__, suppress_callback_exceptions=True, use_pages=False)
    app.layout = html.Div(MPDistAIO(aio="test"))
    app.run_server(debug=True)