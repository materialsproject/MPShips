import dash # Required for MPShips functionality — do not remove
from mpships_infra import MPShipsApp # Required for MPShips functionality — do not remove

from dash import html, dcc, callback, Output, Input
from dash.exceptions import PreventUpdate

import dash_bootstrap_components as dbc
import plotly.graph_objects as go


from.data_processing import (
    load_experiment_list,
    load_experiment_data,
)
from .sublayout import (
    generate_experiment_content
)

class Alab(MPShipsApp): # Required for MPShips functionality — do not remove
    # Optional — remove if your app needs no setup, will be called in __init__
    def ships_setup(self, *args, **kwargs):
        # Example code — replace with your own layout
        # Exampled code
        # self.my_role = "Navigator"
        pass
    
    # Required — defines what renders. Omitting this will raise an error on load.
    def ships_layout(self):
        """The layout of the main content.

        Returns:
            html.Div: The Dash layout for this app.
        """
        # Load experiment list for dropdown
        experiment_list = load_experiment_list()
        default_experiment = experiment_list[0]["identifier"]

        return dbc.Container(
            [
                # Main content
                html.Div(
                    id="main-content",
                    style={"display": "block"},
                    children=[
                        # Header with experiment selector
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.H1(
                                            "A-Lab Reaction Genome Portal",
                                            className="display-4 fw-bold text-dark",
                                        ),
                                        html.P(
                                            [
                                                "Material Discovery & Analysis Platform by the ",
                                                html.A(
                                                    "Ceder Group at Berkeley",
                                                    href="https://ceder.berkeley.edu/",
                                                    target="_blank",
                                                    className="text-primary text-decoration-none fw-semibold",
                                                    style={
                                                        "borderBottom": "1px dotted #0d6efd"
                                                    },
                                                ),
                                                ".",
                                            ],
                                            className="lead text-muted",
                                        ),
                                    ],
                                    md=6,
                                    className="py-4",
                                ),
                                dbc.Col(
                                    [
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Select Experiment:",
                                                    className="fw-semibold mb-2",
                                                ),
                                                dcc.Dropdown(
                                                    id="experiment-selector",
                                                    options=[
                                                        {
                                                            "label": f"{exp['identifier']} - {exp.get('formula', 'N/A')}",
                                                            "value": exp["identifier"],
                                                        }
                                                        for exp in experiment_list
                                                    ],
                                                    value=default_experiment,
                                                    clearable=False,
                                                    searchable=True,
                                                    placeholder="Search experiments...",
                                                ),
                                            ],
                                            className="py-4",
                                        )
                                    ],
                                    md=6,
                                ),
                            ]
                        ),
                        # Dynamic content container
                        dcc.Loading(
                            html.Div(id="experiment-content"),
                            style={"position": "fixed", "top": "50%", "left": "50%"},
                        ),
                    ],
                )
            ],
            fluid=True,
            className="py-4",
            style={"backgroundColor": "#f9fafb"},
        )
    

    # Optional — remove if your app has no callbacks
    def ships_callbacks(self, app, cache):
        # Example code — replace with your own layout
        # Exampled code
        @app.callback(
            Output("experiment-content", "children"),
            [Input("experiment-selector", "value")],
        )
        def update_experiment_content(experiment_name):
            """Load and display data for selected experiment"""
            if not experiment_name:
                return html.Div(
                    [
                        dbc.Alert(
                            "Please select an experiment",
                            color="info",
                            className="mt-4",
                        )
                    ]
                )

            # Load experiment data
            raw_data = load_experiment_data(experiment_name)

            # Generate content
            return generate_experiment_content(raw_data)