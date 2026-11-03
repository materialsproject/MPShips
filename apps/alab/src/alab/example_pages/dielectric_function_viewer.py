import dash # Required for MPShips functionality — do not remove
from mpships_infra import get_rester, MPShipsApp # Required for MPShips functionality — do not remove

from dash import html, dcc, callback, Output, Input
from dash.exceptions import PreventUpdate
import plotly.graph_objects as pgo

# Use `get_rester()` for retrieving data from MPRester
# Example:
# get_rester().materials.summary.search(
#        chemsys=["Au"], fields=["material_id", "has_props"]
#    )
# See more information at: https://docs.materialsproject.org/downloading-data/using-the-api/getting-started


class DielectricFunctionViewer(MPShipsApp): # Required for MPShips functionality — do not remove
    # Define your app layout here
    def ships_layout(self): # Required for MPShips functionality — do not remove
        return html.Div(
            [
                html.H2("Display dielectric function", style={"textAlign": "center"}),
                html.Div(
                    [
                        dcc.Input(id="input-box", value="Al-*", debounce=True),
                        html.Button("Enter", id="enter-btn", n_clicks=0),
                    ],
                    style={"marginBottom": "20px"}
                ),
                dcc.Loading(
                    html.Div(
                        [
                            dcc.Graph(id="fig-real-dielec", style={"flex": 1}),
                            dcc.Graph(id="fig-imag-dielec", style={"flex": 1}),
                        ],
                        style={"display": "flex"},
                    ),
                ),
            ]
        )

    # Callbacks in Dash Pages use the global @callback decorator
    def ships_callbacks(self, app, cache):
        @app.callback(
            Output("fig-real-dielec", "figure"),
            Output("fig-imag-dielec", "figure"),
            Input("input-box", "value"),
            Input("enter-btn", "n_clicks"),
            prevent_initial_call=True,
        )
        def update_figure(input_text, n_clicks):
            if not input_text:
                raise PreventUpdate

            # Perform MP search
            all_mat_list = get_rester().materials.summary.search(
                chemsys=input_text, fields=["material_id", "has_props"]
            )

            opt_mpids = [
                mat.material_id for mat in all_mat_list 
                if mat.has_props.get("absorption")
            ]

            if not opt_mpids:
                return pgo.Figure(), pgo.Figure()

            mat_op_docs = get_rester().materials.absorption.search(
                material_ids=opt_mpids,
                fields=[
                    "material_id",
                    "formula_pretty",
                    "energies",
                    "average_imaginary_dielectric",
                    "average_real_dielectric",
                ],
            )

            fig_real = pgo.Figure()
            fig_imag = pgo.Figure()

            for doc in mat_op_docs:
                fig_real.add_trace(
                    pgo.Scatter(
                        x=doc.energies,
                        y=doc.average_real_dielectric,
                        name=doc.formula_pretty,
                    )
                )
                fig_imag.add_trace(
                    pgo.Scatter(
                        x=doc.energies,
                        y=doc.average_imaginary_dielectric,
                        name=doc.formula_pretty,
                    )
                )

            fig_real.update_layout(
                title="Real dielectric function",
                title_x=0.5,
                xaxis_title="Energy (eV)",
                yaxis_title="ε₁",
            )
            fig_imag.update_layout(
                title="Imaginary dielectric function",
                title_x=0.5,
                xaxis_title="Energy (eV)",
                yaxis_title="ε₂",
            )

            return fig_real, fig_imag