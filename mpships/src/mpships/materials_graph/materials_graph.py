"""Main module."""
__description__ = "This module provides the MaterialsGraphAIO class for creating interactive materials graphs."
__author__ = "Your Name"
__url__ = "data-analyzer"
__version__ = "0.1.0"
__preview__ = "/assets/preview.png"

from dash import html, dcc, callback, Input, Output, State, MATCH, no_update, Patch
import dash_ag_grid as dag
import pandas as pd
from mp_api.client import MPRester
import dash
from pymatgen.core.structure import Structure, Composition, Lattice
from mpships.vega_graph_table import VegaGraphTableAIO
import uuid


class MaterialsGraphAIO(html.Div):
    class ids:
        search_bar = lambda aio: {
            "component": "MaterialsGraphAIO",
            "aio": aio,
            "subcomponents": "searchUIAIO",
        }
        quickFilter = lambda aio: {
            "component": "MaterialsGraphAIO",
            "aio": aio,
            "subcomponents": "quickFilter",
        }
        button = lambda aio: {
            "component": "MaterialsGraphAIO",
            "aio": aio,
            "subcomponents": "button",
        }
        datatable = lambda aio: {
            "component": "MaterialsGraphAIO",
            "aio": aio,
            "subcomponents": "datatable",
        }
        vega_output = lambda aio: {
            "component": "MaterialsGraphAIO",
            "aio": aio,
            "subcomponents": "vega_output",
        }

    ids = ids

    def __init__(self, id=None, aio=None, **kwargs):
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

        searchbar = html.Div(
            [
                dcc.Input(
                    id=self.ids.search_bar(aio_id),
                    type="text",
                    placeholder="Enter chemical system",
                ),
                html.Button("Submit", n_clicks=0, id=self.ids.button(aio_id)),
            ]
        )
        quick_filter = dcc.Input(
            id=self.ids.quickFilter(aio_id), type="text", placeholder="Quick Filter"
        )

        datatable = dag.AgGrid(
            id=self.ids.datatable(aio_id),
            dashGridOptions={"pagination": True},
        )

        vega_output = dcc.Loading(html.Div(id=self.ids.vega_output(aio_id)))

        super().__init__(
            children=[searchbar, quick_filter, datatable, vega_output], **kwargs
        )

    @callback(
        Output(ids.datatable(MATCH), "dashGridOptions"),
        Input(ids.quickFilter(MATCH), "value"),
        allow_duplicate=True,
    )
    def update_filter(filter_value):
        newFilter = Patch()
        newFilter["quickFilterText"] = filter_value
        return newFilter

    @callback(
        Output(ids.datatable(MATCH), "rowData"),
        Output(ids.datatable(MATCH), "columnDefs"),
        Output(ids.vega_output(MATCH), "children"),
        Input(ids.button(MATCH), "n_clicks"),
        State(ids.search_bar(MATCH), "value"),
        prevent_initial_call=True,
        allow_duplicate=True,
    )
    def update_datatable(n_clicks, value):
        if not n_clicks:
            return no_update
        mpr = MPRester()
        docs = mpr.materials.summary.search(chemsys=value)

        doc_list = [_clean_dict(doc.model_dump()) for doc in docs]

        df = pd.DataFrame(doc_list)
        column_defs = [{"field": i} for i in df.columns]

        # create a VegaGraphTableAIO object
        vega_graph_table = VegaGraphTableAIO(aio="test", df=df)

        return df.to_dict("records"), column_defs, vega_graph_table


def _clean_dict(d):
    """
    Remove fields with None values or custom object types from the dictionary.
    """
    cleaned_dict = {}
    for key, value in d.items():
        if value is not None and not isinstance(
            value, (Structure, dict, Composition, Lattice, list)
        ):
            cleaned_dict[key] = value
    return cleaned_dict


if __name__ == "__main__":
    app = dash.Dash(__name__, suppress_callback_exceptions=True, use_pages=False)
    app.layout = html.Div(MaterialsGraphAIO(aio="test"))
    app.run_server(debug=True)
