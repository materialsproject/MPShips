"""Main module."""
from dash import html, dcc, callback, Input, Output, State, MATCH, dash_table
from dash.exceptions import PreventUpdate
import altair as alt
import dash_vega_components as dvc
from mpships.redis_store import redis_store


class VegaGraphTableAIO(html.Div):
    class ids:
        store = lambda aio: {
            "component": "VegaGraphTableAIO",
            "aio": aio,
            "subcomponents": "store",
        }

        vega_table = lambda aio: {
            "component": "VegaGraphTableAIO",
            "aio": aio,
            "subcomponents": "vega_table",
        }
        vega_graph = lambda aio: {
            "component": "VegaGraphTableAIO",
            "aio": aio,
            "subcomponents": "vega_graph",
        }

    ids = ids

    def __init__(self, aio, df, graph_props=None, table_props=None, **kwargs):
        self.aio = aio

        store_data = {}
        if df is not None:
            store_data["df"] = redis_store.save(df)
        store = dcc.Store(id=self.ids.store(aio), data=store_data)
        if graph_props:
            vega_graph = dvc.Vega(
                id=self.ids.vega_graph(aio),
                spec=_make_chart(df),
                signalsToObserve=["brush_selection"],
                **graph_props,
            )
        else:
            vega_graph = dvc.Vega(
                id=self.ids.vega_graph(aio),
                spec=_make_chart(df),
                signalsToObserve=["brush_selection"],
            )

        vega_table = dash_table.DataTable(
            id=self.ids.vega_table(aio),
            columns=[{"name": i, "id": i} for i in df.columns],
            page_action="native",
            page_size=10,
        )

        super().__init__(children=[store, vega_graph, vega_table], **kwargs)

    @callback(
        Output(ids.vega_table(MATCH), "data"),
        Input(ids.vega_graph(MATCH), "signalData"),
        State(ids.store(MATCH), "data"),
        prevent_initial_call=True,
    )
    def update_datatable(signal_data, store_data):
        if not signal_data:
            raise PreventUpdate
        brush_selection = signal_data.get("brush_selection", {})

        df = redis_store.load(store_data["df"])
        if brush_selection:
            filter = " and ".join(
                [f"{v[0]} <= `{k}` <= {v[1]}" for k, v in brush_selection.items()]
            )
            filtered_source = df.query(filter)
        else:
            filtered_source = df
        return filtered_source.to_dict("records")


def _make_chart(df):
    chart = (
        alt.Chart(df)
        .mark_point(size=90)
        .encode(
            alt.X("volume").scale(zero=False),
            alt.Y("formation_energy_per_atom").scale(zero=False),
            color="num_unique_magnetic_sites",
        )
    )

    brush = alt.selection_interval(name="brush_selection")
    chart = chart.add_params(brush)
    return chart.to_dict()
