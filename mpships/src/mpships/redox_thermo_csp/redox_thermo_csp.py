import crystal_toolkit.components as ctc
import crystal_toolkit.helpers.layouts as ctl
import dash
import dash_ag_grid as dag
import gzip
import json
import logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import warnings
import os.path
import uuid
from dash import callback, dcc, html, MATCH, Patch, State, no_update
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from mpships.redox_thermo_csp.redox_views import InitData as ID
from mpships.redox_thermo_csp.redox_views import Isographs as Iso
from mpships.redox_thermo_csp.redox_views import energy_analysis
from mp_web.core.utils import (
    get_rester,
    get_tooltip,
)
from mp_web.settings import SETTINGS
from pymatgen.util.string import unicodeify

logger = logging.getLogger(__name__)


# Load the JSON file
with gzip.open(
    os.path.join(os.path.dirname(__file__), "exp_data.json.gz"), "r"
) as _data_file:
    _EXP_DATA = json.load(_data_file)

ISOGRAPHS_TOOLTIPS = {
    "Isotherm": "Shows the non-stoichiometry δ as a function of the oxygen partial pressure pO\N{SUPERSCRIPT TWO} (in bar) with fixed temperature T (in K)",
    "Isobar": "Shows the non-stoichiometry δ as a function of the temperature T (in K) with fixed oxygen partial pressure pO2 (in bar)",
    "Isoredox": "Shows the oxygen partial pressure pO2 (in bar) as a function of the temperature T (in K) with fixed non-stoichiometry δ",
    "Enthalpy (dH)": "Shows the redox enthalpy ΔH as a function of the non-stoichiometry δ. Please note: The experimental dataset only contains values of ΔH(δ) instead of ΔH(δ,T) due to the measurement method. The fixed temperature value T therefore only refers to the theoretical data.",
    "Entropy (dS)": "Shows the redox entropy ΔS as a function of the non-stoichiometry δ. Please note: The experimental dataset only contains values of ΔS(δ) instead of ΔS(δ,T) due to the measurement method. The fixed temperature value T therefore only refers to the theoretical data.",
    "Ellingham": "Shows ΔG0 as a function of the temperature T (in K) with fixed non-stoichiometry δ. The gray isobar line can be adjusted to account for different oxygen partial pressures according to ΔG(pO2) = ΔG0 - 1/2 * RT * ln(pO2). If ΔG0 is below the isobar line, the reduction occurs spontaneously.",
}


class RedoxThermoCSPAIO(html.Div):
    class ids:
        # define all components' ids here

        ################################
        # Isograph Tab Components
        ################################
        quick_filter = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "quick_filter",
        }
        isograph_information = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isograph_information",
        }
        isographs_load = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isographs_load",
        }
        isographs_mount = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isographs_mount",
        }
        isographs_tab = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isographs_tab",
        }
        energy_analysis_tab = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "energy_analysis_tab",
        }
        energy_analysis_mount = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "energy_analysis_mount",
        }
        energy_analysis_load = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "energy_analysis_load",
        }
        tabs = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "tabs",
        }
        isographs_data_table = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isographs_data_table",
        }
        temp_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "temp_slider",
        }
        temp_range_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "temp_range_slider",
        }
        pressure_range = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "pressure_range",
        }
        pressure_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "pressure_slider",
        }
        redox_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "redox_slider",
        }
        redox_temp_range = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "redox_temp_range",
        }
        dH_temp_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "dH_temp_slider",
        }
        dS_temp_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "dS_temp_slider",
        }
        elling_redox_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "elling_redox_slider",
        }
        elling_temp_range = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "elling_temp_range",
        }
        elling_pressure_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "elling_pressure_slider",
        }
        isotherm = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isotherm",
        }
        enthalpy = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "enthalpy",
        }
        isobar = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isobar",
        }
        entropy = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "entropy",
        }
        isoredox = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "isoredox",
        }
        ellingham = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "ellingham",
        }
        ################################
        # Energy Analysis Tab components
        ################################
        t_ox_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "t_ox_slider",
        }
        t_red_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "t_red_slider",
        }
        p_ox_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "p_ox_slider",
        }
        p_red_slider = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "p_red_slider",
        }
        text_p_ox = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "text_p_ox",
        }
        h_rec_solid = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "h_rec_solid",
        }
        mech_env = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "mech_env",
        }
        pump_ener = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "pump_ener",
        }
        w_feed = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "w_feed",
        }
        w_hrec = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "w_hrec",
        }
        redox_thermo_explorer = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "redox_thermo_explorer",
        }
        process = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "process",
        }
        variable_input = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "variable_input",
        }
        enera_graph = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "enera_graph",
        }
        param_disp = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "param_disp",
        }
        no_disp = lambda aio: {
            "component": "RedoxThermoCSPAIO",
            "aio": aio,
            "subcomponents": "no_disp",
        }

    ids = ids

    how_to_cite = ctl.MessageContainer(
        [
            ctl.MessageHeader("How to Cite"),
            ctl.MessageBody(
                dcc.Markdown(
                    """
                        Please cite these publications if this data is useful for your work.
                        * [Perovskite Materials Design for Two-step Solar thermochemical Redox Cycles](https://doi.org/10.13140/RG.2.2.17964.92800)
                        * [Materials design of perovskite solid solutions for thermochemical applications](https://doi.org/10.1039/C9EE00085B)
                        * [Redox behavior of solid solutions in the SrFe1-xCuxO3-δ system for application in thermochemical oxygen storage and air separation] (https://doi.org/10.1002/ente.201800554)
                        * [Redox thermodynamics and phase composition in the system SrFeO3−δ — SrMnO3−δ.](https://doi.org/10.1016/j.ssi.2017.06.014)
                        * [Statistical thermodynamics of non-stoichiometric ceria and ceria zirconia solid solutions](https://doi.org/10.1039/c6cp03158g)
                        * [Formation of Na0.9Mo6O17 in a Solid-Phase Process. Transformations of a Hydrated Soldium Molybdenum Bronze, Na0.23(H2O)0.78MoO3, with Heat Treatments in a Nitrogen Atmosphere](https://doi.org/10.1246/bcsj.64.161)
                    """
                )
            ),
        ],
        kind="info",
    )

    def __init__(self, aio=None, *args, **kwargs):
        # define the layout here and put the layout inside of:
        # "super().__init__(children=[<< put layout here>>], **kwargs)"
        # at the end of the __init__ method.
        if aio is None:
            # Otherwise use a uuid that has virtually no chance of collision.
            # Uuids are safe in dash deployments with processes
            # because this component's callbacks
            # use a stateless pattern-matching callback:
            # The actual ID does not matter as long as its unique and matches
            # the PMC `MATCH` pattern..
            aio = str(uuid.uuid4())
        self.aio = aio
        self.kwargs = kwargs

        ##########################
        # layout
        ##########################

        tabs = dcc.Tabs(
            [
                dcc.Tab(
                    children=[
                        html.Br(),
                        html.Div(id=self.ids.isographs_mount(aio)),
                        dcc.Store(id=self.ids.isographs_load(aio), data=False),
                    ],
                    label="Isographs",
                    id=self.ids.isographs_tab(aio),
                    value="isographs",
                ),
                dcc.Tab(
                    children=[
                        html.Br(),
                        html.Div(id=self.ids.energy_analysis_mount(aio)),
                        dcc.Store(id=self.ids.energy_analysis_load(aio), data=False),
                    ],
                    label="Energy Analysis",
                    id=self.ids.energy_analysis_tab(aio),
                    value="energy",
                ),
            ],
            value="isographs",
            id=self.ids.tabs(aio),
        )
        super().__init__(children=[tabs], **kwargs)

    @classmethod
    def get_isographs_layout(cls, aio):
        """create layout for the isographs tab with the Dash AGGrid"""

        # lists of data for DataFrame
        formula = []
        oxidized_mpid = []
        oxidized_composition = []
        reduced_mpid = []
        reduced_composition = []
        theoretical_tolerance = []
        theoretical_composition = []
        theoretical_delta_H_min = []
        theoretical_delta_H_max = []
        solution = []
        availability = []
        updated = []

        # organize MpContribs data into lists for DataFrame
        mpr = get_rester()
        isographs_contributions_resp = mpr.contribs.query_contributions(
            query={
                "project": "redox_thermo_csp",
            },
            fields=[
                "data.phases",
                "data.theoretical",
                "data.solution",
                "data.availability",
                "data.updated",
            ],
            timeout=SETTINGS.CONTRIBS_TIMEOUT,
        )
        for entry in isographs_contributions_resp["data"]:
            phases = entry["data"]["phases"]
            formula.append(phases["oxidized"]["composition"])
            oxidized_mpid.append(phases["oxidized"]["mpid"])
            oxidized_composition.append(phases["oxidized"]["composition"])
            reduced_mpid.append(phases["reduced"]["mpid"])
            if "composition" in phases["reduced"]:
                reduced_composition.append(phases["reduced"]["composition"])
            else:
                reduced_composition.append("-")

            theo = entry["data"]["theoretical"]
            theoretical_tolerance.append(theo["tolerance"]["value"])
            theoretical_composition.append(theo["composition"])
            theoretical_delta_H_min.append(theo["ΔH"]["min"]["value"])
            theoretical_delta_H_max.append(theo["ΔH"]["max"]["value"])
            solution.append(entry["data"]["solution"])
            availability.append(entry["data"]["availability"])
            updated.append(entry["data"]["updated"])

        data = {
            "Formula": formula,
            "Oxidized mp-id": oxidized_mpid,
            "Oxidized Composition": oxidized_composition,
            "Reduced mp-id": reduced_mpid,
            "Reduced Composition": reduced_composition,
            "Theoretical Tolerance": theoretical_tolerance,
            "Theoretical Composition": theoretical_composition,
            "Theoretical ΔH Min (kJ/mol)": theoretical_delta_H_min,
            "Theoretical ΔH Max (kJ/mol)": theoretical_delta_H_max,
            "Solution": solution,
            "Availability": availability,
            "Last Updated": updated,
        }

        df = pd.DataFrame(data)

        isographs_data_table = html.Div(
            [
                dag.AgGrid(
                    id=cls.ids.isographs_data_table(aio),
                    columnDefs=[
                        {
                            "field": x,
                        }
                        for x in df.columns
                    ],
                    rowData=df.to_dict("records"),
                    className="ag-theme-quartz",
                    columnSize="autoSize",
                    dashGridOptions={
                        "rowSelection": "single",
                    },
                    selectedRows=df[
                        df["Theoretical Composition"] == "Sr1Fe1Ox"
                    ].to_dict("records"),
                ),
            ]
        )

        def get_plot(plot_title, plot, plot_id, sliders):
            """Create one of the isographs"""
            return ctl.Box(
                [
                    get_tooltip(
                        tooltip_label=ctl.H3(plot_title),
                        tooltip_text=ISOGRAPHS_TOOLTIPS[plot_title],
                    ),
                    dcc.Graph(id=plot_id, figure=plot),
                ]
                + [
                    ctl.Container(
                        [ctl.H4(slider["label"]), slider["component"], html.Br()]
                    )
                    for slider in sliders
                ]
            )

        def get_slider(id, min, max, value):
            """returns a dcc.Slider or dcc.RangeSlider component"""

            def get_marks():
                """helper function to get marks for the slider"""

                def round_mark(i, n):
                    """round marks up to n digits"""
                    return round((np.ceil(i / 10**n)) * 10**n, abs(int(n)))

                larger = np.max([abs(min), abs(max)])
                n = np.floor(np.log10(larger)) - 1
                return {
                    int(
                        "{}".format(int(round_mark(i, n)))
                        if round_mark(i, n).is_integer()
                        else int(round_mark(i, n))
                    ): "{}".format(
                        int(round_mark(i, n))
                        if round_mark(i, n).is_integer()
                        else round_mark(i, n)
                    )
                    for i in np.linspace(min, max, 6)
                }

            if type(value) != list:
                return dcc.Slider(
                    id=id,
                    min=min,
                    max=max,
                    value=value,
                    marks=get_marks() if max > 999 else {},
                    tooltip={"always_visible": False},
                )
            elif type(value) == list:
                return dcc.RangeSlider(
                    id=id,
                    min=min,
                    max=max,
                    value=value,
                    marks=get_marks() if max > 999 else {},
                    tooltip={"always_visible": False},
                )

        # sliders components for all the graphs
        temp_slider = get_slider(
            id=cls.ids.temp_slider(aio), min=500, max=1800, value=1000
        )
        pressure_range = dcc.RangeSlider(
            id=cls.ids.pressure_range(aio),
            min=-7,
            max=3,
            value=[-5, 1],
            marks={i: "{}".format(10**i) for i in range(-7, 4, 2)},
            tooltip={"always_visible": False, "transform": "powerOfTen"},
        )
        fig_0_sliders = [
            {"label": "Temperature (K)", "component": temp_slider},
            {"label": "Pressure Range (bar)", "component": pressure_range},
        ]

        pressure_slider = dcc.Slider(
            id=cls.ids.pressure_slider(aio),
            min=-7,
            max=4,
            value=0,
            marks={i: "{}".format(10**i) for i in range(-7, 5, 2)},
            tooltip={"always_visible": False, "transform": "powerOfTen"},
        )
        temp_range_slider = get_slider(
            id=cls.ids.temp_range_slider(aio), min=500, max=1800, value=[700, 1400]
        )
        fig_1_sliders = [
            {"label": "Pressure (bar)", "component": pressure_slider},
            {"label": "Temperature Range (K)", "component": temp_range_slider},
        ]

        redox_slider = get_slider(
            id=cls.ids.redox_slider(aio), min=0, max=0.5, value=0.3
        )
        redox_temp_range_slider = get_slider(
            id=cls.ids.redox_temp_range(aio), min=500, max=1800, value=[700, 1400]
        )
        fig_2_sliders = [
            {"label": "Redox δ", "component": redox_slider},
            {"label": "Temperature Range (K)", "component": redox_temp_range_slider},
        ]

        dH_temp_slider = get_slider(
            id=cls.ids.dH_temp_slider(aio), min=100, max=2000, value=500
        )
        fig_3_sliders = [{"label": "Redox δ", "component": dH_temp_slider}]

        dS_temp_slider = get_slider(
            id=cls.ids.dS_temp_slider(aio), min=100, max=2000, value=500
        )
        fig_4_sliders = [{"label": "Temperature (K)", "component": dS_temp_slider}]

        elling_redox_slider = get_slider(
            id=cls.ids.elling_redox_slider(aio), min=0, max=0.5, value=0.3
        )
        elling_temp_range = get_slider(
            id=cls.ids.elling_temp_range(aio), min=200, max=2000, value=[400, 1500]
        )
        elling_pressure_slider = get_slider(
            id=cls.ids.elling_pressure_slider(aio), min=-20, max=10, value=0
        )
        fig_5_sliders = [
            {"label": "Redox δ", "component": elling_redox_slider},
            {"label": "Temperature Range (K)", "component": elling_temp_range},
            {"label": "Isobar line", "component": elling_pressure_slider},
        ]

        return ctl.Container(
            [
                html.Div(
                    html.Div(
                        [
                            html.P(
                                "Search perovskites and view the relationships between their thermodynamic variables for different conditions",
                                style={
                                    "text-align": "center !important",
                                    "margin-bottom": ".5rem !important",
                                    "unicode-bidi": "isolate",
                                    "color": "#4a4a4a",
                                    "font-size": "18px",
                                    "font-weight": "400",
                                    "line-height": "1.5",
                                    "padding": "12px",
                                },
                            ),
                            dcc.Input(
                                id=cls.ids.quick_filter(aio),
                                placeholder="e.g. SrFeO3",
                                style={
                                    "font-size": "1.25rem",
                                    "background-color": "#fff",
                                    "border-color": "#dbdbdb",
                                    "border-radius": "4px",
                                    "color": "#363636",
                                    "webkit-appearance": "none",
                                    "align-items": "center",
                                    "border": "1px solid rgba(0, 0, 0, 0)",
                                    "height": "2.5em",
                                    "justify-content": "flex-start",
                                    "line-height": "1.5",
                                    "padding": "12px",
                                    "position": "relative",
                                    "vertical-align": "top",
                                    "min-width": "400px",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "justify-content": "center",
                            "align-items": "center",
                            "flex-direction": "column",
                            "position": "relative",
                            "width": "auto",
                            "unicode-bidi": "isolate",
                            "color": "#4a4a4a",
                            "font-size": "1em",
                            "font-weight": "400",
                            "line-height": "1.5",
                        },
                    ),
                    style={
                        "display": "flex",
                        "justify-content": "center",
                        "align-items": "center",
                        "background": "#ebebeb",
                        "border-bottom": "1px solid #dbdbdb",
                        "padding-bottom": "1.5rem",
                        "padding-top": "1.5rem",
                        "margin": "-1.5rem -1.5rem 1.5rem -1.5rem",
                        "unicode-bidi": "isolate",
                        "color": "#4a4a4a",
                        "font-size": "1em",
                        "font-weight": "400",
                        "line-height": "1.5",
                    },
                ),
                ctl.Box(
                    [
                        isographs_data_table,
                        ctl.H3(
                            id=cls.ids.isograph_information(aio),
                            style={
                                "padding-top": "24px",
                            },
                        ),
                        ctl.Columns(
                            [
                                ctl.Column(
                                    [
                                        get_plot(
                                            plot_title="Isotherm",
                                            plot={},
                                            plot_id=cls.ids.isotherm(aio),
                                            sliders=fig_0_sliders,
                                        ),
                                        get_plot(
                                            plot_title="Enthalpy (dH)",
                                            plot={},
                                            plot_id=cls.ids.enthalpy(aio),
                                            sliders=fig_3_sliders,
                                        ),
                                    ]
                                ),
                                ctl.Column(
                                    [
                                        get_plot(
                                            plot_title="Isobar",
                                            plot={},
                                            plot_id=cls.ids.isobar(aio),
                                            sliders=fig_1_sliders,
                                        ),
                                        get_plot(
                                            plot_title="Entropy (dS)",
                                            plot={},
                                            plot_id=cls.ids.entropy(aio),
                                            sliders=fig_4_sliders,
                                        ),
                                    ]
                                ),
                                ctl.Column(
                                    [
                                        get_plot(
                                            plot_title="Isoredox",
                                            plot={},
                                            plot_id=cls.ids.isoredox(aio),
                                            sliders=fig_2_sliders,
                                        ),
                                        get_plot(
                                            plot_title="Ellingham",
                                            plot={},
                                            plot_id=cls.ids.ellingham(aio),
                                            sliders=fig_5_sliders,
                                        ),
                                    ]
                                ),
                            ]
                        ),
                    ]
                ),
                cls.how_to_cite,
            ],
        )

    @classmethod
    def get_energy_analysis_layout(cls, aio):
        # create the layout that will appear in the "energy_analysis_tab"
        enera_fig = enera_fig_gen(en_dat=query_mp_contribs_energy_analysis())
        t_ox_slider = dcc.Slider(
            id=cls.ids.t_ox_slider(aio),
            min=350,
            max=800,
            step=None,
            value=500,
            tooltip={"always_visible": False},
            marks={
                350: "350",
                400: "400",
                450: "450",
                500: "500",
                600: "600",
                700: "700",
                800: "800",
            },
        )
        t_red_slider = dcc.Slider(
            id=cls.ids.t_red_slider(aio),
            min=600,
            max=1400,
            step=None,
            value=900,
            tooltip={"always_visible": False},
            marks={
                600: "600",
                700: "700",
                800: "800",
                900: "900",
                1000: "1000",
                1100: "1100",
                1200: "1200",
                1400: "1400",
            },
        )
        p_ox_slider = dcc.Slider(
            id=cls.ids.p_ox_slider(aio),
            min=-20,
            max=-3,
            step=None,
            value=-6,
            tooltip={"always_visible": False},
            marks={
                -20: "10⁻²⁰",
                -15: "10⁻¹⁵",
                -12: "10⁻¹²",
                -10: "10⁻¹⁰",
                -8: "10⁻⁸",
                -6: "⁻⁶",
                -5: "⁻⁵",
                -4: "⁻⁴",
                -3: "⁻³",
            },
        )
        p_red_slider = dcc.Slider(
            id=cls.ids.p_red_slider(aio),
            min=-8,
            max=0,
            step=None,
            value=-0.67778070526,
            tooltip={"always_visible": False},
            marks={
                -8: "10⁻⁸",
                -6: "10⁻⁶",
                -5: "10⁻⁵",
                -4: "10⁻⁴",
                -3: "10⁻³",
                -0.67778070526: "0.21",
                0: "1",
            },
        )

        contents = [
            ctl.Container(
                [
                    ctl.Box(
                        [
                            ctl.Columns(
                                [
                                    ctl.Column(
                                        [
                                            html.B(
                                                "Redox conditions",
                                                style={
                                                    "font-size": "24px",
                                                    "margin-bottom": "36px",
                                                },
                                            ),
                                            html.Br(),
                                            html.Br(),
                                            html.B(
                                                "Oxidation Temperature (°C)",
                                                style={"font-size": "20px"},
                                            ),
                                            t_ox_slider,
                                            html.Br(),
                                            html.B(
                                                id=cls.ids.text_p_ox(aio),
                                                children="Oxidation Partial Pressure of Oxygen (bar)",
                                                style={"font-size": "20px"},
                                            ),
                                            p_ox_slider,
                                            html.Br(),
                                            html.B(
                                                "Reduction Temperature (°C)",
                                                style={"font-size": "20px"},
                                            ),
                                            t_red_slider,
                                            html.Br(),
                                            html.B(
                                                "Reduction Partial Pressure of Oxygen (bar)",
                                                style={"font-size": "20px"},
                                            ),
                                            p_red_slider,
                                        ]
                                    ),
                                    ctl.Column(
                                        [
                                            html.B(
                                                "Process conditions",
                                                style={
                                                    "font-size": "24px",
                                                    "margin-bottom": "36px",
                                                },
                                            ),
                                            html.Br(),
                                            html.Br(),
                                            html.B(
                                                [
                                                    "Heat Recovery Efficiency η",
                                                    html.Sub(children="hrec, solid"),
                                                ],
                                                style={"font-size": "20px"},
                                            ),
                                            dcc.Slider(
                                                id=cls.ids.h_rec_solid(aio),
                                                min=0,
                                                max=0.99,
                                                value=0.6,
                                                tooltip={"always_visible": False},
                                            ),
                                            html.B(
                                                [
                                                    "Pumping energy Q",
                                                    html.Sub(children="pump"),
                                                ],
                                                style={"font-size": "20px"},
                                            ),
                                            ctl.Columns(
                                                [
                                                    ctl.Column(
                                                        [
                                                            dcc.Checklist(
                                                                id=cls.ids.mech_env(
                                                                    aio
                                                                ),
                                                                options=[
                                                                    {
                                                                        "label": "use mech. envelope (1e-6 to 0.7 bar)",
                                                                        "value": "mech_env_true",
                                                                    }
                                                                ],
                                                                value=["mech_env_true"],
                                                            ),
                                                            html.A(
                                                                children="(Brendelberger et al.)",
                                                                href="https://www.sciencedirect.com/science/article/abs/pii/S0038092X16305552",
                                                            ),
                                                        ]
                                                    ),
                                                    ctl.Column(
                                                        [
                                                            html.B("or define "),
                                                            dcc.Input(
                                                                id=cls.ids.pump_ener(
                                                                    aio
                                                                ),
                                                                type="number",
                                                                min=0.0,
                                                                placeholder="0.0",
                                                                size="5",
                                                                disabled=True,
                                                            ),
                                                            html.B(
                                                                "  kg/kJ of redox material"
                                                            ),
                                                        ]
                                                    ),
                                                ]
                                            ),
                                            ctl.Box(
                                                [
                                                    html.B("For water splitting only"),
                                                    html.Br(),
                                                    html.B(
                                                        "Water Feed Temperature (°C)",
                                                        style={"font-size": "20px"},
                                                    ),
                                                    dcc.Slider(
                                                        id=cls.ids.w_feed(aio),
                                                        min=5,
                                                        max=600,
                                                        value=200,
                                                        disabled=True,
                                                        tooltip={
                                                            "always_visible": False,
                                                            "placement": "bottom",
                                                        },
                                                    ),
                                                    html.B(
                                                        [
                                                            "Steam Heat Recovery η",
                                                            html.Sub(
                                                                children="hrec, steam"
                                                            ),
                                                        ],
                                                        style={"font-size": "20px"},
                                                    ),
                                                    dcc.Slider(
                                                        id=cls.ids.w_hrec(aio),
                                                        min=0,
                                                        max=0.99,
                                                        value=0.8,
                                                        disabled=True,
                                                        tooltip={
                                                            "always_visible": False,
                                                            "placement": "bottom",
                                                        },
                                                    ),
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            )
                        ]
                    )
                ]
            )
        ]

        layout = ctl.Container(
            [
                create_header(
                    "View the redox properties of perovskites for different thermodynamic applications"
                ),
                ctl.Columns(
                    [
                        ctl.Column(
                            [
                                html.B("Process Type"),
                                html.Div(
                                    [
                                        html.Div(
                                            [
                                                dcc.Dropdown(
                                                    id=cls.ids.process(aio),
                                                    options=[
                                                        {
                                                            "label": "Air Separation / Oxygen pumping / Oxygen storage",
                                                            "value": "AS",
                                                        },
                                                        {
                                                            "label": "Water splitting",
                                                            "value": "WS",
                                                        },
                                                        {
                                                            "label": "CO2 splitting",
                                                            "value": "CS",
                                                        },
                                                    ],
                                                    value="AS",
                                                )
                                            ]
                                        )
                                    ]
                                ),
                            ]
                        ),
                        ctl.Column([html.Div("")]),
                    ]
                ),
                html.Div(id=cls.ids.variable_input(aio), children=contents),
                html.Br(),
                ctl.Box(
                    [
                        html.Div(
                            ctl.Loading(
                                dcc.Graph(id=cls.ids.enera_graph(aio), figure=enera_fig)
                            )
                        ),
                        html.Br(),
                        html.Div(
                            [
                                html.B(children="Parameters to display"),
                                dcc.Dropdown(
                                    id=cls.ids.param_disp(aio),
                                    options=[
                                        {
                                            "label": "kJ/mol redox material",
                                            "value": "kJ/mol redox material",
                                        },
                                        {
                                            "label": "kJ/kg redox material",
                                            "value": "kJ/kg redox material",
                                        },
                                        {
                                            "label": "Wh/kg redox material",
                                            "value": "Wh/kg redox material",
                                        },
                                        {
                                            "label": "kJ/mol of product",
                                            "value": "kJ/mol of product",
                                        },
                                        {
                                            "label": "kJ/L of product",
                                            "value": "kJ/L of product",
                                        },
                                        {
                                            "label": "Wh/L of product",
                                            "value": "Wh/L of product",
                                        },
                                        {
                                            "label": "Heat to fuel efficiency in % (only valid for Water Splitting)",
                                            "value": "Heat to fuel efficiency in % (only valid for Water Splitting)",
                                        },
                                        {
                                            "label": "mol product per mol redox material",
                                            "value": "mol product per mol redox material",
                                        },
                                        {
                                            "label": "L product per mol redox material",
                                            "value": "L product per mol redox material",
                                        },
                                        {
                                            "label": "g product per mol redox material",
                                            "value": "g product per mol redox material",
                                        },
                                        {
                                            "label": "Change in non-stoichiometry between T_ox and T_red",
                                            "value": "Change in non-stoichiometry between T_ox and T_red",
                                        },
                                        {
                                            "label": "Mass change between T_ox and T_red",
                                            "value": "Mass change between T_ox and T_red",
                                        },
                                    ],
                                    value="kJ/mol of product",
                                ),
                            ]
                        ),
                        html.Br(),
                        html.Div(
                            children=[
                                html.B("Max number of materials to display"),
                                dcc.Slider(
                                    id=cls.ids.no_disp(aio),
                                    min=1,
                                    max=250,
                                    step=1,
                                    marks={
                                        1: "1",
                                        60: "60",
                                        120: "120",
                                        180: "180",
                                        250: "250",
                                    },
                                    value=15,
                                    tooltip={
                                        "always_visible": False,
                                        "placement": "bottom",
                                    },
                                ),
                            ],
                        ),
                    ]
                ),
                ctl.Box(
                    html.Details(
                        [
                            html.Summary(
                                "Click for additional information about the graphed quantities"
                            ),
                            html.Div(
                                [
                                    html.Table(
                                        # see this page for markdown styling tips: https://dash.plotly.com/dash-core-components/markdown
                                        dcc.Markdown(
                                            """
                                        \n
                                       **Chemical Energy**: The chemical energy is defined by the redox enthalpy and is calculated as the integral over ΔH(δ) from δ$_{ox}$ to δ$_{red}$. It is inversely proportional to the heat recovery efficiency η$_{hrec, solid}$, as a more efficient heat recovery system allows for a larger fraction of the redox material to be re-heated by the waste heat from the previous cycle.
                                        \n
                                       **Sensible Energy**: The chemical energy is defined by the heat capacity and is calculated using the Debye model based on the elastic tensors of the materials. Due to the relatively small changes in oxygen content of many perovskites upon reduction, this typically constitutes the largest share of energy consumption per mol of product. Please note that the experimental dataset does not contain heat capacity data, so the sensible energy is always based on theoretical considerations. The heat recovery efficiency η$_{hrec}$, solid has an analogous effect on the amount of sensible energy required per redox cycle as for the chemical energy.
                                        \n
                                       **Pumping Energy**: This refers to the energy required to pump the oxygen released during reduction out of the reactor chamber. It is independent of the reduction temperature or any oxidation conditions. No heat recovery from pumping is assumed. It is possible to either define a fixed value of pumping energy in terms of kJ/kg of redox material, or use the mechanical envelope function defined by [Brendelberger et. al.](https://www.sciencedirect.com/science/article/pii/S0038092X16305552) The latter refers to the minimum energy required to pump a certain amount of oxygen using mechanical pumps according to an analytical function, which is defined between 10$^{-6}$ bar and 0.7 bar total pressure (=oxygen partial pressure in this case) with an operating temperature of the pump of 200 °C. The value defined by the mechanical envelope may be undercut at low oxygen partial pressures [using thermochemical pumps](https://www.sciencedirect.com/science/article/pii/S0038092X18304961) as an alternative. Sweep gas operation cannot be modelled using our approach.
                                        \n
                                       **Steam Generation**: This value is only displayed if water splitting is selected as process type. It refers to the energy required to heat steam to the oxidation temperature of the redox material. The inlet temperature of the steam generator can be defined. If it is below 100 °C, the heat of evaporation (40.79 kJ/mol) and the energy required to heat liquid water to its boiling point are considered. The heat capacities for water and steam are calculated using data from the [NIST-JANAF thermochemical tables](https://janaf.nist.gov/). For water splitting, a lower ratio of H$_2$ vs. H$_2$O in the product stream increases the amount of energy required for steam generation significantly, as more water needs to be heated up to generate the same amount of hydrogen. It is also possible to define a heat recovery efficiency from steam, which may be different from the heat recovery efficiency from the solid.
                                        """,
                                            mathjax=True,
                                        )
                                    ),
                                ]
                            ),
                        ]
                    )
                ),
                cls.how_to_cite,
            ]
        )
        return layout

    # TODO see where a legend would look good for the isographs

    ######################
    # Isographs Callbacks
    ######################

    @callback(
        Output(ids.isographs_mount(MATCH), "children"),
        Output(ids.isographs_load(MATCH), "data"),
        Input(ids.tabs(MATCH), "value"),
        State(ids.tabs(MATCH), "id"),
        State(ids.isographs_load(MATCH), "data"),
        prevent_initial_call=False,  # run on first page load
    )
    def load_isographs_with_page(active_tab, tabs_id, loaded):
        if loaded or active_tab != "isographs":
            return no_update, loaded
        return RedoxThermoCSPAIO.get_isographs_layout(tabs_id["aio"]), True

    @callback(
        Output(ids.energy_analysis_mount(MATCH), "children"),
        Output(ids.energy_analysis_load(MATCH), "data"),
        Input(ids.tabs(MATCH), "value"),
        State(ids.tabs(MATCH), "id"),
        State(ids.energy_analysis_load(MATCH), "data"),
        prevent_initial_call=False,
    )
    def load_energy_analysis_at_page_load(active_tab, tabs_id, loaded):
        # `active_tab` == "isographs" to load with isographs component
        # this was initial behavior of the app, can change if needed
        if loaded or active_tab != "isographs":
            return no_update, loaded
        return RedoxThermoCSPAIO.get_energy_analysis_layout(tabs_id["aio"]), True

    @callback(
        Output(ids.isographs_data_table(MATCH), "dashGridOptions"),
        Input(ids.quick_filter(MATCH), "value"),
    )
    def update_filter(filter_value):
        newFilter = Patch()
        newFilter["quickFilterText"] = filter_value
        return newFilter

    @callback(
        Output(ids.isograph_information(MATCH), "children"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
    )
    def isograph_information_text(row):
        return f"Showing Isographs for {unicodeify(row[0]['Oxidized Composition'])}"

    @callback(
        Output(ids.isotherm(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.temp_slider(MATCH), "value"),
        Input(ids.pressure_range(MATCH), "value"),
    )
    def update_fig_0(row, temp_slider, pressure_range):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=0,
            theo_data=theo_data,
            compstr=compstr,
            constant=temp_slider,
            rng=pressure_range,
        )

    @callback(
        Output(ids.isobar(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.pressure_slider(MATCH), "value"),
        Input(ids.temp_range_slider(MATCH), "value"),
    )
    def update_fig_1(row, pressure_slider, temp_range_slider):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=1,
            theo_data=theo_data,
            compstr=compstr,
            constant=pressure_slider,
            rng=temp_range_slider,
        )

    @callback(
        Output(ids.isoredox(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.redox_slider(MATCH), "value"),
        Input(ids.redox_temp_range(MATCH), "value"),
    )
    def update_fig_2(row, redox_slider, redox_temp_range):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=2,
            theo_data=theo_data,
            compstr=compstr,
            constant=redox_slider,
            rng=redox_temp_range,
        )

    @callback(
        Output(ids.enthalpy(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.dH_temp_slider(MATCH), "value"),
    )
    def update_fig_3(row, dH_temp_slider):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=3,
            theo_data=theo_data,
            compstr=compstr,
            constant=dH_temp_slider,
        )

    @callback(
        Output(ids.entropy(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.dS_temp_slider(MATCH), "value"),
    )
    def update_fig_4(row, dS_temp_slider):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=4,
            theo_data=theo_data,
            compstr=compstr,
            constant=dS_temp_slider,
        )

    @callback(
        Output(ids.ellingham(MATCH), "figure"),
        Input(ids.isographs_data_table(MATCH), "selectedRows"),
        Input(ids.elling_redox_slider(MATCH), "value"),
        Input(ids.elling_temp_range(MATCH), "value"),
        Input(ids.elling_pressure_slider(MATCH), "value"),
    )
    def update_fig_5(
        row,
        elling_redox_slider,
        elling_temp_range,
        elling_pressure_slider,
    ):
        compstr = row[0]["Theoretical Composition"]
        theo_data = reformat_isograph_data(compstr)
        return get_figure(
            figure_number=5,
            theo_data=theo_data,
            compstr=compstr,
            constant=elling_pressure_slider,
            rng=elling_temp_range,
            delta=elling_redox_slider,
        )

    ###########################
    # Energy Analysis Callbacks
    ###########################

    @callback(
        Output(ids.w_feed(MATCH), "disabled"),
        Output(ids.w_hrec(MATCH), "disabled"),
        Input(ids.process(MATCH), "value"),
    )
    def enable_w_feed_and_w_hrec(process):
        """enable or disable the water feed temperature and steam heat recovery sliders
        depending on the user's choice of process"""
        if process == "WS":
            return False, False
        else:
            return True, True

    @callback(
        Output(ids.text_p_ox(MATCH), "children"), Input(ids.process(MATCH), "value")
    )
    def set_text_p_ox(process):
        if process == "AS":
            return "Oxidation Partial Pressure of Oxygen (bar)"
        elif process == "WS":
            return "Partial pressure ratio p(H2)/p(H2O)"
        else:
            return "Partial pressure ratio p(CO)/p(CO2)"

    @callback(
        Output(ids.t_ox_slider(MATCH), "min"),
        Output(ids.t_ox_slider(MATCH), "max"),
        Output(ids.t_ox_slider(MATCH), "value"),
        Output(ids.t_ox_slider(MATCH), "marks"),
        Output(ids.t_red_slider(MATCH), "min"),
        Output(ids.t_red_slider(MATCH), "max"),
        Output(ids.t_red_slider(MATCH), "value"),
        Output(ids.t_red_slider(MATCH), "marks"),
        Output(ids.p_ox_slider(MATCH), "min"),
        Output(ids.p_ox_slider(MATCH), "max"),
        Output(ids.p_ox_slider(MATCH), "value"),
        Output(ids.p_ox_slider(MATCH), "marks"),
        Output(ids.p_red_slider(MATCH), "min"),
        Output(ids.p_red_slider(MATCH), "max"),
        Output(ids.p_red_slider(MATCH), "value"),
        Output(ids.p_red_slider(MATCH), "marks"),
        Input(ids.process(MATCH), "value"),
    )
    def process_change_sliders(process):
        """change the slider parameters depending on the user's choice of process"""
        if process == "AS":
            return (
                350,
                800,
                500,
                {
                    350: "350",
                    400: "400",
                    450: "450",
                    500: "500",
                    600: "600",
                    700: "700",
                    800: "800",
                },
                600,
                1400,
                900,
                {
                    600: "600",
                    700: "700",
                    800: "800",
                    900: "900",
                    1000: "1000",
                    1100: "1100",
                    1200: "1200",
                    1400: "1400",
                },
                -20,
                -3,
                -6,
                {
                    -20: "10⁻²⁰",
                    -15: "10⁻¹⁵",
                    -12: "10⁻¹²",
                    -10: "10⁻¹⁰",
                    -8: "10⁻⁸",
                    -6: "⁻⁶",
                    -5: "⁻⁵",
                    -4: "⁻⁴",
                    -3: "⁻³",
                },
                -8,
                0,
                -0.67778070526,
                {
                    -8: "10⁻⁸",
                    -6: "10⁻⁶",
                    -5: "10⁻⁵",
                    -4: "10⁻⁴",
                    -3: "10⁻³",
                    -0.67778070526: "0.21",
                    0: "1",
                },
            )
        else:
            return (
                600,
                1150,
                900,
                {
                    600: "600",
                    700: "700",
                    800: "800",
                    900: "900",
                    1000: "1000",
                    1050: "1050",
                    1100: "1100",
                    1150: "1150",
                },
                1100,
                1500,
                1400,
                {
                    1100: "1100",
                    1200: "1200",
                    1250: "1250",
                    1300: "1300",
                    1350: "1350",
                    1400: "1400",
                    1450: "1450",
                    1500: "1500",
                },
                -6,
                -1,
                -3,
                {
                    -6: "10⁻⁶",
                    -5: "10⁻⁵",
                    -4: "10⁻⁴",
                    -3: "0.001",
                    -2: "0.01",
                    -1: "0.1",
                },
                -6,
                0,
                -0.67778070526,
                {
                    -6: "10⁻⁶",
                    -5: "10⁻⁵",
                    -3: "10⁻³",
                    -0.67778070526: "0.21",
                    0: "1",
                },
            )

    @callback(
        Output(ids.enera_graph(MATCH), "figure"),
        [
            Input(ids.t_ox_slider(MATCH), "value"),
            Input(ids.t_red_slider(MATCH), "value"),
            Input(ids.p_ox_slider(MATCH), "value"),
            Input(ids.p_red_slider(MATCH), "value"),
            Input(ids.h_rec_solid(MATCH), "value"),
            Input(ids.pump_ener(MATCH), "value"),
            Input(ids.w_feed(MATCH), "value"),
            Input(ids.w_hrec(MATCH), "value"),
            Input(ids.param_disp(MATCH), "value"),
            Input(ids.no_disp(MATCH), "value"),
            Input(ids.process(MATCH), "value"),
            Input(ids.mech_env(MATCH), "value"),
        ],
    )
    def update_enera(
        t_ox,
        t_red,
        p_ox,
        po2_red,
        h_rec_solid,
        pump_ener,
        w_feed,
        w_hrec,
        param_disp,
        no_disp,
        process,
        mech_env,
    ):
        ptype = "CO2 Splitting"
        if process == "AS":
            ptype = "Air separation / Oxygen pumping / Oxygen storage"
        elif process == "WS":
            ptype = "Water Splitting"
        if not pump_ener:
            pump_ener = 0.0
        p_red_exp = 10**po2_red
        if round(p_red_exp, 2) == 0.21:
            p_red_exp = 0.21
        if mech_env == ["mech_env_true"]:
            mech_env = True
        else:
            mech_env = False
        energy_data = query_mp_contribs_energy_analysis(
            process_type=process,
            t_ox=t_ox,
            t_red=t_red,
            p_ox=10**p_ox,
            p_red=p_red_exp,
            data_source="Theo",
            enth_steps=20,
        )

        fig = enera_fig_gen(
            en_dat=energy_data,
            data_source="Theoretical",
            process_type=ptype,
            t_ox=t_ox,
            t_red=t_red,
            p_ox=10**p_ox,
            p_red=p_red_exp,
            h_rec=h_rec_solid,
            mech_env=mech_env,
            cutoff=no_disp,
            pump_ener=pump_ener,
            w_feed=w_feed,
            steam_h_rec=w_hrec,
            param_disp=param_disp,
        )
        return fig

    @callback(
        Output(ids.pump_ener(MATCH), "disabled"), Input(ids.mech_env(MATCH), "value")
    )
    def toggle_pump_ener(mech_env):
        if mech_env == ["mech_env_true"]:
            return True
        return False


###############################
# energy analysis functions
###############################


def enera_fig_gen(
    en_dat,
    data_source="Theoretical",
    process_type="Air separation / Oxygen pumping / Oxygen storage",
    t_ox=500,
    t_red=1000.0,
    p_ox=1e-6,
    p_red=0.21,
    h_rec=0.6,
    mech_env=True,
    cutoff=25,
    pump_ener=0.0,
    w_feed=200.0,
    steam_h_rec=0.8,
    param_disp="kJ/mol of product",
):
    def get_energy_data(
        en_dat,
        data_source="Theoretical",
        process_type="Air separation / Oxygen pumping / Oxygen storage",
        t_ox=500,
        t_red=1000.0,
        p_ox=1e-6,
        p_red=0.21,
        h_rec=0.6,
        mech_env=True,
        cutoff=25,
        pump_ener=0.0,
        w_feed=200.0,
        steam_h_rec=0.8,
        param_disp="kJ/L of product",
    ):
        # (Below comment from the original coder (Josua Vieten?))
        # this data structure is from the old GET/POST request method
        # I decided to keep it for now to make as little changes as possible
        payload = {}
        payload["data_source"] = data_source
        if process_type == "Air separation / Oxygen pumping / Oxygen storage":
            payload["process_type"] = "Air Separation"
        else:
            payload["process_type"] = process_type
        payload["t_ox"] = t_ox
        payload["t_red"] = t_red
        payload["p_ox"] = p_ox
        payload["p_red"] = p_red
        payload["h_rec"] = h_rec
        payload["mech_env"] = mech_env
        payload["cutoff"] = cutoff
        payload["pump_ener"] = str(pump_ener)
        payload["w_feed"] = w_feed
        payload["steam_h_rec"] = steam_h_rec
        payload["param_disp"] = param_disp
        # since the experimental data only covers a limited range of delta values it is currently not reliable enough
        # to be used for the energy analysis; this parameter is therefore fixed
        payload["data_source"] = "Theoretical"

        enera = energy_analysis(en_dat, payload=payload)

        return enera

    data = get_energy_data(
        en_dat=en_dat,
        data_source=data_source,
        process_type=process_type,
        t_ox=t_ox,
        t_red=t_red,
        p_ox=p_ox,
        p_red=p_red,
        h_rec=h_rec,
        mech_env=mech_env,
        cutoff=cutoff,
        pump_ener=pump_ener,
        w_feed=w_feed,
        steam_h_rec=steam_h_rec,
        param_disp=param_disp,
    )
    x = data[0]["x"]
    y = [el["y"] for el in data if el["y"] is not None]
    name = [el["name"] for el in data if el["name"] is not None]
    try:
        title = data[0]["title"]
        yaxis_title = data[0]["yaxis_title"]
    except KeyError:
        title, yaxis_title = None, None  # noqa: F841

    bardata = []
    for n, slices in enumerate(y):
        bardata.append(go.Bar(name=name[n], x=x, y=slices))

    fig = go.Figure(data=bardata)
    if cutoff < 15:
        fig.update_xaxes(linecolor="rgb(0,0,0)", tickfont_size=24)
    elif cutoff < 19:
        fig.update_xaxes(linecolor="rgb(0,0,0)", tickfont_size=22)
    elif cutoff < 23:
        fig.update_xaxes(linecolor="rgb(0,0,0)", tickfont_size=20)
    elif cutoff < 26:
        fig.update_xaxes(linecolor="rgb(0,0,0)", tickfont_size=18)
    else:
        fig.update_xaxes(linecolor="rgb(0,0,0)", tickfont_size=16)
    fig.update_yaxes(
        title=yaxis_title,
        gridcolor="rgb(210,210,210)",
        title_font_size=24,
        tickfont_size=18,
    )

    fig.update_layout(
        barmode="stack",
        plot_bgcolor="rgb(255,255,255)",
        # showlegend=False,
        height=800,
        legend_font_size=18,
    )
    fig.update_xaxes()

    # if no data in MPContribs
    if not fig["data"]:
        return get_no_data_message()

    return fig


def query_mp_contribs_energy_analysis(
    process_type="AS",
    t_ox=500,
    t_red=1000,
    p_ox=1e-6,
    p_red=0.21,
    data_source="Theo",
    enth_steps=20,
):
    if process_type == "AS":
        db_id = "AS_"
    elif process_type == "WS":
        db_id = "WS_"
    else:
        db_id = "CS_"
    db_id += str(float(t_ox)) + "_"
    db_id += str(float(t_red)) + "_"
    db_id += str(float(p_ox)) + "_"
    db_id += str(float(p_red)) + "_"
    db_id += str(data_source) + "_"
    db_id += str(float(enth_steps))
    mpr = get_rester()
    # Fetch contribution-level data
    contributions_resp = mpr.contribs.query_contributions(
        query={"project": "redox_thermo_csp_energy", "data__id__exact": db_id},
        fields=[
            "data.prodstr",
            "data.unstable",
            "data.compstr",
            "data.id",
            "data.updated",
            "data.chemicalEnergy",
            "data.sensibleEnergy",
            "data.pRed",
            "data.lProdKgRed",
            "data.molMassOx",
            "data.molProdMolRed",
            "data.pOx",
            "data.TRed",
            "data.massRedox",
            "data.delta1",
            "data.TOx",
            "data.prodstrAlt",
            "data.gProdKgRed",
            "data.delta2",
        ],
        paginate=True,
        timeout=SETTINGS.CONTRIBS_TIMEOUT,
    )
    if contributions_resp:
        # reformat MPContribs data to work with Josua Vieten's original code
        data = [
            {
                "energy_analysis": [
                    {
                        "Chemical Energy": float(
                            contributions_resp["data"][i]["data"]["chemicalEnergy"]
                        ),
                        "p_red": float(contributions_resp["data"][i]["data"]["pRed"]),
                        "l_prod_kg_red": float(
                            contributions_resp["data"][i]["data"]["lProdKgRed"]
                        ),
                        "prodstr": contributions_resp["data"][i]["data"]["prodstr"],
                        "mol_mass_ox": float(
                            contributions_resp["data"][i]["data"]["molMassOx"]
                        ),
                        "mol_prod_mol_red": float(
                            contributions_resp["data"][i]["data"]["molProdMolRed"]
                        ),
                        "Sensible Energy": float(
                            contributions_resp["data"][i]["data"]["sensibleEnergy"]
                        ),
                        "p_ox": float(contributions_resp["data"][i]["data"]["pOx"]),
                        "T_red": float(contributions_resp["data"][i]["data"]["TRed"]),
                        "mass_redox": float(
                            contributions_resp["data"][i]["data"]["massRedox"]
                        ),
                        "delta_1": float(
                            contributions_resp["data"][i]["data"]["delta1"]
                        ),
                        "T_ox": float(contributions_resp["data"][i]["data"]["TOx"]),
                        "prodstr_alt": contributions_resp["data"][i]["data"][
                            "prodstrAlt"
                        ],
                        "g_prod_kg_red": float(
                            contributions_resp["data"][i]["data"]["gProdKgRed"]
                        ),
                        "unstable": (
                            True
                            if contributions_resp["data"][i]["data"]["unstable"]
                            == "True"
                            else False
                        ),
                        "compstr": contributions_resp["data"][i]["data"]["compstr"],
                        "delta_2": float(
                            contributions_resp["data"][i]["data"]["delta2"]
                        ),
                    }
                    for i in range(len(contributions_resp["data"]))
                ],
                "_id": contributions_resp["data"][0]["data"]["id"],
                "updated": contributions_resp["data"][0]["data"]["updated"],
            }
        ]
    else:
        data = None
    return data


#####################################
# Method for creating the isographs
####################################


def get_figure(figure_number, theo_data, compstr, constant=None, rng=None, delta=None):
    def get_isograph_data(
        theo_data, _EXP_DATA, compstr, plottype, constant, rng, delta
    ):
        try:
            pars = ID.init_isographs(theo_data, _EXP_DATA, compstr=compstr)[1]
            Iso_I = Iso(compstr, plottype, constant, rng)
            payload, x_val = Iso_I.prepare_limits()
            if plottype == "dH" or plottype == "dS":
                result = Iso_I.enthalpy_entropy(pars=pars, payload=payload, x_val=x_val)
            elif plottype == "ellingham":
                result = Iso_I.ellingham(
                    pars=pars, payload=payload, x_val=x_val, delta=delta
                )
            else:
                result = Iso_I.isographs(pars=pars, payload=payload, x_val=x_val)
        except ValueError:
            result = None
            warnings.warn("No material selected")
        return result

    def figure_data(isodat_input):
        data = data = [
            go.Scatter(
                x=isodat_input[0]["x"],
                y=isodat_input[0]["y"],
                mode="lines",
                name=isodat_input[0]["name"],
                line=isodat_input[0]["line"],
                showlegend=False,
            ),
            go.Scatter(
                x=isodat_input[1]["x"],
                y=isodat_input[1]["y"],
                mode="lines",
                name=isodat_input[1]["name"],
                line=isodat_input[1]["line"],
                showlegend=False,
            ),
            go.Scatter(
                x=isodat_input[2]["x"],
                y=isodat_input[2]["y"],
                mode="lines",
                name=isodat_input[2]["name"],
                line=isodat_input[2]["line"],
                showlegend=False,
            ),
        ]
        return data

    def format_plot(fig):
        """make a few aesthetic changes to all the plots"""
        fig.update_layout(margin={"t": 10}, font=dict(size=16))

    if figure_number == 0:
        isodat_0 = get_isograph_data(
            theo_data, _EXP_DATA, compstr, "isotherm", constant, rng, None
        )
        if not isodat_0:
            fig_0 = go.Figure(data=[])
        else:
            isodat_0[1]["line"].update({"dash": "dash"})
            fig_0 = go.Figure(data=figure_data(isodat_0))
        fig_0.update_xaxes(
            type="log",
            title="p<sub>O2</sub> (bar)",
            linecolor="rgb(0,0,0)",
            gridcolor="rgb(210,210,210)",
        )
        fig_0.update_yaxes(
            title="δ", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        format_plot(fig_0)

        if not fig_0["data"]:
            return get_no_data_message()
        return fig_0

    elif figure_number == 1:
        isodat_1 = get_isograph_data(
            theo_data, _EXP_DATA, compstr, "isobar", constant, rng, None
        )
        if not isodat_1:
            fig_1 = go.Figure(data=[])
        else:
            isodat_1[1]["line"].update({"dash": "dash"})
            fig_1 = go.Figure(data=figure_data(isodat_1))
        fig_1.update_xaxes(
            title="T (K)", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        fig_1.update_yaxes(
            title="δ", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        format_plot(fig_1)

        if not fig_1["data"]:
            return get_no_data_message()
        return fig_1

    elif figure_number == 2:
        isodat_2 = get_isograph_data(
            theo_data, _EXP_DATA, compstr, "isoredox", constant, rng, None
        )
        if not isodat_2:
            fig_2 = go.Figure(data=[])
        else:
            isodat_2[1]["line"].update({"dash": "dash"})
            fig_2 = go.Figure(data=figure_data(isodat_2))
        fig_2.update_xaxes(
            title="T (K)", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        fig_2.update_yaxes(
            type="log",
            title="p<sub>O2</sub> (bar)",
            linecolor="rgb(0,0,0)",
            gridcolor="rgb(210,210,210)",
        )
        format_plot(fig_2)

        if not fig_2["data"]:
            return get_no_data_message()
        return fig_2

    elif figure_number == 3:
        isodat_3 = get_isograph_data(
            theo_data, _EXP_DATA, compstr, "dH", constant, None, None
        )
        if not isodat_3:
            fig_3 = go.Figure(data=[])
            fig_3.update_yaxes(
                title="ΔH<sub>O</sub> (kJ/mol)",
                linecolor="rgb(0,0,0)",
                gridcolor="rgb(210,210,210)",
            )
        else:
            isodat_3[1]["line"].update({"dash": "dash"})
            fig_3 = go.Figure(data=figure_data(isodat_3))
            fig_3.update_yaxes(
                title="ΔH<sub>O</sub> (kJ/mol)",
                range=isodat_3[3],
                linecolor="rgb(0,0,0)",
                gridcolor="rgb(210,210,210)",
            )
        fig_3.update_xaxes(
            title="δ", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        format_plot(fig_3)

        if not fig_3["data"]:
            return get_no_data_message()
        return fig_3

    elif figure_number == 4:
        isodat_4 = get_isograph_data(
            theo_data, _EXP_DATA, compstr, "dS", constant, None, None
        )
        if not isodat_4:
            fig_4 = go.Figure(data=[])
            fig_4.update_yaxes(
                title="ΔS<sub>O</sub> (J/mol\u22C5K)",
                linecolor="rgb(0,0,0)",
                gridcolor="rgb(210,210,210)",
            )
        else:
            isodat_4[1]["line"].update({"dash": "dash"})
            fig_4 = go.Figure(data=figure_data(isodat_4))
            fig_4.update_yaxes(
                title="ΔS<sub>O</sub> (J/mol\u22C5K)",
                range=isodat_4[3],
                linecolor="rgb(0,0,0)",
                gridcolor="rgb(210,210,210)",
            )
        fig_4.update_xaxes(
            title="δ", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        format_plot(fig_4)

        if not fig_4["data"]:
            return get_no_data_message()
        return fig_4

    elif figure_number == 5:
        isodat_5 = get_isograph_data(
            theo_data,
            _EXP_DATA,
            compstr,
            "ellingham",
            constant,
            rng,
            delta,
        )
        if not isodat_5:
            fig_5 = go.Figure(data=[])
        else:
            isodat_5[1]["line"].update({"dash": "dash"})
            fig_5 = go.Figure(data=figure_data(isodat_5))
            fig_5.add_scatter(
                x=isodat_5[3]["x"],
                y=isodat_5[3]["y"],
                mode="lines",
                name=isodat_5[3]["name"],
                line=isodat_5[3]["line"],
                showlegend=False,
            )
        fig_5.update_xaxes(
            title="T (K)", linecolor="rgb(0,0,0)", gridcolor="rgb(210,210,210)"
        )
        fig_5.update_yaxes(
            title="ΔG<sub>O</sub> (kJ/mol)",
            linecolor="rgb(0,0,0)",
            gridcolor="rgb(210,210,210)",
        )
        format_plot(fig_5)

        if not fig_5["data"]:
            return get_no_data_message()
        return fig_5


def reformat_isograph_data(compstr):
    """for use in isographs callbacks to get the isographs data into the correct format for
    use in other methods"""
    # find the data for the row the user clicked on
    mpr = get_rester()
    isographs_contributions_resp = mpr.contribs.query_contributions(
        query={
            "project": "redox_thermo_csp",
            "data__theoretical__composition__exact": compstr,
        },
        fields=[
            "data.theoretical",
            "data.solution",
            "data.phases",
            "data.availability",
            "data.updated",
        ],
        timeout=SETTINGS.CONTRIBS_TIMEOUT,
    )
    if not isographs_contributions_resp["data"]:
        logger.error(f"Failed to load contribution for {compstr}")
        raise PreventUpdate

    requested_data = isographs_contributions_resp["data"][0]
    if not requested_data:
        logger.error(f"Failed to load contribution for {compstr}")
        raise PreventUpdate

    # get the contribs data back into the original json format that works with
    # all the functions in 'redox_views.py'
    theo_data = {
        "collection": [
            {
                "data": {
                    "tolerance_factor": requested_data["data"]["theoretical"][
                        "tolerance"
                    ]["value"],
                    "solid_solution": requested_data["data"]["solution"],
                    "oxidized_phase": {
                        "crystal-structure": [],
                        "composition": requested_data["data"]["phases"]["oxidized"][
                            "composition"
                        ],
                    },
                    "reduced_phase": {
                        "closest_MP": requested_data["data"]["phases"]["reduced"][
                            "mpid"
                        ],
                        "composition": requested_data["data"]["phases"]["reduced"][
                            "composition"
                        ],
                    },
                },
                "_id": requested_data["data"]["phases"]["oxidized"]["mpid"],
                "pars": {
                    "theo_compstr": requested_data["data"]["theoretical"][
                        "composition"
                    ],
                    "act_mat": [
                        [],
                        requested_data["data"]["theoretical"]["active"]["value"],
                    ],
                    "elastic": {
                        "Elastic tensors available": requested_data["data"][
                            "theoretical"
                        ]["elastic"]["tensors"]
                        != "False",
                        "Debye temp brownmillerite": requested_data["data"][
                            "theoretical"
                        ]["elastic"]["debye"]["brownmillerite"]["value"],
                        "Debye temp perovskite": requested_data["data"]["theoretical"][
                            "elastic"
                        ]["debye"]["perovskite"]["value"],
                    },
                    "data_availability": requested_data["data"]["availability"],
                    "last_updated": requested_data["data"]["updated"],
                    "dh_min": requested_data["data"]["theoretical"]["ΔH"]["min"][
                        "value"
                    ],
                    "dh_max": requested_data["data"]["theoretical"]["ΔH"]["max"][
                        "value"
                    ],
                },
            }
        ]
    }
    return theo_data


def create_header(header_text):
    return html.Div(
        html.Div(
            html.P(
                children=header_text,
                style={
                    "text-align": "center !important",
                    "margin-bottom": ".5rem !important",
                    "unicode-bidi": "isolate",
                    "color": "#4a4a4a",
                    "font-size": "18px",
                    "font-weight": "400",
                    "line-height": "1.5",
                },
            ),
            style={
                "display": "flex",
                "justify-content": "center",
                "align-items": "center",
                "position": "relative",
                "width": "auto",
                "unicode-bidi": "isolate",
                "color": "#4a4a4a",
                "font-size": "1em",
                "font-weight": "400",
                "line-height": "1.5",
            },
        ),
        style={
            "display": "flex",
            "justify-content": "center",
            "align-items": "center",
            "background": "#ebebeb",
            "border-bottom": "1px solid #dbdbdb",
            "padding-bottom": "1.5rem",
            "padding-top": "1.5rem",
            "margin": "-1.5rem -1.5rem 1.5rem -1.5rem",
            "unicode-bidi": "isolate",
            "color": "#4a4a4a",
            "font-size": "1em",
            "font-weight": "400",
            "line-height": "1.5",
        },
    )


def get_no_data_message():
    fig = go.Figure()
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": "No data found for the selected conditions, please try another set of conditions",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 28},
            }
        ],
    )
    return fig


if __name__ == "__main__":
    app = dash.Dash(__name__, assets_folder="./assets")
    app.layout = RedoxThermoCSPAIO(aio="test")
    ctc.register_crystal_toolkit(app=app, layout=app.layout, cache=None)
    app.run_server(debug=True, port=8055)
