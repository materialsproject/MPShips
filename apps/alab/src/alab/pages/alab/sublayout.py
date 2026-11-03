import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Output, Input

from .const import (
    ELEMENT_COLORS,
    CLUSTER_COLORS
)
from .plot import (
    create_xrd_chart,
    create_temperature_chart,
    create_element_pie_chart
)


def create_xrd_analysis_section(raw_data):
    """Create XRD phase analysis section if DARA results are available"""
    metadata = raw_data.get("metadata", {})
    xrd_analysis = metadata.get("xrd_analysis")

    if not xrd_analysis:
        return None

    # Handle failed analyses
    if not xrd_analysis.get("success", True):  # Default True for backwards compat
        error = xrd_analysis.get("error", "Unknown error")
        error_type = xrd_analysis.get("error_type", "analysis_error")

        # Map error types to user-friendly messages
        error_messages = {
            "no_peaks_detected": "No crystalline peaks detected - sample may be amorphous",
            "cif_download": "Unable to download reference structures from COD database",
            "timeout": "Analysis timed out - please retry",
            "no_chemical_system": "Chemical system not defined for this experiment",
            "dara_internal_error": "Internal DARA processing error",
            "analysis_error": "Analysis failed",
        }
        friendly_error = error_messages.get(error_type, error)

        return dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H2("🔬 XRD Phase Analysis (DARA)", className="h4 mb-3"),
                        dbc.Alert(
                            [
                                html.I(className="bi bi-exclamation-triangle me-2"),
                                html.Strong("Analysis Failed: "),
                                friendly_error,
                                html.Br(),
                                html.Small(f"Error: {error}", className="text-muted"),
                            ],
                            color="warning",
                            className="mb-0",
                        ),
                    ]
                )
            ],
            className="shadow-sm mb-4",
        )

    phases = xrd_analysis.get("phases", [])
    rwp = xrd_analysis.get("rwp", 0)
    rp = xrd_analysis.get("rp", 0)

    # Determine quality badge color based on Rwp
    if rwp < 5:
        quality_color = "success"
        quality_text = "Excellent"
    elif rwp < 35:
        quality_color = "warning"
        quality_text = "Marginal - Consult Expert"
    else:
        quality_color = "danger"
        quality_text = "Poor - Consult Expert"

    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("🔬 XRD Phase Auto-Analysis by DARA", className="h4 mb-4"),
                    # Quality metrics row
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    html.P(
                                                        "Rwp",
                                                        className="small text-muted mb-1",
                                                    ),
                                                    html.H3(
                                                        f"{rwp:.2f}%",
                                                        className=f"h4 mb-0 text-{quality_color}",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="bg-light border-0",
                                    )
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    html.P(
                                                        "Rp",
                                                        className="small text-muted mb-1",
                                                    ),
                                                    html.H3(
                                                        f"{rp:.2f}%",
                                                        className="h4 mb-0",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="bg-light border-0",
                                    )
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    html.P(
                                                        "Phases Found",
                                                        className="small text-muted mb-1",
                                                    ),
                                                    html.H3(
                                                        str(len(phases)),
                                                        className="h4 mb-0 text-primary",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="bg-light border-0",
                                    )
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    html.P(
                                                        "Fit Quality",
                                                        className="small text-muted mb-1",
                                                    ),
                                                    dbc.Badge(
                                                        quality_text,
                                                        color=quality_color,
                                                        className="h5",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="bg-light border-0",
                                    )
                                ],
                                md=3,
                            ),
                        ],
                        className="mb-4",
                    ),
                    # Phases table
                    html.H3("Identified Phases", className="h6 mb-3"),
                    dbc.Table(
                        [
                            html.Thead(
                                [
                                    html.Tr(
                                        [
                                            html.Th("Phase"),
                                            html.Th("Spacegroup"),
                                            html.Th("Weight %", className="text-end"),
                                            html.Th("R-phase %", className="text-end"),
                                        ]
                                    )
                                ]
                            ),
                            html.Tbody(
                                [
                                    html.Tr(
                                        [
                                            html.Td(
                                                phase.get("phase_name", "Unknown")[:40]
                                                + (
                                                    "..."
                                                    if len(phase.get("phase_name", ""))
                                                    > 40
                                                    else ""
                                                ),
                                                className="font-monospace small",
                                            ),
                                            html.Td(phase.get("spacegroup", "N/A")),
                                            html.Td(
                                                # For single phase, show 100%; otherwise use weight_fraction
                                                f"{100.0 if len(phases) == 1 else (phase.get('weight_fraction') or 0) * 100:.1f}%",
                                                className="text-end",
                                            ),
                                            html.Td(
                                                f"{phase.get('r_phase', 0):.2f}%",
                                                className="text-end",
                                            ),
                                        ]
                                    )
                                    for phase in phases
                                ]
                            ),
                        ],
                        bordered=True,
                        hover=True,
                        responsive=True,
                        size="sm",
                    ),
                    # Info alert
                    dbc.Alert(
                        [
                            html.Strong("About Phase Analysis: "),
                            "Rietveld refinement identifies crystalline phases and their proportions. ",
                            "Rwp (weighted profile R-factor) measures fit quality - lower is better. ",
                            "Analysis performed automatically with DARA/BGMN. WARNING! No results here are suggestions only.",
                        ],
                        color="info",
                        className="mt-3 small",
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )


def get_sample_id_card(raw_data):
    # Sample Identification Information Card
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2(
                        "Sample Identification Information", className="h4 mb-4"
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.P(
                                        "Sample Name",
                                        className="small text-muted mb-1",
                                    ),
                                    # We use a dash or "TBD" to show the spot is reserved
                                    html.P(
                                        raw_data.get("identifier", ""),
                                        className="h5 fw-semibold text-muted",
                                    ),
                                ],
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.P(
                                        "Formula",
                                        className="small text-muted mb-1",
                                    ),
                                    html.P(
                                        raw_data.get("formula", ""),
                                        className="h5 font-monospace text-break",
                                    ),
                                ],
                                md=6,
                            ),
                        ]
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )


def get_element_composition_card(raw_data):
    if not raw_data.get("composition", {}):
        return dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H2("No Avaliable Composition", className="h4 mb-4"),
                    ]
                )
            ]
        )

    # Target Element Composition
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("Target Element Composition", className="h4 mb-4"),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    # "Elemental Distribution" title removed from here
                                    dcc.Graph(
                                        figure=create_element_pie_chart(
                                            raw_data.get("composition", {})
                                        ),
                                        config={"displayModeBar": False},
                                    )
                                ],
                                lg=6,
                            ),
                            dbc.Col(
                                [
                                    html.H3(
                                        "Atomic Composition (%)",
                                        className="h6 mb-3",
                                    ),
                                    html.Div(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            dbc.Row(
                                                                [
                                                                    dbc.Col(
                                                                        [
                                                                            html.Div(
                                                                                [
                                                                                    html.Div(
                                                                                        style={
                                                                                            "width": "16px",
                                                                                            "height": "16px",
                                                                                            "backgroundColor": ELEMENT_COLORS[
                                                                                                idx
                                                                                                % len(
                                                                                                    ELEMENT_COLORS
                                                                                                )
                                                                                            ],
                                                                                            "borderRadius": "4px",
                                                                                            "display": "inline-block",
                                                                                            "marginRight": "12px",
                                                                                        }
                                                                                    ),
                                                                                    html.Span(
                                                                                        el,
                                                                                        className="fw-semibold h5 mb-0",
                                                                                    ),
                                                                                ],
                                                                                style={
                                                                                    "display": "flex",
                                                                                    "alignItems": "center",
                                                                                },
                                                                            )
                                                                        ],
                                                                        width="auto",
                                                                    ),
                                                                    # Consolidated percentage view - set to text-dark (black)
                                                                    dbc.Col(
                                                                        [
                                                                            html.H5(
                                                                                f"{raw_data["composition"]['percentages'][el]:.2f}%",
                                                                                className="mb-0 text-end font-monospace text-dark",
                                                                            )
                                                                        ]
                                                                    ),
                                                                ],
                                                                className="align-items-center",
                                                            )
                                                        ],
                                                        className="py-2",
                                                    )
                                                ],
                                                className="mb-2",
                                            )
                                            for idx, (el, amt) in enumerate(
                                                sorted(
                                                    raw_data["composition"]["elements"].items(),
                                                    key=lambda x: x[1],
                                                    reverse=True,
                                                )
                                            )
                                        ],
                                        style={
                                            "maxHeight": "350px",
                                            "overflowY": "auto",
                                        },
                                    ),
                                ],
                                lg=6,
                            ),
                        ]
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )


def get_precursor_card(raw_data):
    # Precursors (Precursors used in Experiment)
    if not raw_data.get("_sample_preparation", {}):
        return dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H2("No Avaliable Precursors Profile", className="h4 mb-4"),
                    ]
                )
            ]
        )

    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("Precursors", className="h4 mb-4"),
                    dbc.Table(
                        [
                            html.Thead(
                                [
                                    html.Tr(
                                        [
                                            html.Th("Powder"),
                                            html.Th(
                                                "Target (g)",
                                                className="text-end",
                                            ),
                                            html.Th(
                                                "Actual (g)",
                                                className="text-end",
                                            ),
                                            html.Th(
                                                "Accuracy", className="text-end"
                                            ),
                                        ]
                                    )
                                ]
                            ),
                            html.Tbody(
                                [
                                    html.Tr(
                                        [
                                            html.Td(
                                                raw_data.get("_sample_preparation", {}).get("powderName", {}).get(idx, "-"),
                                                className="fw-semibold",
                                            ),
                                            html.Td(
                                                f"{v:.5f}" if (v := raw_data.get("_sample_preparation", {}).get("targetMass", {}).get(idx)) is not None else "-",
                                                className="font-monospace text-end",
                                            ),
                                            html.Td(
                                                f"{v:.5f}" if (v := raw_data.get("_sample_preparation", {}).get("actualMass", {}).get(idx)) is not None else "-",
                                                className="font-monospace text-end",
                                            ),
                                            html.Td(
                                                f"{v:.2f}%" if (v := raw_data.get("_sample_preparation", {}).get("doseAccuracyPercent", {}).get(idx)) is not None else "-",
                                                className=f"fw-semibold text-end",
                                            ),
                                        ]
                                    )
                                    for idx in range(len(raw_data.get("_sample_preparation", {}).get("powderName", {})))
                                ]
                            ),
                        ],
                        bordered=True,
                        hover=True,
                        responsive=True,
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )


def get_heating_card(raw_data, temperature_chart):
    if not raw_data.get("_heating", {}) or not temperature_chart:
        return dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H2("No Avaliable Heating Profile", className="h4 mb-4"),
                    ]
                )
            ]
        )
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("🔥 Heating Profile", className="h4 mb-4"),
                    html.Div(
                        [
                            # ROW 1: Target Temp, Duration, Max Temp
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Target Temp",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.H3(
                                                                f"{raw_data.get("_heating", {}).get('temperatureTarget', [0])[0]}°C",
                                                                className="h4 mb-0 text-danger",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Duration",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.H3(
                                                                f"{raw_data.get("_heating", {}).get('dwellTime', 0)[0] / 60:.1f} hrs",
                                                                className="h4 mb-0 text-primary",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Max Temp",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.H3(
                                                                f"{float(max(raw_data.get("_heating", {}).get('temperature', []).values())):.0f}°C",
                                                                className="h4 mb-0 text-warning",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # ROW 2: Atmosphere, Furnace ID, Data Points
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Atmosphere",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            # Placeholder set to dash
                                                            html.H3(
                                                                raw_data.get("_heating", {}).get('atmosphere', ["-"])[0],
                                                                className="h4 mb-0 text-info",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Furnace ID",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            # Placeholder set to dash
                                                            html.H3(
                                                                raw_data.get("_heating", {}).get('furnaceName', ["-"])[0],
                                                                className="h4 mb-0 text-dark",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Data Points",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            # This still pulls dynamically from your data
                                                            html.H3(
                                                                str(
                                                                    len(raw_data.get("_heating", {}).get('temperature', []))
                                                                ),
                                                                className="h4 mb-0 text-success",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=4,
                                    ),
                                ],
                                className="mb-4",
                            ),
                            # The Graph section remains at the bottom
                            dcc.Graph(
                                figure=temperature_chart,
                                config={"displayModeBar": False},
                            )
                            if temperature_chart
                            else html.P(
                                "Temperature data not available",
                                className="text-muted",
                            ),
                        ]
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )

def get_xrd_card(raw_data, xrd_chart):
    # XRD Characterization (always show if data available)
    if not raw_data.get("_characterization", {}) or not xrd_chart:
        return dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H2("No Avaliable XRD Profile", className="h4 mb-4"),
                    ]
                )
            ]
        )
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("📐 XRD Characterization", className="h4 mb-4"),
                    html.Div(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "XRD source",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.P(
                                                                f"{raw_data.get("_characterization", {}).get("xrdSource", ["-"])[0]}",
                                                                className="h6 mb-0",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=3,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Data Points",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.P(
                                                                str(
                                                                    len(raw_data.get("_characterization", {}).get("xrdCounts", {}))
                                                                ),
                                                                className="h6 mb-0",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=3,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "2θ Range",
                                                                className="small text-muted mb-1",
                                                            ),
                                                            html.P(
                                                                f"{min(raw_data.get("_characterization", {}).get("xrdTwoTheta", {}).values()):.1f}° - {max(raw_data.get("_characterization", {}).get("xrdTwoTheta", {}).values()):.1f}°",
                                                                className="h6 mb-0",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="bg-light border-0",
                                            )
                                        ],
                                        md=3,
                                    ),
                                ],
                                className="mb-4",
                            ),
                            dcc.Graph(
                                figure=xrd_chart,
                                config={"displayModeBar": False},
                            )
                            if xrd_chart
                            else None,
                            dbc.Alert(
                                [
                                    html.Strong("About XRD: "),
                                    "X-ray diffraction pattern shows the crystalline structure of the material. ",
                                    "Peaks correspond to specific crystal planes and can be used to identify phases ",
                                    "and calculate lattice parameters.",
                                ],
                                color="info",
                                className="mt-3",
                            ),
                        ]
                    )
                    if raw_data.get("_characterization", {})
                    else html.P(
                        "XRD data not available for this sample",
                        className="text-muted text-center py-4",
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )

def get_references_card():
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2("🛠 Methods", className="h4 mb-4"),
                    html.P(
                        [
                            "Samples were synthesized in A-Lab. All methods below were executed automatically (without human intervention).",
                            html.Br(),
                            html.Br(),
                            html.Strong("Abbreviated Process:"),
                        ],
                        className="mb-2",
                    ),
                    # Standardized text size for the methods list
                    html.Ol(
                        [
                            html.Li(
                                "Powders were weighed into a plastic vial."
                            ),
                            html.Li(
                                "Ethanol added; wet spin mixing at 2000rpm."
                            ),
                            html.Li(
                                "Slurry pipetted to crucible and dried at 80°C."
                            ),
                            html.Li("Furnace heating at specified profile."),
                            html.Li("Powdered sample collection."),
                            html.Li(
                                "PXRD preparation. Optional additional SEM/EDS characterization."
                            ),
                        ],
                        className="mb-4",
                    ),  # Removed 'small' and 'text-muted'
                    html.Hr(),
                    html.Div(
                        [
                            html.H3(
                                "📚 Reference Papers & Citations",
                                className="h6 fw-bold mb-3",
                            ),
                            # Paper 1: A-Lab Nature 2023
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Badge(
                                            "A-Lab",
                                            color="primary",
                                            className="w-100",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Span(
                                                "Nathan J. Szymanski, Bernardus Rendy, Yuxing Fei, Rishi E. Kumar, Tanjin He, David Milsted, Matthew J. McDermott, Max Gallant, Ekin Dogus Cubuk, Amil Merchant, Haegyeom Kim, Anubhav Jain, Christopher J. Bartel, Kristin Persson, Yan Zeng, Gerbrand Ceder, ",
                                                className="small",
                                            ),
                                            html.Span(
                                                "'An autonomous laboratory for the accelerated synthesis of inorganic materials', ",
                                                className="fst-italic small",
                                            ),
                                            html.Span(
                                                "Nature (2023). ",
                                                className="small",
                                            ),
                                            html.A(
                                                "View ↗",
                                                href="https://doi.org/10.1038/s41586-023-06734-w",
                                                target="_blank",
                                                className="ms-2 small text-decoration-none",
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3 align-items-center",
                            ),
                            # Paper 2: AlabOS 2024
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Badge(
                                            "AlabOS / Orchestration",
                                            color="info",
                                            className="w-100",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Span(
                                                "Yuxing Fei, Bernardus Rendy, Rishi Kumar, Olympia Dartsi, Hrushikesh P. Sahasrabuddhe, Matthew J. McDermott, Zheren Wang, Nathan J. Szymanski, Lauren N. Walters, David Milsted, Yan Zeng, Anubhav Jain, Gerbrand Ceder, ",
                                                className="small",
                                            ),
                                            html.Span(
                                                "'AlabOS: a Python-based reconfigurable workflow management framework for autonomous laboratories', ",
                                                className="fst-italic small",
                                            ),
                                            html.Span(
                                                "Digital Discovery (2024). ",
                                                className="small",
                                            ),
                                            html.A(
                                                "View ↗",
                                                href="https://doi.org/10.1039/D4DD00129J",
                                                target="_blank",
                                                className="ms-2 small text-decoration-none",
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3 align-items-center",
                            ),
                            # Paper 3: EMSBot 2025
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Badge(
                                            "EMSBot / SEM-TEM Sample Prep",
                                            color="dark",
                                            className="w-100",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Span(
                                                "Yuxing Fei, Rishi Kumar, Matthew J. McDermott, Bernardus Rendy, Zheren Wang, Nathan J. Szymanski, Olympia Dartsi, Yan Zeng, Anubhav Jain, Gerbrand Ceder, ",
                                                className="small",
                                            ),
                                            html.Span(
                                                "'EMSBot: an autonomous robotic system for SEM and TEM sample preparation of inorganic powders', ",
                                                className="fst-italic small",
                                            ),
                                            html.Span(
                                                "Digital Discovery (2025). ",
                                                className="small",
                                            ),
                                            html.A(
                                                "View ↗",
                                                href="https://pubs.rsc.org/en/content/articlelanding/2025/dd/d5dd00116a",
                                                target="_blank",
                                                className="ms-2 small text-decoration-none",
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3 align-items-center",
                            ),
                            # Paper 4: Dara 2026
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Badge(
                                            "Dara / Automated XRD Analysis",
                                            color="success",
                                            className="w-100",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Span(
                                                "Yuxing Fei, Matthew J. McDermott, Christopher L. Rom, Shilong Wang, Gerbrand Ceder, ",
                                                className="small",
                                            ),
                                            html.Span(
                                                "'Dara: Automated Multiple-Hypothesis Phase Identification and Refinement from Powder X-ray Diffraction', ",
                                                className="fst-italic small",
                                            ),
                                            html.Span(
                                                "Chemistry of Materials (2026). ",
                                                className="small",
                                            ),
                                            html.A(
                                                "View ↗",
                                                href="https://doi.org/10.1021/acs.chemmater.5c02820",
                                                target="_blank",
                                                className="ms-2 small text-decoration-none",
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3 align-items-center",
                            ),
                            # Paper 5: SEM-EDS Analysis 2026
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Badge(
                                            "Auto SEM-EDS Analysis",
                                            color="secondary",
                                            className="w-100",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Span(
                                                "Andrea Giunto, Yuxing Fei, Pragnay Nevatia, Bernardus Rendy, Nathan Szymanski, Gerbrand Ceder, ",
                                                className="small",
                                            ),
                                            html.Span(
                                                "'Accurate SEM‑EDS Quantification, Automation, and Machine Learning Enable High‑Throughput Compositional Characterization of Powders', ",
                                                className="fst-italic small",
                                            ),
                                            html.Span(
                                                "Research Square (2026). ",
                                                className="small",
                                            ),
                                            html.A(
                                                "View ↗",
                                                href="https://www.researchsquare.com/article/rs-7837297/v2",
                                                target="_blank",
                                                className="ms-2 small text-decoration-none",
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-2 align-items-center",
                            ),
                        ],
                        className="mt-4 p-3 bg-light rounded shadow-sm",
                    ),
                ]
            )
        ],
        className="shadow-sm mb-4",
    )


def generate_experiment_content(raw_data):
    """Generate dashboard content for an experiment"""
    if not raw_data:
        return html.Div(
            [
                dbc.Alert(
                    "No data available for this experiment",
                    color="warning",
                    className="mt-4",
                )
            ]
        )

    xrd_chart = create_xrd_chart(raw_data.get("_characterization", {}))
    temperature_chart = create_temperature_chart(raw_data.get("_heating", {}))

    return html.Div(
        [
            get_sample_id_card(raw_data),
            get_element_composition_card(raw_data),
            get_precursor_card(raw_data),
            get_heating_card(raw_data, temperature_chart),
            get_xrd_card(raw_data, xrd_chart),
            create_xrd_analysis_section(raw_data), # XRD Phase Analysis (DARA) - only shown if analysis results available. Do such thing now
            get_references_card()
        ]
    )
