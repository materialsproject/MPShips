import plotly.graph_objects as go

from .const import (
    ELEMENT_COLORS,
    POWDER_COLORS,
    ELEMENT_MAP,
    CLUSTER_COLORS
)


def create_element_pie_chart(data):
    """Pie chart with full element names on hover and massive legend"""
    elements = data["elements"]

    sorted_elements = sorted(elements.items(), key=lambda x: x[1], reverse=True)
    symbols = [el for el, _ in sorted_elements]
    values = [amt for _, amt in sorted_elements]

    # Generate the list of full names based on the symbols
    full_names = [ELEMENT_MAP.get(s, s) for s in symbols]

    colors = ELEMENT_COLORS[: len(symbols)]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=symbols,
                values=values,
                # 'customdata' stores extra info we want to use in the hover box
                customdata=full_names,
                marker=dict(colors=colors),
                # Use %{customdata} to pull the full name into the hover label
                hovertemplate="<b>%{customdata}</b><br>%{percent}<extra></extra>",
                textfont=dict(size=20),
                texttemplate="%{label}<br>%{percent}",
                domain={"x": [0, 0.7]},
            )
        ]
    )

    fig.update_layout(
        height=550,
        margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            font=dict(size=20),
            title=None,
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=0.75,
            traceorder="normal",
        ),
    )

    return fig


def create_powder_pie_chart(data):
    """Create pie chart for powder composition"""
    precursors = data["precursors"]
    labels = [p["name"][:15] for p in precursors]
    values = [p["actual_mass"] for p in precursors]
    colors = POWDER_COLORS[: len(labels)]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                marker=dict(colors=colors),
                hovertemplate="<b>%{label}</b><br>%{value:.5f}g<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(t=30, b=0, l=0, r=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    return fig


def create_temperature_chart(data):
    """Create temperature profile chart with enlarged fonts"""
    if not data:
        return None

    time_mins = [float(t) / 60 for t in data.get("time").values()]
    temperatures = [float(t) for t in data.get("temperature").values()]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=time_mins,
            y=temperatures,
            mode="lines",
            line=dict(color="#f97316", width=3),
            name="Temperature",
        )
    )

    fig.update_layout(
        xaxis_title="Time (mins)",
        yaxis_title="Temperature (°C)",
        height=400,
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        # Matching margins to the XRD chart (l=70, b=60)
        margin=dict(t=30, b=60, l=70, r=30),
        xaxis=dict(
            title=dict(font=dict(size=20, color="black")),
            tickfont=dict(size=16, color="black"),
        ),
        yaxis=dict(
            title=dict(font=dict(size=20, color="black")),
            tickfont=dict(size=16, color="black"),
        ),
    )

    return fig


def create_xrd_chart(data):
    """Create XRD pattern chart with enlarged fonts"""
    if not data:
        return None

    two_theta = [float(x) for x in data.get("xrdTwoTheta").values()]
    counts = [float(c) for c in data.get("xrdCounts").values()]
    max_counts = max(counts)
    normalized_counts = [(c / max_counts) * 100 for c in counts]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=two_theta,
            y=normalized_counts,
            mode="lines",
            line=dict(color="#8b5cf6", width=2),
            name="XRD Pattern",
        )
    )

    fig.update_layout(
        xaxis_title="2θ (degrees)",
        yaxis_title="Intensity (%)",
        height=400,
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        # Increased bottom (b) and left (l) margins to accommodate larger fonts
        margin=dict(t=30, b=60, l=70, r=30),
        xaxis=dict(
            title=dict(font=dict(size=20, color="black")),
            tickfont=dict(size=16, color="black"),
        ),
        yaxis=dict(
            title=dict(font=dict(size=20, color="black")),
            tickfont=dict(size=16, color="black"),
        ),
    )

    return fig



def create_cluster_comparison_chart(sem_data, data):
    """Create bar chart comparing clusters to target composition"""
    if not sem_data:
        return None

    elements = ["Na", "Y", "Hf", "Zr", "Nb", "In", "Sn", "Si", "P", "O"]
    target_percentages = data["target_composition"]["percentages"]

    fig = go.Figure()

    # Add target composition
    target_values = [target_percentages.get(el, 0) for el in elements]
    fig.add_trace(
        go.Bar(
            name="Target",
            x=elements,
            y=target_values,
            marker_color="rgba(59, 130, 246, 0.7)",
        )
    )

    # Add cluster compositions
    for i, cluster in enumerate(sem_data["clusters"]):
        cluster_values = [cluster["composition"].get(el, 0) for el in elements]
        fig.add_trace(
            go.Bar(
                name=f'Cluster {cluster["id"]}',
                x=elements,
                y=cluster_values,
                marker_color=CLUSTER_COLORS[i % len(CLUSTER_COLORS)],
            )
        )

    fig.update_layout(
        barmode="group",
        xaxis_title="Element",
        yaxis_title="Atomic %",
        height=500,
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        margin=dict(t=30, b=50, l=50, r=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_element_distribution_chart(sem_data):
    """Create bar chart showing element distribution across clusters"""
    if not sem_data or not sem_data.get("clusters"):
        return None

    # Get all elements from all clusters
    all_elements = set()
    for cluster in sem_data["clusters"]:
        all_elements.update(cluster["composition"].keys())

    elements = sorted(all_elements)

    fig = go.Figure()

    for i, cluster in enumerate(sem_data["clusters"]):
        values = [cluster["composition"].get(el, 0) for el in elements]
        fig.add_trace(
            go.Bar(
                name=f'Cluster {cluster["id"]}',
                x=elements,
                y=values,
                marker_color=CLUSTER_COLORS[i % len(CLUSTER_COLORS)],
            )
        )

    fig.update_layout(
        barmode="group",
        xaxis_title="Element",
        yaxis_title="Atomic %",
        height=400,
        title="Element Distribution by Cluster",
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        margin=dict(t=50, b=50, l=50, r=30),
    )

    return fig


def create_composition_heatmap(sem_data):
    """Create heatmap of composition across clusters"""
    if not sem_data or not sem_data.get("clusters"):
        return None

    # Get all elements
    all_elements = set()
    for cluster in sem_data["clusters"]:
        all_elements.update(cluster["composition"].keys())

    elements = sorted(all_elements)

    # Build matrix
    matrix = []
    cluster_labels = []
    for cluster in sem_data["clusters"]:
        cluster_labels.append(f"Cluster {cluster['id']}")
        row = [cluster["composition"].get(el, 0) for el in elements]
        matrix.append(row)

    fig = go.Figure(
        data=go.Heatmap(
            z=matrix,
            x=elements,
            y=cluster_labels,
            colorscale="Viridis",
            text=[[f"{val:.1f}%" for val in row] for row in matrix],
            texttemplate="%{text}",
            textfont={"size": 10},
            colorbar=dict(title="Atomic %"),
        )
    )

    fig.update_layout(
        title="Composition Heatmap",
        xaxis_title="Element",
        yaxis_title="Cluster",
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        margin=dict(t=50, b=50, l=100, r=30),
    )

    return fig


def create_target_vs_actual_chart(sem_data, sample_data):
    """Create chart comparing target vs actual composition"""
    if not sem_data or not sem_data.get("target_comparison"):
        return None

    comparisons = sem_data["target_comparison"]
    elements = [c["Element"] for c in comparisons]
    target = [c["Target_at%"] for c in comparisons]
    actual = [c["Cluster_0_at%"] for c in comparisons]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name="Target", x=elements, y=target, marker_color="rgba(59, 130, 246, 0.7)"
        )
    )

    fig.add_trace(
        go.Bar(
            name="Measured (Cluster 0)",
            x=elements,
            y=actual,
            marker_color="rgba(239, 68, 68, 0.7)",
        )
    )

    fig.update_layout(
        barmode="group",
        title="Target vs Measured Composition (Cluster 0)",
        xaxis_title="Element",
        yaxis_title="Atomic %",
        height=400,
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        margin=dict(t=50, b=50, l=50, r=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_variability_chart(sem_data):
    """Create chart showing composition variability within clusters"""
    if not sem_data or not sem_data.get("clusters"):
        return None

    # Calculate standard deviations (simplified - would need raw spectra data for true std dev)
    # For now, show the number of points per cluster as a proxy
    cluster_ids = [f"Cluster {c['id']}" for c in sem_data["clusters"]]
    n_points = [c["n_points"] for c in sem_data["clusters"]]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=cluster_ids,
            y=n_points,
            marker_color=[
                CLUSTER_COLORS[i % len(CLUSTER_COLORS)] for i in range(len(cluster_ids))
            ],
            text=n_points,
            textposition="auto",
        )
    )

    fig.update_layout(
        title="Number of Spectra per Cluster",
        xaxis_title="Cluster",
        yaxis_title="Number of Spectra",
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(250,250,250,1)",
        margin=dict(t=50, b=50, l=50, r=30),
        showlegend=False,
    )

    return fig
