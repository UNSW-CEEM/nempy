import math

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

pio.kaleido.scope.mathjax = None

fig = make_subplots(rows=2, cols=1, row_heights=[0.3, 0.7], vertical_spacing=0.15,
                    shared_xaxes=False)

colors = px.colors.qualitative.Plotly

results = pd.read_csv("bdu_prices_1000.csv")

results['price_weighted'] = results['price'] * results['demand']
results['rop_weighted'] = results['ROP'] * results['demand']

results = results.groupby('time', as_index=False).agg(
    {
        'price_weighted': 'sum',
        'rop_weighted': 'sum',
        'demand': 'sum'
     }
)

results['price_weighted'] = results['price_weighted'] / results['demand']
results['rop_weighted'] = results['rop_weighted'] / results['demand']

results = results.sort_values('rop_weighted', ascending=False)
results = results.reset_index(drop=True)
results['position'] = results.index

marker = dict(size=4, color=colors[0], symbol='circle-open')
marker_log_axis = dict(size=4, color=colors[0], symbol='circle-open')

fig.add_trace(go.Scatter(x=results['position'],
                         y=results['rop_weighted'],
                         name="Historical", mode='markers',
                         marker=marker_log_axis,
                         ), row=1, col=1
              )

fig.add_trace(go.Scatter(x=results['position'],
                         y=results['rop_weighted'],
                         name="Historical", mode='markers',
                         marker=marker,
                         showlegend=False,
                         opacity=1.0
                         ), row=2, col=1
              )

marker_log_axis = dict(size=1.5, color=colors[1])
marker = dict(size=1.5, color=colors[1])

fig.add_trace(go.Scatter(x=results['position'],
                         y=results['price_weighted'],
                         name="Nempy", mode='markers',
                         marker=marker_log_axis,
                         ), row=1, col=1
              )

fig.add_trace(go.Scatter(x=results['position'],
                         y=results['price_weighted'],
                         name="Nempy", mode='markers',
                         marker=marker,
                         showlegend=False,
                         opacity=1.0
                         ), row=2, col=1
              )

fig['layout']['xaxis2']['title'] = 'Price order descending'
fig['layout']['yaxis2']['title'] = 'Price ($/MW/h)'
fig['layout']['yaxis1']['title'] = ''
fig['layout']['yaxis1']['title'] = ''


fig.update_layout(
    yaxis1_type="log",
    yaxis1_range=[math.log10(299.0), math.log10(17500.0)],
    yaxis1_tickmode='array',
    yaxis1_tickvals=[300.0, 500.0, 1000, 17500.0],
    xaxis1_range=[0, 100],
    xaxis1_tickvals=[0, 50, 100],
    xaxis1_showticklabels=True,
    yaxis2_range=[-100.0, 300.0],
    xaxis2_range=[0, 1000],
    xaxis1={"domain": [0.0, 0.3]}
)

_template = dict(
    layout=go.Layout(
        scene=dict(
            xaxis=dict(
                linecolor='black'
            )
        ),
        xaxis=dict(
            ticks='outside',
            showline=True,
            automargin=True
        ),
        yaxis=dict(
            ticks='outside',
            showline=True,
            automargin=True
        ),
        legend={'itemsizing': 'constant'}
    )
)

_template = dict(font=dict(size=12),
              margin=dict(t=20),
              legend=dict(font=dict(size=12)),
              template=_template)

fig.update_layout(**_template)

fig.write_image("bdu_benchmarking.pdf")
