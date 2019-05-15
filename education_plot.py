import plotly
import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import pandas as pd

plotly.tools.set_credentials_file(username='', api_key='')

file_name = 'education_attainment.csv'
df = pd.read_csv(file_name)
fips = df['FIPS'].tolist()
lvl0 = df['2013-0'].tolist()
lvl1 = df['2013-1'].tolist()
lvl2 = df['2013-2'].tolist()
lvl3 = df['2013-3'].tolist()

colorscale = ["#f7fbff","#ebf3fb","#deebf7","#d2e3f3","#c6dbef","#b3d2e9","#9ecae1",
              "#85bcdb","#6baed6","#57a0ce","#4292c6","#3082be","#2171b5","#1361a9",
              "#08519c","#0b4083","#08306b"]

lvls = [lvl0, lvl1, lvl2, lvl3]
for i in range(len(lvls)):
	lvl = lvls[i]
	endpts = list(np.linspace(min(lvl), max(lvl), len(colorscale) - 1))

	fig = ff.create_choropleth(
	    fips=fips, 
	    values=lvl,
	    binning_endpoints=endpts,
	    colorscale=colorscale,
	    show_state_data=False,
	    show_hover=True, 
	    centroid_marker={'opacity': 0},
	    asp=2.9, 
	    title='USA by Education (Level {})'.format(i),
	    legend_title='Rate'
	)
	py.plot(fig, filename='choropleth_full_usa_{}'.format(i))
