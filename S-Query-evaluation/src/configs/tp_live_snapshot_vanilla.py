"""
String: title
The title of the plot
"""
title = 'Source-sink latency distribution'

"""
String: x_axis_label
Label below the x axis
"""
x_axis_label = 'Percentiles'

"""
String: y_axis_label
Label to the left of the y axis
"""
y_axis_label = 'Latency (ms)'

"""
float: font_scale
By how much to scale the font (1.0 is normal)
"""
font_scale = 1.4

"""
bool: dark_mode
Whether to use dark background and white text
"""
dark_mode = False

"""
bool: y_log
Whether to print the y axis on a log scale or not
"""
y_log = False

"""
String: file_name
The output plot image filename (directories in the path must already exist otherwise it won't work)
"""
file_name = 'images/tp_live_snapshot_vanilla.pgf'

"""
Int: num_intervals
Amount of intervals to display
0-90, 90-99, 99-99.9, 99.9-99.99 are the first four intervals.
So the more intervals the futher into the nines you go.
Must be at least 1
"""
num_intervals = 4

"""
Int: skip
Amount of latencies to skip (warmup)
Must be 0 or higher
"""
skip = 500

"""
Int: skip
Amount of latencies to plot (starting from beginning)
Must be skip or higher
"""
plot_amount = 5000

histogram_columns = {}
histogram_columns['S-Query live+snap'] = 'benchmarks/nexmark/3-node/1k/s-query-live+snapshot.hgrm'
histogram_columns['S-Query live'] = 'benchmarks/nexmark/3-node/1k/live+stock.hgrm'
histogram_columns['S-Query snap'] = 'benchmarks/nexmark/3-node/1k/s-query-snapshot.hgrm'

histogram_columns['\\vanilla[upper]'] = 'benchmarks/nexmark/3-node/1k/stock.hgrm'

""" 
Dict: label_line
Where: Key = label in plot
       Value = tuple of (marker, line, color) for the given label
"""
"""
Example values for markers, line types, and colors below, source: https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.plot.html
Markers
character 	description
'.' 	point marker
',' 	pixel marker
'o' 	circle marker
'v' 	triangle_down marker
'^' 	triangle_up marker
'<' 	triangle_left marker
'>' 	triangle_right marker
'1' 	tri_down marker
'2' 	tri_up marker
'3' 	tri_left marker
'4' 	tri_right marker
's' 	square marker
'p' 	pentagon marker
'*' 	star marker
'h' 	hexagon1 marker
'H' 	hexagon2 marker
'+' 	plus marker
'x' 	x marker
'D' 	diamond marker
'd' 	thin_diamond marker
'|' 	vline marker
'_' 	hline marker

Line Styles
character 	description
'-' 	solid line style
'--' 	dashed line style
'-.' 	dash-dot line style
':' 	dotted line style

Colors
The supported color abbreviations are the single letter codes
character 	color
'b' 	blue
'g' 	green
'r' 	red
'c' 	cyan
'm' 	magenta
'y' 	yellow
'k' 	black
'w' 	white
"""
label_line = {}
