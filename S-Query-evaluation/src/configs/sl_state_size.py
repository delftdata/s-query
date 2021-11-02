# Import data from benchmark time files under the data/ folder
from .data.snapshot_latency import (
    p1_stock_1k as p1_stock_1k_7,
    p2_stock_1k as p2_stock_1k_7,
    p1_stock_10k as p1_stock_10k_7,
    p2_stock_10k as p2_stock_10k_7,
    p1_stock_100k as p1_stock_100k_7,
    p2_stock_100k as p2_stock_100k_7,
    p1_snapshot_1k as p1_snapshot_1k_7,
    p2_snapshot_1k as p2_snapshot_1k_7,
    p1_snapshot_10k as p1_snapshot_10k_7,
    p2_snapshot_10k as p2_snapshot_10k_7,
    p1_snapshot_100k as p1_snapshot_100k_7,
    p2_snapshot_100k as p2_snapshot_100k_7,
)

"""
String: title
The title of the plot
"""
title = 'Snapshot 2PC latency distribution'

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
file_name = 'images/sl_state_size.pgf'

"""
Int: num_intervals
Amount of intervals to display
0-90, 90-99, 99-99.9, 99.9-99.99 are the first four intervals.
So the more intervals the futher into the nines you go.
Must be at least 1
"""
num_intervals = 3

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

"""
Dict: latency_map
Where: Key = label in plot
       Value = list of latencies
"""
latency_map = {}

latency_map['Phase 1 (S-Query 7 1k)'] = p1_snapshot_1k_7
latency_map['Phase 2 (S-Query 7 1k)'] = p2_snapshot_1k_7
latency_map['Phase 1 (S-Query 7 10k)'] = p1_snapshot_10k_7
latency_map['Phase 2 (S-Query 7 10k)'] = p2_snapshot_10k_7
latency_map['Phase 1 (S-Query 7 100k)'] = p1_snapshot_100k_7
latency_map['Phase 2 (S-Query 7 100k)'] = p2_snapshot_100k_7

latency_map['Phase 1 (Vanilla 7 1k)'] = p1_stock_1k_7
latency_map['Phase 2 (Vanilla 7 1k)'] = p2_stock_1k_7
latency_map['Phase 1 (Vanilla 7 10k)'] = p1_stock_10k_7
latency_map['Phase 2 (Vanilla 7 10k)'] = p2_stock_10k_7
latency_map['Phase 1 (Vanilla 7 100k)'] = p1_stock_100k_7
latency_map['Phase 2 (Vanilla 7 100k)'] = p2_stock_100k_7

"""
Dict: combined_labels
Where: Key = label of combined value in plot
       Value = labels of other values to combine together (through addition)
"""
combined_labels = {}

combined_labels['S-Query 1k'] = ['Phase 1 (S-Query 7 1k)',
                                 'Phase 2 (S-Query 7 1k)']
combined_labels['S-Query 10k'] = ['Phase 1 (S-Query 7 10k)',
                                  'Phase 2 (S-Query 7 10k)']
combined_labels['S-Query 100k'] = ['Phase 1 (S-Query 7 100k)',
                                   'Phase 2 (S-Query 7 100k)']

combined_labels['\\vanilla[upper] 1k'] = ['Phase 1 (Vanilla 7 1k)',
                                          'Phase 2 (Vanilla 7 1k)']
combined_labels['\\vanilla[upper] 10k'] = ['Phase 1 (Vanilla 7 10k)',
                                           'Phase 2 (Vanilla 7 10k)']
combined_labels['\\vanilla[upper] 100k'] = ['Phase 1 (Vanilla 7 100k)',
                                            'Phase 2 (Vanilla 7 100k)']

"""
Dict: show_label
Where: Key = label in plot
       Value = True to show in plot, False otherwise
"""
show_label = {}

show_label['Phase 1 (Vanilla 7 1k)'] = False
show_label['Phase 2 (Vanilla 7 1k)'] = False
show_label['\\vanilla[upper] 1k'] = True

show_label['Phase 1 (Vanilla 7 10k)'] = False
show_label['Phase 2 (Vanilla 7 10k)'] = False
show_label['\\vanilla[upper] 10k'] = True

show_label['Phase 1 (Vanilla 7 100k)'] = False
show_label['Phase 2 (Vanilla 7 100k)'] = False
show_label['\\vanilla[upper] 100k'] = True


show_label['Phase 1 (S-Query 7 1k)'] = False
show_label['Phase 2 (S-Query 7 1k)'] = False
show_label['S-Query 1k'] = True

show_label['Phase 1 (S-Query 7 10k)'] = False
show_label['Phase 2 (S-Query 7 10k)'] = False
show_label['S-Query 10k'] = True

show_label['Phase 1 (S-Query 7 100k)'] = False
show_label['Phase 2 (S-Query 7 100k)'] = False
show_label['S-Query 100k'] = True

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

label_line['\\vanilla[upper] 1k'] = ('v', '-', 'r')
label_line['\\vanilla[upper] 10k'] = ('^', '-', 'g')
label_line['\\vanilla[upper] 100k'] = ('>', '-', 'b')

label_line['S-Query 1k'] = ('<', '--', 'r')
label_line['S-Query 10k'] = ('x', '--', 'g')
label_line['S-Query 100k'] = ('o', '--', 'b')
