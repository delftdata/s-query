# Import data from benchmark time files under the data/ folder
from .data.query_time import (
    p1_1k_2 as p1_1k,
    p1_10k_2 as p1_10k,
    p1_100k_2 as p1_100k,
    p2_1k_2 as p2_1k,
    p2_10k_2 as p2_10k,
    p2_100k_2 as p2_100k
)

from .data.snapshot_latency import (
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
title = 'Snapshot 2PC latency distribution (query vs. no query)'

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
file_name = 'images/sl_snapshot_query_time.pgf'

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
skip = 300

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

latency_map['Phase 1 (1k+7n)'] = p1_snapshot_1k_7
latency_map['Phase 2 (1k+7n)'] = p2_snapshot_1k_7
latency_map['Phase 1 (10k+7n)'] = p1_snapshot_10k_7
latency_map['Phase 2 (10k+7n)'] = p2_snapshot_10k_7
latency_map['Phase 1 (100k+7n)'] = p1_snapshot_100k_7
latency_map['Phase 2 (100k+7n)'] = p2_snapshot_100k_7

latency_map['Phase 1 (1k+7n+query)'] = p1_1k
latency_map['Phase 2 (1k+7n+query)'] = p2_1k
latency_map['Phase 1 (10k+7n+query)'] = p1_10k
latency_map['Phase 2 (10k+7n+query)'] = p2_10k
latency_map['Phase 1 (100k+7n+query)'] = p1_100k
latency_map['Phase 2 (100k+7n+query)'] = p2_100k

"""
Dict: combined_labels
Where: Key = label of combined value in plot
       Value = labels of other values to combine together (through addition)
"""
combined_labels = {}

combined_labels['No Query 1k'] = ['Phase 1 (1k+7n)', 'Phase 2 (1k+7n)']
combined_labels['No Query 10k'] = ['Phase 1 (10k+7n)',
                                   'Phase 2 (10k+7n)']
combined_labels['No Query 100k'] = ['Phase 1 (100k+7n)',
                                    'Phase 2 (100k+7n)']

combined_labels['Query 1k'] = ['Phase 1 (1k+7n+query)',
                               'Phase 2 (1k+7n+query)']
combined_labels['Query 10k'] = ['Phase 1 (10k+7n+query)',
                                'Phase 2 (10k+7n+query)']
combined_labels['Query 100k'] = ['Phase 1 (100k+7n+query)',
                                 'Phase 2 (100k+7n+query)']

"""
Dict: show_label
Where: Key = label in plot
       Value = True to show in plot, False otherwise
"""
show_label = {}

show_label['Phase 1 (1k+7n)'] = False
show_label['Phase 2 (1k+7n)'] = False
show_label['No Query 1k'] = True

show_label['Phase 1 (10k+7n)'] = False
show_label['Phase 2 (10k+7n)'] = False
show_label['No Query 10k'] = True

show_label['Phase 1 (100k+7n)'] = False
show_label['Phase 2 (100k+7n)'] = False
show_label['No Query 100k'] = True


show_label['Phase 1 (1k+7n+query)'] = False
show_label['Phase 2 (1k+7n+query)'] = False
show_label['Query 1k'] = True

show_label['Phase 1 (10k+7n+query)'] = False
show_label['Phase 2 (10k+7n+query)'] = False
show_label['Query 10k'] = True

show_label['Phase 1 (100k+7n+query)'] = False
show_label['Phase 2 (100k+7n+query)'] = False
show_label['Query 100k'] = True

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

label_line['No Query 1k'] = ('v', '-', 'r')
label_line['No Query 10k'] = ('>', '-', 'g')
label_line['No Query 100k'] = ('<', '-', 'b')

label_line['Query 1k'] = ('^', '--', 'r')
label_line['Query 10k'] = ('x', '--', 'g')
label_line['Query 100k'] = ('o', '--', 'b')
