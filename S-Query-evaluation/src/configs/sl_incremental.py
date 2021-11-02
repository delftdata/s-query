# Import data from benchmark time files under the data/ folder
from .data.incremental_sl import (
    p1_2k as p1_2k,
    p1_20k as p1_20k,
    p1_200k as p1_200k,
    p2_2k as p2_2k,
    p2_20k as p2_20k,
    p2_200k as p2_200k
)

from .data.snapshot_latency import (
    p1_snapshot_100k as p1_fs,
    p2_snapshot_100k as p2_fs,
)

from .data.query_time import (
    p1_100k_2 as p1_fsq,
    p2_100k_2 as p2_fsq
)

"""
String: title
The title of the plot
"""
title = 'Snapshot 2PC latency distribution (full vs. incremental)'

"""
String: legend_title
The title of the legend
"""
# legend_title = 'Delta'

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
file_name = 'images/sl_incremental.pgf'

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

latency_map['Phase 1 (2k)'] = p1_2k
latency_map['Phase 2 (2k)'] = p2_2k
latency_map['Phase 1 (20k)'] = p1_20k
latency_map['Phase 2 (20k)'] = p2_20k
latency_map['Phase 1 (200k)'] = p1_200k
latency_map['Phase 2 (200k)'] = p2_200k

# Full snapshot no query
latency_map['Phase 1 (fs)'] = p1_fs
latency_map['Phase 2 (fs)'] = p2_fs

# Full snapshot query
latency_map['Phase 1 (fsq)'] = p1_fsq
latency_map['Phase 2 (fsq)'] = p2_fsq

"""
Dict: combined_labels
Where: Key = label of combined value in plot
       Value = labels of other values to combine together (through addition)
"""
combined_labels = {}


combined_labels['1% delta'] = ['Phase 1 (2k)', 'Phase 2 (2k)']
combined_labels['10% delta'] = ['Phase 1 (20k)',
                                'Phase 2 (20k)']
combined_labels['100% delta'] = ['Phase 1 (200k)',
                                 'Phase 2 (200k)']
combined_labels['Full snapshot'] = ['Phase 1 (fs)',
                                    'Phase 2 (fs)']
combined_labels['Full snap. query'] = ['Phase 1 (fsq)',
                                       'Phase 2 (fsq)']

"""
Dict: show_label
Where: Key = label in plot
       Value = True to show in plot, False otherwise
"""
show_label = {}
show_label['Phase 1 (2k)'] = False
show_label['Phase 2 (2k)'] = False
show_label['1% delta'] = True

show_label['Phase 1 (20k)'] = False
show_label['Phase 2 (20k)'] = False
show_label['10% delta'] = True

show_label['Phase 1 (200k)'] = False
show_label['Phase 2 (200k)'] = False
show_label['100% delta'] = True

show_label['Phase 1 (fs)'] = False
show_label['Phase 2 (fs)'] = False
show_label['Full snapshot'] = True

show_label['Phase 1 (fsq)'] = False
show_label['Phase 2 (fsq)'] = False
show_label['Full snap. query'] = False

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

label_line['1% delta'] = ('v', '-', 'r')
label_line['10% delta'] = ('>', '-', 'g')
label_line['100% delta'] = ('<', '-', 'b')
label_line['Full snapshot'] = ('^', '--', 'c')
label_line['Full snap. query'] = ('+', '--', 'm')
