# Import data from benchmark time files under the data/ folder
from .data.incremental_query import (
    query_1k as query_1k,
    query_10k as query_10k,
    query_100k as query_100k,
    ssid_1k as snapshotid_1k,
    ssid_10k as snapshotid_10k,
    ssid_100k as snapshotid_100k
)

from .data.query_time import (
    query_1k_2 as query_1k_fs,
    query_10k_2 as query_10k_fs,
    query_100k_2 as query_100k_fs,
    snapshotid_1k_2 as snapshotid_1k_fs,
    snapshotid_10k_2 as snapshotid_10k_fs,
    snapshotid_100k_2 as snapshotid_100k_fs
)

"""
String: title
The title of the plot
"""
title = 'SQL query latency distribution (incremental vs. full snapshot)'

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
font_scale = 1.2

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
file_name = 'images/sl_incremental_query_time.pgf'

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
skip = 100

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

latency_map['Incremental 1k'] = query_1k
latency_map['Snapshot ID time 1k'] = snapshotid_1k
latency_map['Incremental 10k'] = query_10k
latency_map['Snapshot ID time 10k'] = snapshotid_10k
latency_map['Incremental 100k'] = query_100k
latency_map['Snapshot ID time 100k'] = snapshotid_100k

latency_map['Full 1k'] = query_1k_fs
latency_map['Snapshot ID time 1k fs'] = snapshotid_1k_fs
latency_map['Full 10k'] = query_10k_fs
latency_map['Snapshot ID time 10k fs'] = snapshotid_10k_fs
latency_map['Full 100k'] = query_100k_fs
latency_map['Snapshot ID time 100k fs'] = snapshotid_100k_fs

"""
Dict: combined_labels
Where: Key = label of combined value in plot
       Value = labels of other values to combine together (through addition)
"""
combined_labels = {}

"""
Dict: show_label
Where: Key = label in plot
       Value = True to show in plot, False otherwise
"""
show_label = {}
show_label['Incremental 1k'] = True
show_label['Snapshot ID time 1k'] = False
show_label['Incremental 10k'] = True
show_label['Snapshot ID time 10k'] = False
show_label['Incremental 100k'] = True
show_label['Snapshot ID time 100k'] = False

show_label['Full 1k'] = True
show_label['Snapshot ID time 1k fs'] = False
show_label['Full 10k'] = True
show_label['Snapshot ID time 10k fs'] = False
show_label['Full 100k'] = True
show_label['Snapshot ID time 100k fs'] = False


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

label_line['Incremental 1k'] = ('v', '-', 'r')
label_line['Full 1k'] = ('<', '-', 'r')
label_line['Incremental 10k'] = ('^', '-', 'g')
label_line['Full 10k'] = ('o', '-', 'g')
label_line['Incremental 100k'] = ('+', '-', 'b')
label_line['Full 100k'] = ('>', '-', 'b')
