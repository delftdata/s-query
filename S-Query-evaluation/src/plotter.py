import argparse
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import itertools
import importlib
import sys
import os
import matplotlib as mpl
from hdr_parser import parse_hgrm
from errno import EEXIST

# import configs.example_plot_config


def parse_args(args):
    """
    Parses the arguments of the benchmark plot cli
    Arguments:
        args: Program arguments, excluding first argument (filename being executed)
    """
    parser = argparse.ArgumentParser(description="Jet benchmark plot tool")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="configs.plot_config",
        help="The config file location",
    )
    return parser.parse_args(args)


def nanos_to_millis(array):
    """
    Converts an array of nanoseconds to milliseconds
    Arguments:
        array: The list of latencies in nanoseconds
    """
    return list(map(lambda x: x/1000000.0, array))


def combine(*arrays):
    """
    Combines n arrays by adding the elements at the same index together in the new array
    Arguments:
        *arrays: N arrays to combine
    """
    return [sum(elements) for elements in zip(*arrays)]


def get_percentiles(latency_list, percentages):
    """
    Get the percentiles of the given latency list
    Arguments:
        latency_list: The list with latencies as elements
        p: List of percentiles to plot
    """
    latency_np = np.array(latency_list)  # Convert to numpy array
    sorted_np = np.sort(latency_np)  # Sort latencies

    perc = np.percentile(sorted_np, percentages)
    return perc


def preprocessing(percentages, array):
    """
    Calls the nanos to millis and get_percentile function
    Arguments:
        percentages: The percentiles to calculate
        array: The list of latencies
    """
    return get_percentiles(nanos_to_millis(array), percentages)


def cut(length, skip_last, array):
    last = min(skip_last, len(array))
    return array[length:last]


def cut_multiple(length, skip_last, *arrays):
    return [cut(length, skip_last, arr) for arr in arrays]


def preprocessing_combined(percentages, *arrays):
    """
    Calls the nanos to millis, combine and get_percentile function
    Arguments:
        percentages: The percentiles to calculate
        *arrays: N arrays to combine and preprocess
    """
    return get_percentiles(nanos_to_millis(combine(*arrays)), percentages)


def main(args):
    """
    Main function, program entrypoint
    Arguments:
        args: The program arguments (excluding first argument which is the file being executed)
    """
    params = parse_args(args)
    config_name = params.config

    # Print config we are using
    print(f'Config: {config_name}')

    # Import config as module
    config = importlib.import_module(config_name)

    # Get plot details
    plot_title = config.title
    x_axis_label = config.x_axis_label
    y_axis_label = config.y_axis_label
    font_scale = config.font_scale
    dark_mode = config.dark_mode
    y_log = config.y_log

    # Get number of intervals
    num_intervals = config.num_intervals

    # Get sample points at which to sample intervals
    sample_points = [15.0, 30.0, 50.0, 60.0, 70.0, 80.0, 90.0]
    total_sample_points = sample_points.copy()
    for i in range(1, num_intervals):
        final_point = total_sample_points[-1]
        total_sample_points.extend(
            [final_point + j/(10.0 ** i) for j in sample_points])
    np_sample_points = np.array(total_sample_points)

    # Get file names
    filename = config.file_name

    # Get line formats
    line_formats = config.label_line

    # Get legend title
    legend_title = ''
    try:
        legend_title = config.legend_title
    except:
        pass

    # Dict where key = label, value = percentiles
    perc_map = {}

    # Get n first latencies (warmup)
    skip = 0
    try:
        skip = config.skip
    except:
        pass

    # Only take n latencies

    skip_last = 5000
    try:
        skip_last = config.plot_amount
    except:
        pass

    percentage_map = {}

    try:
        # Get mappings
        label_map = config.latency_map
        combined_columns = config.combined_labels
        show_label = config.show_label

        # Individual latencies
        for label, latencies in label_map.items():
            if show_label[label]:
                if (len(latencies) == 0):
                    continue
                # Preprocess the latencies and put them in the map
                perc_map[label] = preprocessing(
                    np_sample_points, cut(skip, skip_last, latencies))
                percentage_map[label] = np_sample_points
                taking = min(skip_last, len(latencies)) - skip
                print(f"{label} plotting {taking} latencies")

        # Combined latencies
        for label, labels in combined_columns.items():
            if show_label[label]:
                # Get list of latencies to combine
                latencies = [label_map[sub_label] for sub_label in labels]
                if (len(latencies[0]) == 0):
                    continue
                # Preprocess and combine the latencies
                perc_map[label] = preprocessing_combined(
                    np_sample_points, *cut_multiple(skip, skip_last, *latencies))
                percentage_map[label] = np_sample_points
                taking = min(skip_last, len(latencies[0])) - skip
                print(f"{label} plotting {taking} latencies")
    except:
        pass

    try:
        hgrm_columns = config.histogram_columns
        for label, hgrm_filename in hgrm_columns.items():
            (latencies, percentiles) = parse_hgrm(hgrm_filename)
            perc_map[label] = latencies
            percentage_map[label] = percentiles
    except:
        pass

        # Plot the percentiles
    plot_percentiles_multiple(plot_title, perc_map, percentage_map, filename, num_intervals,
                              y_log, line_formats, x_axis_label, y_axis_label, font_scale, dark_mode, legend_title)

    return


def plot_percentiles_multiple(title, percentiles_map, percentages_map, filename, num_intervals, y_log, line_formats, x_axis_label, y_axis_label, font_scale, dark_mode, legend_title):
    """
    Plot function for the given percentiles
    Adapted from https://stackoverflow.com/questions/42072734/percentile-distribution-graph
    Arguments:
        title: Title of the plot
        percentiles_map: Dict where key is label and value are percentiles
        percentages_map: Dict where key is label and value list of percentages used
        filename: The destination file name of the output image
        num_intervals: The number of intervals to display
        y_log: True will display y axis on log scale, False will use linear scale
        line_formats: Dict where key is label and value is tuple of (marker, linestyle, color) for plot
        x_axis_label: Label below x axis
        y_axis_label: Label to the left of y axis
        font_scale: Scale of font, 1 is normal
        dark_mode: Whether to use dark mode
        legend_title: Title of the legend
    """
    # Reset sns before every plot
    sns.reset_defaults()

    clear_bkgd = {
        'axes.facecolor': 'none',
        'figure.facecolor': 'none'
    }

    # Dark mode
    if dark_mode:
        dark_text_color = 'white'

        clear_bkgd['text.color'] = dark_text_color
        clear_bkgd['axes.labelcolor'] = dark_text_color
        clear_bkgd['xtick.color'] = dark_text_color
        clear_bkgd['ytick.color'] = dark_text_color

        sns.set(style='darkgrid', font_scale=font_scale,
                palette="muted", rc=clear_bkgd)
    else:
        clear_bkgd = {'axes.facecolor': 'none', 'figure.facecolor': 'none'}
        sns.set(style='ticks', font_scale=font_scale,
                palette="muted", rc=clear_bkgd)

    if os.path.splitext(filename)[1] == '.pgf':
        mpl.use("pgf")
        mpl.rcParams.update({
            # "pgf.texsystem": "pdflatex",
            "pgf.preamble": "\n".join([
                r"\newcommand{\vanilla}{Jet}"
            ]),
        })

    # Increase by one to get last xtick included as well
    num_intervals += 1

    # Number of intervals to display.
    # Later calculations add 2 to this number to pad it to align with the reversed axis
    x_values = 1.0 - 1.0 / 10 ** np.arange(0, num_intervals + 2)

    # Start with hard-coded lengths for 0,90,99
    # Rest of array generated to display correct number of decimal places as precision increases
    lengths = [1, 2, 2] + \
        [int(v) + 1 for v in list(np.arange(3, num_intervals + 2))]

    # Build the label string by trimming on the calculated lengths and appending %
    labels = [str(100 * v)[0:l] + "%" for v, l in zip(x_values, lengths)]

    fig, ax = plt.subplots()
    # sns.set(rc={'figure.figsize': (10, 5)})
    # Set x axis to log scale
    ax.set_xscale('log')
    # Invert x axis
    plt.gca().invert_xaxis()
    # Labels have to be reversed because axis is reversed
    ax.xaxis.set_ticklabels(labels[::-1])

    # Cyclic iteraters for markers and linestyles
    markers = itertools.cycle(['.', ',', 'o', 'v', '^', '<', '>', '1', '2',
                               '3', '4', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd', '|', '_'])
    linestyles = itertools.cycle(['-', '--', '-.', ':'])

    # Plot distribution with different markers and lines each time
    for key in percentiles_map:
        if key in line_formats:
            marker = line_formats[key][0]
            linestyle = line_formats[key][1]
            color = line_formats[key][2]
            ax.plot([100.0 - v for v in percentages_map[key]], percentiles_map[key], marker=marker,
                    linestyle=linestyle, color=color, label=key, alpha=0.7)
        else:
            ax.plot([100.0 - v for v in percentages_map[key]], percentiles_map[key], marker=next(markers),
                    linestyle=next(linestyles), label=key, alpha=0.7)

    # Grid lines
    # Major lines (every 90%, 99%, 99.9%, etc.)
    ax.grid(True, linewidth=0.5, zorder=5)
    # Minor lines (10%, 20%,..., 80%, 91%, 92%,...,98%, etc.)
    ax.grid(True, which='minor', linewidth=0.5,
            linestyle=':')

    # Make y axis log scale
    if (y_log):
        ax.set_yscale('log')

    # Set y axis label
    ax.set_ylabel(y_axis_label)
    # Set x axis label
    ax.set_xlabel(x_axis_label)
    # Set title
    ax.set_title(title)

    # Put a legend to the right of the plot
    lg = ax.legend(loc='center left', title=legend_title, bbox_to_anchor=(
        1.0, 0.5))

    sns.despine(fig=fig)

    print(f"Saving to: {filename}")

    directory = os.path.dirname(filename)
    create_dir(directory)

    fig.savefig(filename, bbox_extra_artists=(
        lg,), bbox_inches='tight')
    fig.savefig(os.path.splitext(filename)[0] + '.png', bbox_extra_artists=(
        lg,), bbox_inches='tight')


def create_dir(dir_path):
    """
    Ensures a given directory exists.
    Arguments:
        dir_path: directory to potentially create
    """
    try:
        os.makedirs(dir_path)
    except OSError as error:
        if error.errno == EEXIST and os.path.isdir(dir_path):
            pass
        else: raise error


if __name__ == "__main__":
    main(sys.argv[1:])
