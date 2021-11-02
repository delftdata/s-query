# Helper script to plot all experiments

import plotter

plotter.main(['-c', 'configs.tp_live_snapshot_vanilla'])
plotter.main(['-c', 'configs.tp_vanilla_squery'])
plotter.main(['-c', 'configs.sl_state_size'])
plotter.main(['-c', 'configs.sl_snapshot_query_time'])
plotter.main(['-c', 'configs.sl_incremental'])
plotter.main(['-c', 'configs.sl_incremental_query_time'])
