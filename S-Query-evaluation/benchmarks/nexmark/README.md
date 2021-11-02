NEXMark results folder structure

- `x-node` where `x` is the amount of nodes in the cluster.
  - Folder names inside `x-node` represent the throughput in events/s.
    - File names inside those folders represent the configuration.
      - `s-query` means S-Query was used.
      - `query` means queries were running on state.
      - `500ms/1s/2s` is the snapshot interval.
      - `stock` means normal Jet.
      - `snapshot` means snapshot state in S-Query is enabled.
      - `live` means live state in S-Query is enabled.
      - `.hgrm` extension means histogram file (output from NEXMark tool)
      - No extension means snapshot latency output from `benchmark-getter-job` (see `S-Query-examples` project).
