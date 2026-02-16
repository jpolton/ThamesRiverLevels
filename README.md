# DeeRiverLevels

### [Click here for graph](https://jpolton.github.io/DeeRiverLevels/index.html)



Useage (when running locally on branch local-server):

conda activate weir_waterlevel_web_env

(weir_waterlevel_web_env) jelt@LIVMAC13 DeeRiverLevels/scripts % python db_updater.py --db ../docs/data/timeseries.sqlite --once --days 7 --log-file test_update.log

(weir_waterlevel_web_env) jelt@LIVMAC13 DeeRiverLevels/scripts % python db_plotly.py
