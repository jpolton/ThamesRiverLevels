# DeeRiverLevels

### [Click here for table](https://jpolton.github.io/DeeRiverLevels/index2.html)

Haven't managed to get this working on GitHub. Works locally...


Useage (locally tested only):

conda activate weir_waterlevel_web_env

(weir_waterlevel_web_env) jelt@LIVMAC13 DeeRiverLevels/scripts % python db_updater.py --db ../docs/data/timeseries.sqlite --once --days 7 --log-file test_update.log

(weir_waterlevel_web_env) jelt@LIVMAC13 DeeRiverLevels/scripts % python db_plotly.py