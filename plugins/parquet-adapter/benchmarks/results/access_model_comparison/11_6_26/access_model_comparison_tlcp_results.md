# Access Model Comparison TLCP Results

This file contains the raw benchmark result rows from the TLCP access model comparison CSV files.

## Polypheny Relational

Source: `access_model_comparison_polypheny_rf_tlcp_results.csv`

| timestamp                      | query_id | description                 | phase    | run | elapsed_ms | rows    | columns | success | error |
|--------------------------------|----------|-----------------------------|----------|-----|------------|---------|---------|---------|-------|
| 2026-06-11T20:02:04.322905500Z | Q01      | Full record access          | warmup   | 1   | 27460      | 3711544 | 22      | true    |       |
| 2026-06-11T20:02:30.702631600Z | Q01      | Full record access          | measured | 1   | 26375      | 3711544 | 22      | true    |       |
| 2026-06-11T20:02:56.508888700Z | Q01      | Full record access          | measured | 2   | 25808      | 3711544 | 22      | true    |       |
| 2026-06-11T20:03:22.371052800Z | Q01      | Full record access          | measured | 3   | 25859      | 3711544 | 22      | true    |       |
| 2026-06-11T20:03:48.611189800Z | Q01      | Full record access          | measured | 4   | 26238      | 3711544 | 22      | true    |       |
| 2026-06-11T20:04:15.053012Z    | Q01      | Full record access          | measured | 5   | 26432      | 3711544 | 22      | true    |       |
| 2026-06-11T20:04:23.672906800Z | Q02      | Projection                  | warmup   | 1   | 8618       | 3711544 | 5       | true    |       |
| 2026-06-11T20:04:32.827854800Z | Q02      | Projection                  | measured | 1   | 9154       | 3711544 | 5       | true    |       |
| 2026-06-11T20:04:41.665483800Z | Q02      | Projection                  | measured | 2   | 8836       | 3711544 | 5       | true    |       |
| 2026-06-11T20:04:50.278581600Z | Q02      | Projection                  | measured | 3   | 8612       | 3711544 | 5       | true    |       |
| 2026-06-11T20:04:58.725750700Z | Q02      | Projection                  | measured | 4   | 8446       | 3711544 | 5       | true    |       |
| 2026-06-11T20:05:07.676704800Z | Q02      | Projection                  | measured | 5   | 8953       | 3711544 | 5       | true    |       |
| 2026-06-11T20:05:08.635764Z    | Q03      | Filtered count              | warmup   | 1   | 955        | 1       | 1       | true    |       |
| 2026-06-11T20:05:09.194810Z    | Q03      | Filtered count              | measured | 1   | 560        | 1       | 1       | true    |       |
| 2026-06-11T20:05:09.719193900Z | Q03      | Filtered count              | measured | 2   | 523        | 1       | 1       | true    |       |
| 2026-06-11T20:05:10.229289800Z | Q03      | Filtered count              | measured | 3   | 508        | 1       | 1       | true    |       |
| 2026-06-11T20:05:10.740282Z    | Q03      | Filtered count              | measured | 4   | 509        | 1       | 1       | true    |       |
| 2026-06-11T20:05:11.371187100Z | Q03      | Filtered count              | measured | 5   | 631        | 1       | 1       | true    |       |
| 2026-06-11T20:05:11.451489900Z | Q04      | Filtered full record access | warmup   | 1   | 77         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.463724200Z | Q04      | Filtered full record access | measured | 1   | 11         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.476779800Z | Q04      | Filtered full record access | measured | 2   | 12         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.488827900Z | Q04      | Filtered full record access | measured | 3   | 12         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.502028800Z | Q04      | Filtered full record access | measured | 4   | 12         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.512541700Z | Q04      | Filtered full record access | measured | 5   | 11         | 0       | 22      | true    |       |
| 2026-06-11T20:05:11.590520800Z | Q05      | Filtered projection         | warmup   | 1   | 74         | 0       | 5       | true    |       |
| 2026-06-11T20:05:11.599519500Z | Q05      | Filtered projection         | measured | 1   | 8          | 0       | 5       | true    |       |
| 2026-06-11T20:05:11.612525500Z | Q05      | Filtered projection         | measured | 2   | 12         | 0       | 5       | true    |       |
| 2026-06-11T20:05:11.622031100Z | Q05      | Filtered projection         | measured | 3   | 8          | 0       | 5       | true    |       |
| 2026-06-11T20:05:11.633037Z    | Q05      | Filtered projection         | measured | 4   | 10         | 0       | 5       | true    |       |
| 2026-06-11T20:05:11.644040600Z | Q05      | Filtered projection         | measured | 5   | 10         | 0       | 5       | true    |       |

## Polypheny Document MQL

Source: `access_model_comparison_polypheny_mql_tlcp_results.csv`

| timestamp                      | query_id | description                   | phase    | run | elapsed_ms | rows    | columns | success | error |
|--------------------------------|----------|-------------------------------|----------|-----|------------|---------|---------|---------|-------|
| 2026-06-11T18:57:18.950866100Z | Q01      | Full document access          | warmup   | 1   | 146487     | 3711544 | 20      | true    |       |
| 2026-06-11T18:59:40.878060600Z | Q01      | Full document access          | measured | 1   | 141914     | 3711544 | 20      | true    |       |
| 2026-06-11T19:02:01.558648500Z | Q01      | Full document access          | measured | 2   | 140681     | 3711544 | 20      | true    |       |
| 2026-06-11T19:04:22.409971200Z | Q01      | Full document access          | measured | 3   | 140847     | 3711544 | 20      | true    |       |
| 2026-06-11T19:06:43.085124100Z | Q01      | Full document access          | measured | 4   | 140674     | 3711544 | 20      | true    |       |
| 2026-06-11T19:09:02.860315100Z | Q01      | Full document access          | measured | 5   | 139777     | 3711544 | 20      | true    |       |
| 2026-06-11T19:10:49.613562100Z | Q02      | Projection                    | warmup   | 1   | 106748     | 3711544 | 6       | true    |       |
| 2026-06-11T19:12:37.236250Z    | Q02      | Projection                    | measured | 1   | 107622     | 3711544 | 6       | true    |       |
| 2026-06-11T19:14:24.269443500Z | Q02      | Projection                    | measured | 2   | 107031     | 3711544 | 6       | true    |       |
| 2026-06-11T19:16:11.689652200Z | Q02      | Projection                    | measured | 3   | 107420     | 3711544 | 6       | true    |       |
| 2026-06-11T19:17:58.630301400Z | Q02      | Projection                    | measured | 4   | 106939     | 3711544 | 6       | true    |       |
| 2026-06-11T19:19:50.212246Z    | Q02      | Projection                    | measured | 5   | 111583     | 3711544 | 6       | true    |       |
| 2026-06-11T19:21:11.230251400Z | Q03      | Filtered count                | warmup   | 1   | 81015      | 1       | 1       | true    |       |
| 2026-06-11T19:22:32.692593700Z | Q03      | Filtered count                | measured | 1   | 81461      | 1       | 1       | true    |       |
| 2026-06-11T19:23:54.995095900Z | Q03      | Filtered count                | measured | 2   | 82302      | 1       | 1       | true    |       |
| 2026-06-11T19:25:16.529502700Z | Q03      | Filtered count                | measured | 3   | 81532      | 1       | 1       | true    |       |
| 2026-06-11T19:26:37.617071Z    | Q03      | Filtered count                | measured | 4   | 81087      | 1       | 1       | true    |       |
| 2026-06-11T19:27:57.890478900Z | Q03      | Filtered count                | measured | 5   | 80273      | 1       | 1       | true    |       |
| 2026-06-11T19:29:24.111617200Z | Q04      | Filtered full document access | warmup   | 1   | 86220      | 283006  | 20      | true    |       |
| 2026-06-11T19:30:49.465861100Z | Q04      | Filtered full document access | measured | 1   | 85352      | 283006  | 20      | true    |       |
| 2026-06-11T19:32:15.176393900Z | Q04      | Filtered full document access | measured | 2   | 85709      | 283006  | 20      | true    |       |
| 2026-06-11T19:33:40.304629900Z | Q04      | Filtered full document access | measured | 3   | 85128      | 283006  | 20      | true    |       |
| 2026-06-11T19:35:05.504422200Z | Q04      | Filtered full document access | measured | 4   | 85199      | 283006  | 20      | true    |       |
| 2026-06-11T19:36:30.339635500Z | Q04      | Filtered full document access | measured | 5   | 84831      | 283006  | 20      | true    |       |
| 2026-06-11T19:37:53.751224200Z | Q05      | Filtered projection           | warmup   | 1   | 83409      | 283006  | 6       | true    |       |
| 2026-06-11T19:39:17.115780200Z | Q05      | Filtered projection           | measured | 1   | 83364      | 283006  | 6       | true    |       |
| 2026-06-11T19:40:40.715102800Z | Q05      | Filtered projection           | measured | 2   | 83598      | 283006  | 6       | true    |       |
| 2026-06-11T19:42:03.812402600Z | Q05      | Filtered projection           | measured | 3   | 83096      | 283006  | 6       | true    |       |
| 2026-06-11T19:43:27.234388Z    | Q05      | Filtered projection           | measured | 4   | 83422      | 283006  | 6       | true    |       |
| 2026-06-11T19:44:50.397084400Z | Q05      | Filtered projection           | measured | 5   | 83162      | 283006  | 6       | true    |       |

## DuckDB

Source: `access_model_comparison_duckdb_tlcp_results.csv`

| timestamp                      | query_id | description                 | phase    | run | elapsed_ms | rows    | columns | success | error |
|--------------------------------|----------|-----------------------------|----------|-----|------------|---------|---------|---------|-------|
| 2026-06-11T20:06:31.581772500Z | Q01      | Full record access          | warmup   | 1   | 2568       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:34.366902100Z | Q01      | Full record access          | measured | 1   | 2786       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:36.924498100Z | Q01      | Full record access          | measured | 2   | 2558       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:39.735769300Z | Q01      | Full record access          | measured | 3   | 2807       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:42.398329600Z | Q01      | Full record access          | measured | 4   | 2662       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:45.678604900Z | Q01      | Full record access          | measured | 5   | 3269       | 3711544 | 22      | true    |       |
| 2026-06-11T20:06:45.842128700Z | Q02      | Projection                  | warmup   | 1   | 161        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.009288800Z | Q02      | Projection                  | measured | 1   | 168        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.183710500Z | Q02      | Projection                  | measured | 2   | 173        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.369324Z    | Q02      | Projection                  | measured | 3   | 183        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.550356800Z | Q02      | Projection                  | measured | 4   | 184        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.732551300Z | Q02      | Projection                  | measured | 5   | 179        | 3711544 | 5       | true    |       |
| 2026-06-11T20:06:46.768651200Z | Q03      | Filtered count              | warmup   | 1   | 32         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.799113100Z | Q03      | Filtered count              | measured | 1   | 31         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.836965200Z | Q03      | Filtered count              | measured | 2   | 35         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.883365600Z | Q03      | Filtered count              | measured | 3   | 46         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.927137900Z | Q03      | Filtered count              | measured | 4   | 41         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.970439800Z | Q03      | Filtered count              | measured | 5   | 42         | 1       | 1       | true    |       |
| 2026-06-11T20:06:46.983052700Z | Q04      | Filtered full record access | warmup   | 1   | 11         | 0       | 22      | true    |       |
| 2026-06-11T20:06:46.998657800Z | Q04      | Filtered full record access | measured | 1   | 13         | 0       | 22      | true    |       |
| 2026-06-11T20:06:47.018166900Z | Q04      | Filtered full record access | measured | 2   | 17         | 0       | 22      | true    |       |
| 2026-06-11T20:06:47.030161300Z | Q04      | Filtered full record access | measured | 3   | 12         | 0       | 22      | true    |       |
| 2026-06-11T20:06:47.045945500Z | Q04      | Filtered full record access | measured | 4   | 13         | 0       | 22      | true    |       |
| 2026-06-11T20:06:47.063416700Z | Q04      | Filtered full record access | measured | 5   | 18         | 0       | 22      | true    |       |
| 2026-06-11T20:06:47.076852700Z | Q05      | Filtered projection         | warmup   | 1   | 13         | 0       | 5       | true    |       |
| 2026-06-11T20:06:47.092357100Z | Q05      | Filtered projection         | measured | 1   | 13         | 0       | 5       | true    |       |
| 2026-06-11T20:06:47.107606100Z | Q05      | Filtered projection         | measured | 2   | 12         | 0       | 5       | true    |       |
| 2026-06-11T20:06:47.122789200Z | Q05      | Filtered projection         | measured | 3   | 16         | 0       | 5       | true    |       |
| 2026-06-11T20:06:47.138406400Z | Q05      | Filtered projection         | measured | 4   | 12         | 0       | 5       | true    |       |
| 2026-06-11T20:06:47.148626400Z | Q05      | Filtered projection         | measured | 5   | 12         | 0       | 5       | true    |       |

## Apache Spark

Source: `access_model_comparison_spark_tlcp_results.csv`

| timestamp                   | query_id | description                 | phase    | run | elapsed_ms | rows    | columns | success | error |
|-----------------------------|----------|-----------------------------|----------|-----|------------|---------|---------|---------|-------|
| 2026-06-11T20:08:11.202281Z | Q01      | Full record access          | warmup   | 1   | 27845      | 3711544 | 22      | true    |       |
| 2026-06-11T20:08:31.149931Z | Q01      | Full record access          | measured | 1   | 19947      | 3711544 | 22      | true    |       |
| 2026-06-11T20:08:52.997061Z | Q01      | Full record access          | measured | 2   | 19449      | 3711544 | 22      | true    |       |
| 2026-06-11T20:09:12.591150Z | Q01      | Full record access          | measured | 3   | 19593      | 3711544 | 22      | true    |       |
| 2026-06-11T20:09:35.059883Z | Q01      | Full record access          | measured | 4   | 20172      | 3711544 | 22      | true    |       |
| 2026-06-11T20:09:56.709063Z | Q01      | Full record access          | measured | 5   | 20031      | 3711544 | 22      | true    |       |
| 2026-06-11T20:10:10.017439Z | Q02      | Projection                  | warmup   | 1   | 13307      | 3711544 | 5       | true    |       |
| 2026-06-11T20:10:25.359033Z | Q02      | Projection                  | measured | 1   | 13229      | 3711544 | 5       | true    |       |
| 2026-06-11T20:10:38.260598Z | Q02      | Projection                  | measured | 2   | 12901      | 3711544 | 5       | true    |       |
| 2026-06-11T20:10:53.632397Z | Q02      | Projection                  | measured | 3   | 13294      | 3711544 | 5       | true    |       |
| 2026-06-11T20:11:05.793956Z | Q02      | Projection                  | measured | 4   | 12161      | 3711544 | 5       | true    |       |
| 2026-06-11T20:11:18.581505Z | Q02      | Projection                  | measured | 5   | 12787      | 3711544 | 5       | true    |       |
| 2026-06-11T20:11:20.488438Z | Q03      | Filtered count              | warmup   | 1   | 1905       | 1       | 1       | true    |       |
| 2026-06-11T20:11:23.866016Z | Q03      | Filtered count              | measured | 1   | 1264       | 1       | 1       | true    |       |
| 2026-06-11T20:11:24.992046Z | Q03      | Filtered count              | measured | 2   | 1125       | 1       | 1       | true    |       |
| 2026-06-11T20:11:25.983035Z | Q03      | Filtered count              | measured | 3   | 990        | 1       | 1       | true    |       |
| 2026-06-11T20:11:26.877336Z | Q03      | Filtered count              | measured | 4   | 893        | 1       | 1       | true    |       |
| 2026-06-11T20:11:27.903714Z | Q03      | Filtered count              | measured | 5   | 1025       | 1       | 1       | true    |       |
| 2026-06-11T20:11:28.029976Z | Q04      | Filtered full record access | warmup   | 1   | 124        | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.121348Z | Q04      | Filtered full record access | measured | 1   | 90         | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.216447Z | Q04      | Filtered full record access | measured | 2   | 94         | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.298063Z | Q04      | Filtered full record access | measured | 3   | 81         | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.392095Z | Q04      | Filtered full record access | measured | 4   | 93         | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.481201Z | Q04      | Filtered full record access | measured | 5   | 88         | 0       | 22      | true    |       |
| 2026-06-11T20:11:28.599850Z | Q05      | Filtered projection         | warmup   | 1   | 117        | 0       | 5       | true    |       |
| 2026-06-11T20:11:28.697370Z | Q05      | Filtered projection         | measured | 1   | 96         | 0       | 5       | true    |       |
| 2026-06-11T20:11:28.811912Z | Q05      | Filtered projection         | measured | 2   | 113        | 0       | 5       | true    |       |
| 2026-06-11T20:11:28.926642Z | Q05      | Filtered projection         | measured | 3   | 114        | 0       | 5       | true    |       |
| 2026-06-11T20:11:29.012868Z | Q05      | Filtered projection         | measured | 4   | 85         | 0       | 5       | true    |       |
| 2026-06-11T20:11:29.096270Z | Q05      | Filtered projection         | measured | 5   | 82         | 0       | 5       | true    |       |
