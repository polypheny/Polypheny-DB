# Benchmark Results

## Aggregation Results 

Time displayed in milliseconds (ms)

| Query | Description                                       | Polypheny before optimization | Polypheny after optimization | DuckDB | Spark | 
|-------|---------------------------------------------------|------------------------------:|-----------------------------:|-------:|------:|
| Q01   | Full Yellow Taxi Row Count                        |                         86707 |                              |        |       |                   
| Q02   | Partition-pruned count, one yellow taxi "month"   |                          3833 |                              |        |       |                   
| Q03   | Yellow Taxi Count For One Partition Month         |                         24707 |                              |        |       |                    
| Q04   | Long And Expensive Yellow Taxi Trips              |                         95229 |                              |        |       |                   
| Q05   | Monthly Yellow Taxi Amount And Distance Summary   |                        159617 |                              |        |       |                   
| Q06   | Full High-Volume FHV Row Count                    |                        787924 |                              |        |       |                    
| Q07   | High-Volume FHV Count For One Partition Month     |                         28321 |                              |        |       |                    
| Q08   | Long And Expensive High-Volume FHV Trips          |                        759776 |                              |        |       |                    
| Q09   | Monthly High-Volume FHV Fare And Distance Summary |                       1224409 |                              |        |       |                    
| Q10   | High-Volume FHV Shared-Ride Flag Distribution     |                        376649 |                              |        |       |                    
