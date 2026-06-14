# Access Model Comparison Query Specification

| Query | Operation           | Predicate                                          | Returned data                                      | Purpose                                                    |
|-------|---------------------|----------------------------------------------------|----------------------------------------------------|------------------------------------------------------------|
| Q01   | Full scan           | None                                               | All fields                                         | Measures full dataset read performance                     |
| Q02   | Projection          | None                                               | Selected fields                                    | Measures projection behavior                               |
| Q03   | Filtered count      | `trip_distance >= 10.0` and `total_amount >= 40.0` | Aggregate count                                    | Measures filter evaluation without returning matching rows |
| Q04   | Filtered scan       | `trip_distance >= 10.0` and `total_amount >= 40.0` | All matching records or documents                  | Measures filtering with full result consumption            |
| Q05   | Filtered projection | `trip_distance >= 10.0` and `total_amount >= 40.0` | Selected fields from matching records or documents | Measures combined filtering and projection                 |
