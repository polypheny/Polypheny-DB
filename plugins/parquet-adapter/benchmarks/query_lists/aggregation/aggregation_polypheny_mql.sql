-- Q01: Full Yellow Taxi Row Count
tlcpd_document.tlcpd__yellow_tripdata.aggregate([
  { "$count": "row_count" }
]);

-- Q02: Yellow Taxi Count For One Partition Month
tlcpd_document.tlcpd__yellow_tripdata.aggregate([
  {
    "$match": {
      "year": "2022",
      "month": "10"
    }
  },
  { "$count": "row_count" }
]);

-- Q03: Yellow Taxi Trips On One Day
tlcpd_document.tlcpd__yellow_tripdata.aggregate([
  {
    "$match": {
      "tpep_pickup_datetime": {
        "$gte": "2022-10-15 00:00:00",
        "$lt": "2022-10-16 00:00:00"
      }
    }
  },
  { "$count": "row_count" }
]);

-- Q04: Long And Expensive Yellow Taxi Trips
tlcpd_document.tlcpd__yellow_tripdata.aggregate([
  {
    "$match": {
      "trip_distance": { "$gte": 10.0 },
      "total_amount": { "$gte": 40.0 }
    }
  },
  { "$count": "row_count" }
]);

-- Q05: Yearly Yellow Taxi Amount And Distance Summary
tlcpd_document.tlcpd__yellow_tripdata.aggregate([
  {
    "$group": {
      "_id": "$year",
      "gross_amount": { "$sum": "$total_amount" },
      "min_distance": { "$min": "$trip_distance" },
      "max_distance": { "$max": "$trip_distance" }
    }
  },
  { "$sort": { "_id": 1 } }
]);

-- Q06: Full High-Volume FHV Row Count
tlcpd_document.tlcpd__fhvhv_tripdata.aggregate([
  { "$count": "row_count" }
]);

-- Q07: High-Volume FHV Count For One Partition Month
tlcpd_document.tlcpd__fhvhv_tripdata.aggregate([
  {
    "$match": {
      "year": "2022",
      "month": "10"
    }
  },
  { "$count": "row_count" }
]);

-- Q08: Long And Expensive High-Volume FHV Trips
tlcpd_document.tlcpd__fhvhv_tripdata.aggregate([
  {
    "$match": {
      "trip_miles": { "$gte": 10.0 },
      "base_passenger_fare": { "$gte": 40.0 }
    }
  },
  { "$count": "row_count" }
]);

-- Q09: Yearly High-Volume FHV Fare And Distance Summary
tlcpd_document.tlcpd__fhvhv_tripdata.aggregate([
  {
    "$group": {
      "_id": "$year",
      "passenger_fare": { "$sum": "$base_passenger_fare" },
      "driver_pay": { "$sum": "$driver_pay" },
      "min_miles": { "$min": "$trip_miles" },
      "max_miles": { "$max": "$trip_miles" }
    }
  },
  { "$sort": { "_id": 1 } }
]);

-- Q10: High-Volume FHV Shared-Request Flag Distribution
tlcpd_document.tlcpd__fhvhv_tripdata.aggregate([
  {
    "$match": {
      "year": "2022"
    }
  },
  {
    "$group": {
      "_id": "$shared_request_flag"
    }
  }
]);
