# Change Log

## 0.9.0 (April 12, 2023)

* Adding the ability to use sha256 digest for Docker images
* A new -version command line option
* Introduction of Plugin System & Plugin Refactor
* A data source adapter for Excel sheets
* A data source adapter for Google sheets
* Improve handling of file-based sources and allow linking of files
* Fix a naming error in graphs
* Fix an issue with deploying Docker containers from Polypheny
* Improve handling of logical operators on graph properties


## 0.8.1 (November 3, 2022)

* Fixing support for Java 19
* Fix an issue with the status handling for Docker containers that lead to the wrong status of a container being reported
* Extend connection options for Docker containers


## 0.8.0 (October 23, 2022)

* Add support for the labeled property graph data model
* Add support for the cypher query language
* Add a neo4j data store adapter
* Improve cross-model query support
* Improve the document data model
* Improve handling of DML queries containing operations not supported on the underlying data store
* Improve constraint enforcement
* Refactor transaction locking
* Add a remote deployment support for the MongoDB adapter
* Allow specifying colors for information graphs
* Add a logging for the number of monitoring threads
* Option to select a time interval for the graph in the UI dashboard


## 0.7.0 (February 22, 2022)

* Initial release
