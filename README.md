<p align="center">
<picture>
  <source 
    srcset="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-white-text.png" 
    media="(prefers-color-scheme: dark)">
  <a href="https://polypheny.org/">
    <img align="center" width="300" height="300" src="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-transparent.png">
   </a>
</picture>
</p>

# Polypheny-DB

_Polypheny-DB_ is a self-adaptive Polystore that provides cost- and workload aware access to heterogeneous data. As a Polystore, Polypheny-DB seamlessly combines different underlying data storage engines to provide good query performance independent of the type of workload.

If you want to learn more about Polypheny-DB, we recommend having a look on our [vision paper](https://ieeexplore.ieee.org/document/8622353).

## Getting Started ##
The easiest way to setup Polypheny-DB is to use a [release](https://github.com/polypheny/Polypheny-DB/releases/latest). Alternatively, you can use [Polypheny Control](https://github.com/polypheny/Polypheny-Control) to automatically build Polypheny-DB.

## Roadmap ##
See the [open issues](https://github.com/polypheny/Polypheny-DB/issues) for a list of proposed features (and known issues).

## Contributing ##
We highly welcome your contributions to Polypheny-DB. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.

## Credits ##
_Polypheny-DB_ builds upon the great work of several other projects:

* [Apache Avatica](https://calcite.apache.org/avatica/): A framework for building database drivers
* [Apache Calcite](https://calcite.apache.org/): A framework for building databases
* [HSQLDB](http://hsqldb.org/): A relational database written in Java
* [JavaCC](https://javacc.org/): A parser generator
* [Java Spark](http://sparkjava.com/): A framework for building web services
* [Project Lombok](https://projectlombok.org/): A library which provides annotations for tedious tasks

Except for the first two, those projects are used "as is" and integrated as a library. _Apache Avatica_ we [forked](https://github.com/polypheny/Avatica) and made some Polypheny-DB specific adjustments. From _Apache Calcite_ we use parts of the code as foundation for Polypheny-DB.

## Acknowledgements
The Polypheny-DB project is supported by the Swiss National Science Foundation (SNSF) under the contract no. 200021_172763.

## License ##
The Apache 2.0 License
