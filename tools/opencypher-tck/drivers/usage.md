# Native driver adapters

The runner starts one adapter process per matrix entry. Each process keeps a
Prism connection open and requests `CypherResultFormat=relational` to preserve
columns, row boundaries, typed graph values, and graph identity. The adapters
send Cypher statements and named bindings via their own driver APIs. Set
`M23_HOST`, `M23_PORT`, `M23_USERNAME`, and `M23_PASSWORD` in the environment.
Use an isolated Polypheny instance with a single writable store. The tests
create and drop graph namespaces.

| Driver | Release base | Source for this matrix | Prism | Adapter prerequisites |
| --- | --- | --- | --- | --- |
| Python | [2.2.0](https://github.com/polypheny/Polypheny-Connector-Python/releases/tag/v2.2.0) | [Python #7](https://github.com/polypheny/Polypheny-Connector-Python/pull/7), exact revision in the run metadata | 2.4 | Python 3.9+, installed Polypheny package and its dependencies |
| Go | [0.3.0](https://github.com/polypheny/Polypheny-Go-Driver/releases/tag/v0.3.0) | [89ac731](https://github.com/polypheny/Polypheny-Go-Driver/commit/89ac7319331999437dac16f1aa34daa83126b75a) | 2.4 | Go 1.23+ and a checkout of the named-parameter changes |
| JDBC | [3.2](https://github.com/polypheny/Polypheny-JDBC-Driver/releases/tag/v3.2) | [3.3 candidate c2226ff](https://github.com/polypheny/Polypheny-JDBC-Driver/commit/c2226ff46f99cbb622eb769fb75db2261d45fd5d) from [JDBC #25](https://github.com/polypheny/Polypheny-JDBC-Driver/pull/25) | 2.4 | JDK 17+ for the adapter, JDBC shadow jar, and [Gson 2.11.0](https://repo.maven.apache.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar) |
| .NET | [2.2.0](https://github.com/polypheny/Polypheny-DotNet-Driver/releases/tag/v2.2.0) | [7e5e89e](https://github.com/polypheny/Polypheny-DotNet-Driver/commit/7e5e89e450be20d06a9697659ce8673ed909d4d5) | 2.4 | .NET 8 SDK and a checkout of the named-parameter changes |
| C++ | [2.1.0](https://github.com/polypheny/Polypheny-CPP-Driver/releases/tag/v2.1.0) | [f0dc47a](https://github.com/polypheny/Polypheny-CPP-Driver/commit/f0dc47a50eed9a51055e7d9166505073b4bc6ea4) | 2.4 | C++17, Meson, Ninja, matching Protobuf compiler and library, OpenSSL, GMP, Catch2 and [JSON for Modern C++ 3.11.3](https://github.com/nlohmann/json/releases/tag/v3.11.3) |
| PHP | [1.2.0](https://github.com/polypheny/Polypheny-PHP-Driver/releases/tag/v1.2.0) | [777f635](https://github.com/polypheny/Polypheny-PHP-Driver/commit/777f635a82c9416bd35b9f92583c3bc03c697fb3) | 2.4 | 64-bit PHP 8.2+, Composer install in the driver checkout |

JDBC 3.2 can submit the same prepared queries and decode the same graph rows.
Its Prism handshake advertises 2.3, which means structured server error
details are unavailable. The strict matrix uses the JDBC 3.3 candidate to check
server errors through Prism 2.4. The [MATLAB repository](https://github.com/polypheny/Matlab-Connector/tree/27a279a8b41a5210c8a3d8bc3c01c0a4a0559ab5)
contains a README, license, and `.gitignore`. Its initial commit supplies no
driver to execute. The legacy Polypheny Client uses JDBC and the Polypheny
IPython extension uses the HTTP interface.

## Build and run

Run from the root of the Polypheny-DB checkout. Build paths and the driver
checkout paths below must be absolute. Pass the resulting command to the
runner's `--driver-command` argument. The Python adapter is the default.

### Go

```sh
adapter_dir="$PWD/tools/opencypher-tck/drivers/go"
module_dir=$(mktemp -d)
cp "$adapter_dir/go.mod" "$adapter_dir/go.sum" "$module_dir"
(
    cd "$adapter_dir"
    go mod edit -modfile "$module_dir/go.mod" \
        -replace "github.com/polypheny/Polypheny-Go-Driver=$M23_GO_DRIVER"
    go build -modfile "$module_dir/go.mod" -o "$M23_GO_ADAPTER" .
)
```

Use `"$M23_GO_ADAPTER"` as the driver command.

### JDBC

Build the JDBC 3.3 candidate with `./gradlew -PdisableToolchain=true shadowJar`
in the JDBC checkout and set `M23_JDBC_JAR` to that jar.

```sh
java -cp "$M23_JDBC_JAR:$M23_GSON_JAR" tools/opencypher-tck/drivers/Jdbc.java
```

The released JDBC 3.2 jar's SHA-256 is
`394b1a3b1fec6ccabf8cabb3578017ceabd2cde6e0eda7c81918b9973f8e51c8`.

### .NET

```sh
M23_DOTNET_DRIVER="$M23_DOTNET_DRIVER" dotnet build \
    tools/opencypher-tck/drivers/dotnet/TckDriver.csproj --output "$M23_DOTNET_BUILD"
dotnet "$M23_DOTNET_BUILD/TckDriver.dll"
```

### C++

```sh
meson setup "$M23_CPP_BUILD" "$M23_CPP_DRIVER" -Dprism_streaming=false
meson compile -C "$M23_CPP_BUILD"
c++ -std=c++17 -I"$M23_CPP_DRIVER/src" -I"$M23_CPP_DRIVER/include" \
    -I"$M23_CPP_BUILD" -I"$M23_JSON_INCLUDE" \
    -I"$M23_CPP_BUILD/subprojects/polypheny_prism_api-2.4/libpolypheny_prism_api.a.p" \
    $(pkg-config --cflags protobuf) tools/opencypher-tck/drivers/cpp.cpp \
    -Wl,-rpath,"$M23_CPP_BUILD" -L"$M23_CPP_BUILD" -lpolypheny_cpp_library \
    $(pkg-config --libs protobuf) -lgmpxx -lgmp -o "$M23_CPP_BUILD/tck-driver"
```

`M23_JSON_INCLUDE` is the directory containing `nlohmann/json.hpp`.
Use `"$M23_CPP_BUILD/tck-driver"` as the driver command. When using a Linux
container on Docker Desktop, pass `M23_HOST=host.docker.internal` to the
container and keep stdin open.

### PHP

```sh
export M23_PHP_AUTOLOAD="$M23_PHP_DRIVER/vendor/autoload.php"
php tools/opencypher-tck/drivers/php.php
```
