/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.restapi;


import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.restapi.exception.IllegalColumnException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import spark.QueryParamsMap;
import spark.Request;


@Slf4j
public class RequestParser {
    private final Catalog catalog;
    private final TransactionManager transactionManager;
    private final Authenticator authenticator;
    private final String databaseName;
    private final String userName;


    private final static Pattern PROJECTION_ENTRY_PATTERN = Pattern.compile(
            "^(?<column>[a-zA-Z]\\w*\\.[a-zA-Z]\\w*\\.[a-zA-Z]\\w*)(?:@(?<alias>[a-zA-Z]\\w*)(?:\\((?<agg>[A-Z]+)\\))?)?$" );

    private final static Pattern SORTING_ENTRY_PATTERN = Pattern.compile(
            "^(?<column>[a-zA-Z]\\w*(?:\\.[a-zA-Z]\\w*\\.[a-zA-Z]\\w*)?)(?:@(?<dir>ASC|DESC))?$" );


    public RequestParser( final TransactionManager transactionManager, final Authenticator authenticator, final String databaseName, final String userName ) {
        this( Catalog.getInstance(), transactionManager, authenticator, userName, databaseName );
    }


    @VisibleForTesting
    RequestParser( Catalog catalog, TransactionManager transactionManager, Authenticator authenticator, String databaseName, String userName ) {
        this.catalog = catalog;
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
        this.databaseName = databaseName;
        this.userName = userName;
    }


    /**
     * Parses and authenticates the Basic Authorization for a request.
     *
     * @param request the request
     * @return the authorized user
     * @throws UnauthorizedAccessException thrown if no authorization provided or invalid credentials
     */
    public CatalogUser parseBasicAuthentication( Request request ) throws UnauthorizedAccessException {
        if ( request.headers( "Authorization" ) == null ) {
            log.debug( "No Authorization header for request id: {}.", request.session().id() );
            throw new UnauthorizedAccessException( "No Basic Authorization sent by user." );
        }

        final String basicAuthHeader = request.headers( "Authorization" );

        final Pair<String, String> decoded = decodeBasicAuthorization( basicAuthHeader );

        try {
            return this.authenticator.authenticate( decoded.left, decoded.right );
        } catch ( AuthenticationException e ) {
            log.info( "Unable to authenticate user for request id: {}.", request.session().id(), e );
            throw new UnauthorizedAccessException( "Not authorized." );
        }
    }


    @VisibleForTesting
    static Pair<String, String> decodeBasicAuthorization( String encodedAuthorization ) {
        if ( ! Base64.isBase64( encodedAuthorization ) ) {
            throw new UnauthorizedAccessException( "Basic Authorization header is not properly encoded." );
        }
        final String encodedHeader = StringUtils.substringAfter( encodedAuthorization, "Basic" );
        final String decodedHeader = new String( Base64.decodeBase64( encodedHeader ) );
        final String[] decoded = StringUtils.splitPreserveAllTokens( decodedHeader, ":" );
        return new Pair<>( decoded[0], decoded[1] );
    }


    /**
     * Parse the list of tables for a request.
     *
     * @param tableList list of tables
     * @return parsed list of tables
     * @throws IllegalArgumentException thrown if unable to parse table list
     */
    public List<CatalogTable> parseTables( String tableList ) throws IllegalArgumentException {
        log.debug( "Starting to parse table list: {}", tableList );
        String[] tableNameList = tableList.split( "," );

        List<CatalogTable> tables = new ArrayList<>();
        for ( String tableName : tableNameList ) {
            CatalogTable temp = this.parseCatalogTableName( tableName );
            if ( temp != null ) {
                tables.add( temp );
                log.debug( "Added table \"{}\" to table list.", tableName );
            } else {
//                log.error( "Unable to fetch table: {}.", tableName, e );
                throw new IllegalArgumentException( "Improperly formated resources list." );
            }
        }

        log.debug( "Finished parsing table list: {}", tableList );
        return tables;
    }


    /**
     * Parse individual table name.
     *
     * @param tableName table name
     * @return parsed table name
     * @throws IllegalArgumentException thrown if unable to parse table name
     */
    @VisibleForTesting
    CatalogTable parseCatalogTableName( String tableName ) throws IllegalArgumentException {
        String[] tableElements = tableName.split( "\\." );
        if ( tableElements.length != 2 ) {
            log.warn( "Table name \"{}\" not possible to parse.", tableName );
            throw new IllegalArgumentException( "Improperly formated resources list." );
        }

        try {
            CatalogTable table = this.catalog.getTable( this.databaseName, tableElements[0], tableElements[1] );
            log.debug( "Finished parsing table \"{}\".", tableName );
            return table;
        } catch ( UnknownTableException | GenericCatalogException e ) {
            log.error( "Unable to fetch table: {}.", tableName, e );
            throw new IllegalArgumentException( "Improperly formatted resources list." );
        }
    }


    public ProjectionAndAggregation parseProjectionsAndAggregations( Request request ) {
        if ( ! request.queryMap().hasKey( "_project" ) ) {
            // FIXME: how to deal with these?
            return null;
        }
        QueryParamsMap projectionMap = request.queryMap().get( "_project" );
        String[] possibleProjectionValues = projectionMap.values();
        String possibleProjectionsString = possibleProjectionValues[0];
        log.debug( "Starting to parse projection: {}", possibleProjectionsString );

        String[] possibleProjections = possibleProjectionsString.split( "," );

        List<CatalogColumn> projectionColumns = new ArrayList<>();
        List<String> projectionNames = new ArrayList<>();
        List<Pair<CatalogColumn, SqlAggFunction>> aggregates = new ArrayList<>();
//        List<CatalogColumn> aggregateColumns = new ArrayList<>();
//        List<SqlAggFunction> aggregateFunctions = new ArrayList<>();

        for ( String projectionToParse : possibleProjections ) {
            Matcher matcher = PROJECTION_ENTRY_PATTERN.matcher( projectionToParse );
            if ( matcher.find() ) {
                String columnName = matcher.group( "column" );
                CatalogColumn catalogColumn;

                try {
                    catalogColumn = this.getCatalogColumnFromString( columnName );
                    log.debug( "Fetched catalog column for projection key: {}.", columnName );
                } catch ( GenericCatalogException | UnknownColumnException e ) {
                    log.warn( "Unable to fetch column: {}.", columnName, e );
                    return null; // FIXME
                }

                projectionColumns.add( catalogColumn );

                if ( matcher.group( "alias" ) == null ) {
                    // We only have a qualified name
                    projectionNames.add( columnName );
                } else {
                    projectionNames.add( matcher.group( "alias" ) );

                    if ( matcher.group( "agg" ) != null ) {
                        aggregates.add( new Pair<>( catalogColumn, this.decodeAggregateFunction( matcher.group( "agg" ) ) ) );
//                        aggregateColumns.add( catalogColumn );
//                        aggregateFunctions.add( this.decodeAggregateFunction( matcher.group( "agg" ) ) );
                    }
                }
            } else {
                // FIXME: Proper error handling
            }
        }



        return new ProjectionAndAggregation( new Pair<>( projectionColumns, projectionNames ), aggregates);
    }


    @VisibleForTesting
    SqlAggFunction decodeAggregateFunction( String function ) {
        switch ( function ) {
            case "COUNT":
                return SqlStdOperatorTable.COUNT;
            case "MAX":
                return SqlStdOperatorTable.MAX;
            case "MIN":
                return SqlStdOperatorTable.MIN;
            case "AVG":
                return SqlStdOperatorTable.AVG;
            case "SUM":
                return SqlStdOperatorTable.SUM;
            default:
                return null;
        }
    }


    private CatalogColumn getCatalogColumnFromString( String name ) throws IllegalArgumentException, GenericCatalogException, UnknownColumnException {
        String[] splitString = name.split( "\\." );
        if ( splitString.length != 3 ) {
            throw new IllegalArgumentException( "Column name is not 3 fields long. Got: " + name );
        }

        return this.catalog.getColumn( this.databaseName, splitString[0], splitString[1], splitString[2] );

    }


    public List<Pair<CatalogColumn, Boolean>> parseSorting( Request request, Map<String, CatalogColumn> nameAndAliasMapping ) throws IllegalArgumentException {
        if ( ! request.queryMap().hasKey( "_sort" ) ) {
            log.debug( "Request does not contain a sort. Returning null." );
            return null;
        }

        QueryParamsMap sortMap = request.queryMap().get( "_sort" );
        String[] possibleSortValues = sortMap.values();
        String possibleSortString = possibleSortValues[0];
        log.debug( "Starting to parse sort: {}", possibleSortString );

        List<Pair<CatalogColumn, Boolean>> sortingColumns = new ArrayList<>();
        String[] possibleSorts = possibleSortString.split( "," );

        for ( String sortEntry : possibleSorts ) {
            Matcher matcher = SORTING_ENTRY_PATTERN.matcher( sortEntry );

            if ( matcher.find() ) {
                CatalogColumn catalogColumn = nameAndAliasMapping.get( matcher.group( "column" ) );
                boolean inverted = false;
                if ( matcher.group( "dir" ) != null ) {
                    String direction = matcher.group( "dir" );
                    inverted = "DESC".equals( direction );
                }

                sortingColumns.add( new Pair<>( catalogColumn, inverted ) );

            } else {
                // FIXME: Proper error handling.
                throw new IllegalArgumentException( "The following provided sort instruction is not proper: '" + sortEntry + "'." );
            }
        }

        return sortingColumns;
    }


    public List<CatalogColumn> parseGroupings( Request request, Map<String, CatalogColumn> nameAndAliasMapping ) throws IllegalArgumentException {
        if ( ! request.queryMap().hasKey( "_groupby" ) ) {
            log.debug( "Request does not contain a grouping. Returning null." );
            return new ArrayList<>();
        }

        QueryParamsMap groupbyMap = request.queryMap().get( "_groupby" );
        String[] possibleGroupbyValues = groupbyMap.values();
        String possibleGroupbyString = possibleGroupbyValues[0];
        log.debug( "Starting to parse grouping: {}", possibleGroupbyString );

        List<CatalogColumn> groupingColumns = new ArrayList<>();
        String[] possibleGroupings = possibleGroupbyString.split( "," );
        for ( String groupbyEntry : possibleGroupings ) {
            if ( nameAndAliasMapping.containsKey( groupbyEntry ) ) {
                groupingColumns.add( nameAndAliasMapping.get( groupbyEntry ) );
            } else {
                throw new IllegalArgumentException( "The following provided groupby instruction is not proper: '" + groupbyEntry + "'." );
            }
        }

        return groupingColumns;
    }


    public Integer parseLimit( Request request ) {
        if ( ! request.queryMap().hasKey( "_limit" ) ) {
            log.debug( "Request does not contain a limit. Returning -1." );
            return -1;
        }
        QueryParamsMap limitMap = request.queryMap().get( "_limit" );
        String[] possibleLimitValues = limitMap.values();

        try {
            log.debug( "Parsed limit value: {}.", possibleLimitValues[0] );
            return Integer.valueOf( possibleLimitValues[0] );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse limit value: {}", possibleLimitValues[0] );
            return -1;
        }
    }


    public Integer parseOffset( Request request ) {
        if ( ! request.queryMap().hasKey( "_offset" ) ) {
            log.debug( "Request does not contain an offset. Returning -1." );
            return -1;
        }
        QueryParamsMap offsetMap = request.queryMap().get( "_offset" );
        String[] possibleOffsetValues = offsetMap.values();

        return this.parseOffset( possibleOffsetValues[0] );
    }

    @VisibleForTesting
    Integer parseOffset( String offsetString ) {
        try {
            log.debug( "Parsed offset value: {}.", offsetString );
            return Integer.valueOf( offsetString );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse offset value: {}", offsetString );
            return -1;
        }
    }


    public Filters parseFilters( Request request, Map<String, CatalogColumn> nameAndAliasMapping ) {
        Map<CatalogColumn, List<Pair<SqlOperator, Object>>> literalFilters = new HashMap<>();
        Map<CatalogColumn, List<Pair<SqlOperator, CatalogColumn>>> columnFilters = new HashMap<>();

        for ( String possibleFilterKey : request.queryMap().toMap().keySet() ) {
            if ( possibleFilterKey.startsWith( "_" ) ) {
                log.debug( "Not a filter: {}", possibleFilterKey );
                continue;
            }
            log.debug( "Attempting to parse filters for key: {}.", possibleFilterKey );

            if ( ! nameAndAliasMapping.containsKey( possibleFilterKey ) ) {
                throw new IllegalArgumentException( "Not a valid column for filtering: '" + possibleFilterKey + "'." );
            }

            CatalogColumn catalogColumn = nameAndAliasMapping.get( possibleFilterKey );

            List<Pair<SqlOperator, Object>> literalFilterOperators = new ArrayList<>();
            List<Pair<SqlOperator, CatalogColumn>> columnFilterOperators = new ArrayList<>();

            for ( String filterString : request.queryMap().get( possibleFilterKey ).values() ) {
                Pair<SqlOperator, String> rightHandSide = this.parseFilterOperation( filterString );
                Object literal = this.parseLiteralValue( catalogColumn.type, rightHandSide.right );
                // TODO: add column filters here
                literalFilterOperators.add( new Pair<>( rightHandSide.left, literal ) );
            }
            // TODO: Add If Size != 0 checks for both put
            if ( ! literalFilterOperators.isEmpty() ) {
                literalFilters.put( catalogColumn, literalFilterOperators );
            }
            if ( ! columnFilterOperators.isEmpty() ) {
                columnFilters.put( catalogColumn, columnFilterOperators );
            }
            log.debug( "Finished parsing filters for key: {}.", possibleFilterKey );
        }

        return new Filters( literalFilters, columnFilters );
    }




    Pair<SqlOperator, String> parseFilterOperation( String filterString ) throws IllegalArgumentException {
        log.debug( "Starting to parse filter operation. Value: {}.", filterString );

        SqlOperator callOperator;
        String rightHandSide;
        if ( filterString.startsWith( "<" ) ) {
            callOperator = SqlStdOperatorTable.LESS_THAN;
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "<=" ) ) {
            callOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( ">" ) ) {
            callOperator = SqlStdOperatorTable.GREATER_THAN;
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( ">=" ) ) {
            callOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "=" ) ) {
            callOperator = SqlStdOperatorTable.EQUALS;
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!=" ) ) {
            callOperator = SqlStdOperatorTable.NOT_EQUALS;
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "%" ) ) {
            callOperator = SqlStdOperatorTable.LIKE;
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!%" ) ) {
            callOperator = SqlStdOperatorTable.NOT_LIKE;
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else {
            log.warn( "Unable to parse filter operation comparator. Returning null." );
            throw new IllegalArgumentException( "Not a valid filter statement: '" + filterString + "'." );
        }

        return new Pair<>( callOperator, rightHandSide );
    }

    // TODO: REWRITE THIS METHOD
    @VisibleForTesting
    Object parseLiteralValue( PolyType type, Object objectLiteral ) {
        if ( ! ( objectLiteral instanceof String ) ) {
            return objectLiteral;
        } else {
            Object parsedLiteral;
            String literal = (String) objectLiteral;
            if ( PolyType.BOOLEAN_TYPES.contains( type ) ) {
                parsedLiteral = Boolean.valueOf( literal );
            } else if ( PolyType.INT_TYPES.contains( type ) ) {
                parsedLiteral = Long.valueOf( literal );
            } else if ( PolyType.NUMERIC_TYPES.contains( type ) ) {
                parsedLiteral = Double.valueOf( literal );
            } else if ( PolyType.CHAR_TYPES.contains( type ) ) {
                parsedLiteral = literal;
            } else if ( PolyType.DATETIME_TYPES.contains( type ) ) {
                switch ( type ) {
                    case DATE:
                        DateString dateString = new DateString( literal );
                        parsedLiteral = dateString;
                        break;
                    case TIMESTAMP:
                        Instant instant = LocalDateTime.parse( literal ).toInstant( ZoneOffset.UTC );
                        Long millisecondsSinceEpoch = instant.getEpochSecond() * 1000L + instant.getNano() / 1000000L;
                        TimestampString timestampString = TimestampString.fromMillisSinceEpoch( millisecondsSinceEpoch );
                        parsedLiteral = timestampString;
                        break;
                    case TIME:
                        TimeString timeString = new TimeString( literal );
                        parsedLiteral = timeString;
                        break;
                    default:
                        return null;
                }
            } else {
                // TODO js: error handling.
                log.warn( "Unable to convert literal value. Returning null. Type: {}, Value: {}.", type, literal );
                return null;
            }

            return parsedLiteral;
        }
    }


    public List<List<Pair<CatalogColumn, Object>>> parseValues( Request request, Map<String, CatalogColumn> nameAndAliasMapping, Gson gson ) {
        // FIXME: Verify stuff like applications/json, so on, so forth
        Object bodyObject = gson.fromJson( request.body(), Object.class );
        Map bodyMap = (Map) bodyObject;
        List valuesList = (List) bodyMap.get( "data" );
        return this.parseInsertStatementBody( valuesList, nameAndAliasMapping );
    }

    @VisibleForTesting
    List<List<Pair<CatalogColumn, Object>>> parseInsertStatementBody( List<Object> bodyInsertValues, Map<String, CatalogColumn> nameAndAliasMapping ) {
        List<List<Pair<CatalogColumn, Object>>> returnValue = new ArrayList<>();

        for ( Object rowObject : bodyInsertValues ) {
            Map rowMap = (Map) rowObject;
            List<Pair<CatalogColumn, Object>> rowValue = this.parseInsertStatementValues( rowMap, nameAndAliasMapping );
            returnValue.add( rowValue );
        }

        return returnValue;
    }

    private List<Pair<CatalogColumn, Object>> parseInsertStatementValues( Map rowValuesMap, Map<String, CatalogColumn> nameAndAliasMapping ) {
        List<Pair<CatalogColumn, Object>> result = new ArrayList<>();

        for ( Object objectColumnName : rowValuesMap.keySet() ) {
            String stringColumnName = (String) objectColumnName;
            /*if ( possibleValue.startsWith( "_" ) ) {
                log.debug( "FIX THIS MESSAGE: {}", possibleValue );
                continue;
            }*/

            // Make sure we actually have a column
            CatalogColumn catalogColumn;
            try {
                catalogColumn = this.getCatalogColumnFromString( stringColumnName );
                log.debug( "Fetched catalog column for filter key: {}", stringColumnName );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                log.error( "Unable to fetch catalog column for filter key: {}. Returning null.", stringColumnName, e );
                return null;
            }

            Object litVal = rowValuesMap.get( objectColumnName );
            Object parsedValue = this.parseLiteralValue( catalogColumn.type, litVal );
            result.add( new Pair<>( catalogColumn, parsedValue ) );
        }

        result.sort( ( p1, p2 ) -> p1.left.position - p2.left.position );

        return result;
    }


    public Map<String, CatalogColumn> generateNameMapping( List<CatalogTable> tables ) {
        Map<String, CatalogColumn> nameMapping = new HashMap<>();
        for ( CatalogTable table : tables ) {
            for ( CatalogColumn column : this.catalog.getColumns( table.id ) ) {
                nameMapping.put(column.schemaName + "." + column.tableName + "." + column.name, column);
            }
        }

        return nameMapping;
    }


    @AllArgsConstructor
    public static class ProjectionAndAggregation {
        public final Pair<List<CatalogColumn>, List<String>> projection;
        public final List<Pair<CatalogColumn, SqlAggFunction>> aggregateFunctions;
    }

    @AllArgsConstructor
    public static class Filters {
        public final Map<CatalogColumn, List<Pair<SqlOperator, Object>>> literalFilters;
        public final Map<CatalogColumn, List<Pair<SqlOperator, CatalogColumn>>> columnFilters;
    }
}
