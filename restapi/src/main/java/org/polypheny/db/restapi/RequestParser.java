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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.restapi.models.requests.ResourceRequest;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.validate.SqlValidatorUtil;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import spark.QueryParamsMap;


@Slf4j
public class RequestParser {

    private final Catalog catalog = Catalog.getInstance();
    private final TransactionManager transactionManager;
    private final Authenticator authenticator;
    private final String databaseName;
    private final String userName;

    public RequestParser( final TransactionManager transactionManager, final Authenticator authenticator, final String userName, final String databaseName ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
        this.userName = userName;
        this.databaseName = databaseName;
    }


    public ResourceRequest parseResourceRequest( String resourceName, QueryParamsMap queryParamsMap ) {
        List<CatalogTable> tables = this.parseTableList( resourceName );
        Pair<List<CatalogColumn>, List<String>> projections = this.parseRequestProjections( queryParamsMap );
        Map<CatalogColumn, List<Pair<SqlOperator, Object>>> filters = this.parseRequestFilters( queryParamsMap );

        Integer limit = this.parseLimit( queryParamsMap );
        Integer offset = this.parseOffset( queryParamsMap );

        List<Pair<CatalogColumn, Boolean>> sort = this.parseSorting( queryParamsMap );


        return new ResourceRequest( tables, projections, filters, limit, offset, sort );
    }


    // /res/ parsing

    private List<CatalogTable> parseTableList( String tableList ) {
        log.debug( "Starting to parse table list: {}", tableList );
        String[] tableNameList = tableList.split( "," );

        List<CatalogTable> tables = new ArrayList<>();
        for ( String tableName : tableNameList ) {
            String[] tableElements = tableName.split( "\\." );
            if ( tableElements.length != 2 ) {
                log.warn( "Table name \"{}\" not possible to parse.", tableName );
                return null;
            }

            try {
                tables.add( this.catalog.getTable( this.databaseName, tableElements[0], tableElements[1] ) );
                log.debug( "Added table \"{}\" to table list.", tableName );
            } catch ( UnknownTableException | GenericCatalogException e ) {
                log.error( "Unable to fetch table: {}.", tableName, e );
                return null;
            }
        }

        log.debug( "Finished parsing table list: {}", tableList );
        return tables;
    }


    private Map<CatalogColumn, List<Pair<SqlOperator, Object>>> parseRequestFilters( QueryParamsMap queryParamsMap ) {
        log.debug( "Starting to parse request filters." );
        Map<CatalogColumn, List<Pair<SqlOperator, Object>>> result = new HashMap<>();

        for ( String possibleFilterKey : queryParamsMap.toMap().keySet() ) {
            // Check whether this is a filter or a special term
            // Special terms always start with an underscore ("_")
            if ( possibleFilterKey.startsWith( "_" ) ) {
                log.debug( "Not a filter: {}", possibleFilterKey );
                continue;
            }
            log.debug( "Attempting to parse filters for key: {}.", possibleFilterKey );

            // Make sure we actually have a column
            CatalogColumn catalogColumn;
            try {
                catalogColumn = this.getCatalogColumnFromString( possibleFilterKey );
                log.debug( "Fetched catalog column for filter key: {}", possibleFilterKey );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                e.printStackTrace();
                log.error( "Unable to fetch catalog column for filter key: {}. Returning null.", possibleFilterKey );
                return null;
            }

            List<Pair<SqlOperator, Object>> filterOperators = new ArrayList<>();
            for ( String filterString : queryParamsMap.get( possibleFilterKey ).values() ) {
                filterOperators.add( this.parseFilterOperation( catalogColumn, filterString ) );
            }
            result.put( catalogColumn, filterOperators );
//            result.put( catalogColumn, Arrays.asList( queryParamsMap.get( possibleFilterKey ).values() ) );
            log.debug( "Finished parsing filters for key: {}.", possibleFilterKey );
        }

        return result;
    }

    private Pair<List<CatalogColumn>, List<String>> parseRequestProjections( QueryParamsMap queryParamsMap ) {
        if ( ! queryParamsMap.hasKey( "_project" )) {
            log.debug( "Request does not contain a projection. Returning null." );
            return null;
        }
        QueryParamsMap projectionMap = queryParamsMap.get( "_project" );
        String[] possibleProjectionValues = projectionMap.values();
        String possibleProjectionsString = possibleProjectionValues[0];
        log.debug( "Starting to parse projection: {}", possibleProjectionsString );

//        List<Pair<CatalogColumn, String>> projections = new ArrayList<>();
        String[] possibleProjections = possibleProjectionsString.split( "," );

        List<CatalogColumn> projectionColumns = new ArrayList<>();
        List<String> projectionNames = new ArrayList<>();
        for ( String projectionToParse : possibleProjections ) {
            String[] possiblyNamed = projectionToParse.split( "@" );
            CatalogColumn catalogColumn;
            try {
                catalogColumn = this.getCatalogColumnFromString( possiblyNamed[0] );
                log.debug( "Fetched catalog column for projection key: {}.", possiblyNamed[0] );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                log.warn( "Unable to fetch column: {}.", possiblyNamed[0], e );
                return null;
            }

            projectionColumns.add( catalogColumn );
            if ( possiblyNamed.length == 2 ) {
                log.debug( "Parsed projection. Column: {}, Alias: {}.", possiblyNamed[0], possiblyNamed[1] );
                projectionNames.add( possiblyNamed[1] );
            } else {
                log.debug( "Parsed projection. Column: {}.", possiblyNamed[0] );
                projectionNames.add( catalogColumn.name );
            }
        }

        // TODO js: proper case sensitivity
        projectionNames = SqlValidatorUtil.uniquify( projectionNames, false );

        log.debug( "Finished parsing projection: {}", possibleProjectionsString );
        return new Pair<>( projectionColumns, projectionNames );
    }


    private List<Pair<CatalogColumn, Boolean>> parseSorting( QueryParamsMap queryParamsMap ) {
        if ( ! queryParamsMap.hasKey( "_sort" )) {
            log.debug( "Request does not contain a sort. Returning null." );
            return null;
        }
        QueryParamsMap sortMap = queryParamsMap.get( "_sort" );
        String[] possibleSortValues = sortMap.values();
        String possibleSortString = possibleSortValues[0];
        log.debug( "Starting to parse sort: {}", possibleSortString );

        List<Pair<CatalogColumn, Boolean>> sortingColumns = new ArrayList<>();
        String[] possibleSorts = possibleSortString.split( "," );
        for ( String sortToParse : possibleSorts ) {
            String[] splitUp = sortToParse.split( "@" );
            CatalogColumn catalogColumn;
            try {
                catalogColumn = this.getCatalogColumnFromString( splitUp[0] );
                log.debug( "Fetched catalog column for sort key: {}.", splitUp[0] );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                log.warn( "Unable to fetch column: {}", splitUp[0], e );
                return null;
            }

            if ( splitUp.length == 2 && splitUp[1].equalsIgnoreCase( "desc" ) ) {
                sortingColumns.add( new Pair<>( catalogColumn, true ) );
            } else {
                sortingColumns.add( new Pair<>( catalogColumn, false ) );
            }
        }


        log.debug( "Finished parsing sort: {}", possibleSortString );
        return sortingColumns;
    }


    private Integer parseLimit( QueryParamsMap queryParamsMap ) {
        if ( ! queryParamsMap.hasKey( "_limit" ) ) {
            log.debug( "Request does not contain a limit. Returning -1." );
            return -1;
        }
        QueryParamsMap limitMap = queryParamsMap.get( "_limit" );
        String[] possibleLimitValues = limitMap.values();

        try {
            log.debug( "Parsed limit value: {}.", possibleLimitValues[0] );
            return Integer.valueOf( possibleLimitValues[0] );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse limit value: {}", possibleLimitValues[0] );
            return -1;
        }
    }


    private Integer parseOffset( QueryParamsMap queryParamsMap ) {
        if ( ! queryParamsMap.hasKey( "_offset" ) ) {
            log.debug( "Request does not contain an offset. Returning -1." );
            return -1;
        }
        QueryParamsMap offsetMap = queryParamsMap.get( "_offset" );
        String[] possibleOffsetValues = offsetMap.values();

        try {
            log.debug( "Parsed offset value: {}.", possibleOffsetValues[0] );
            return Integer.valueOf( possibleOffsetValues[0] );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse offset value: {}", possibleOffsetValues[0] );
            return -1;
        }
    }


    private CatalogColumn getCatalogColumnFromString( String name ) throws IllegalArgumentException, GenericCatalogException, UnknownColumnException {
        String[] splitString = name.split( "\\." );
        if ( splitString.length != 3 ) {
            throw new IllegalArgumentException( "Column name is not 3 fields long. Got: " + name );
        }

        return this.catalog.getColumn( this.databaseName, splitString[0], splitString[1], splitString[2] );

    }

    private Pair<SqlOperator, Object> parseFilterOperation( CatalogColumn catalogColumn, String filterString ) {
        log.debug( "Starting to parse filter operation. Column: {}, Value: {}.", catalogColumn.id, filterString );
        SqlOperator callOperator;
        String restOfOp;
        if ( filterString.startsWith( "<" ) ) {
            callOperator = SqlStdOperatorTable.LESS_THAN;
            restOfOp = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "<=" ) ) {
            callOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            restOfOp = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( ">" ) ) {
            callOperator = SqlStdOperatorTable.GREATER_THAN;
            restOfOp = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( ">=" ) ) {
            callOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            restOfOp = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "=" ) ) {
            callOperator = SqlStdOperatorTable.EQUALS;
            restOfOp = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!=" ) ) {
            callOperator = SqlStdOperatorTable.NOT_EQUALS;
            restOfOp = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "%" ) ) {
            callOperator = SqlStdOperatorTable.LIKE;
            restOfOp = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!%" ) ) {
            callOperator = SqlStdOperatorTable.NOT_LIKE;
            restOfOp = filterString.substring( 2, filterString.length() );
        } else {
            log.warn( "Unable to parse filter operation comparator. Returning null." );
            return null;
        }

        Object rightHandSide;
        if ( PolyType.BOOLEAN_TYPES.contains( catalogColumn.type ) ) {
            rightHandSide = Boolean.valueOf( restOfOp );
        } else if ( PolyType.INT_TYPES.contains( catalogColumn.type ) ) {
            rightHandSide = Long.valueOf( restOfOp );
        } else if ( PolyType.NUMERIC_TYPES.contains( catalogColumn.type ) ) {
            rightHandSide = Double.valueOf( restOfOp );
        } else if ( PolyType.CHAR_TYPES.contains( catalogColumn.type ) ) {
            rightHandSide = restOfOp;
        } else {
            // TODO js: error handling.
            log.warn( "Unable to convert literal value for filter operation. Returning null. Column: {}, Value: {}.", catalogColumn.id, filterString );
            return null;
        }

        log.debug( "Finished parsing filter operation. Column: {}, Value: {}.", catalogColumn.id, filterString );
        return new Pair<>( callOperator, rightHandSide );
    }
}
