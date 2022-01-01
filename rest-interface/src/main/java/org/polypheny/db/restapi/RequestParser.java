/*
 * Copyright 2019-2021 The Polypheny Project
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
import io.javalin.http.Context;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.restapi.exception.ParserException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.restapi.models.requests.ResourceDeleteRequest;
import org.polypheny.db.restapi.models.requests.ResourceGetRequest;
import org.polypheny.db.restapi.models.requests.ResourcePatchRequest;
import org.polypheny.db.restapi.models.requests.ResourcePostRequest;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


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


    public RequestParser( final TransactionManager transactionManager, final Authenticator authenticator, final String userName, final String databaseName ) {
        this( Catalog.getInstance(), transactionManager, authenticator, userName, databaseName );
    }


    @VisibleForTesting
    RequestParser( Catalog catalog, TransactionManager transactionManager, Authenticator authenticator, String userName, String databaseName ) {
        this.catalog = catalog;
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
        this.userName = userName;
        this.databaseName = databaseName;
    }


    /**
     * Parses and authenticates the Basic Authorization for a request.
     *
     * @return the authorized user
     * @throws UnauthorizedAccessException thrown if no authorization provided or invalid credentials
     */
    public CatalogUser parseBasicAuthentication( Context ctx ) throws UnauthorizedAccessException {
        if ( ctx.req.getHeader( "Authorization" ) == null ) {
            log.debug( "No Authorization header for request id: {}.", ctx.req.getSession().getId() );
            throw new UnauthorizedAccessException( "No Basic Authorization sent by user." );
        }

        final String basicAuthHeader = ctx.req.getHeader( "Authorization" );

        final Pair<String, String> decoded = decodeBasicAuthorization( basicAuthHeader );

        try {
            return this.authenticator.authenticate( decoded.left, decoded.right );
        } catch ( AuthenticationException e ) {
            log.info( "Unable to authenticate user for request id: {}.", ctx.sessionAttribute( "id" ), e );
            throw new UnauthorizedAccessException( "Not authorized." );
        }
    }


    @VisibleForTesting
    static Pair<String, String> decodeBasicAuthorization( String encodedAuthorization ) {
        if ( !Base64.isBase64( encodedAuthorization ) ) {
            throw new UnauthorizedAccessException( "Basic Authorization header is not properly encoded." );
        }
        final String encodedHeader = StringUtils.substringAfter( encodedAuthorization, "Basic" );
        final String decodedHeader = new String( Base64.decodeBase64( encodedHeader ) );
        final String[] decoded = StringUtils.splitPreserveAllTokens( decodedHeader, ":" );
        return new Pair<>( decoded[0], decoded[1] );
    }


    public ResourceGetRequest parseGetResourceRequest( HttpServletRequest req, String resourceName ) throws ParserException {

        List<CatalogTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( req ), tables );

        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );

        List<RequestColumn> groupings = this.parseGroupings( req, nameMapping );

        int limit = this.parseLimit( req );
        int offset = this.parseOffset( req );

        List<Pair<RequestColumn, Boolean>> sorting = this.parseSorting( req, nameMapping );

        Filters filters = this.parseFilters( getFilterMap( req ), nameMapping );

        return new ResourceGetRequest( tables, requestColumns, nameMapping, groupings, limit, offset, sorting, filters );
    }


    public ResourcePostRequest parsePostResourceRequest( Context ctx, String resourceName, Gson gson ) throws ParserException {
        List<CatalogTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( ctx.req ), tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        List<List<Pair<RequestColumn, Object>>> values = this.parseValues( ctx, gson, nameMapping );

        return new ResourcePostRequest( tables, requestColumns, nameMapping, values, false );
    }


    public ResourcePostRequest parsePostMultipartRequest( String resourceName, String[] projections, List<Object> insertValues ) throws ParserException {
        List<CatalogTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( projections, tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        List<List<Pair<RequestColumn, Object>>> values = parseInsertStatementBody( insertValues, nameMapping );

        return new ResourcePostRequest( tables, requestColumns, nameMapping, values, true );
    }


    public ResourcePatchRequest parsePatchResourceRequest( Context ctx, String resourceName, Gson gson ) throws ParserException {
        // TODO js: make sure it's only a single resource
        List<CatalogTable> tables = this.parseTables( resourceName );
        // TODO js: make sure there are no actual projections
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( ctx.req ), tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );

        Filters filters = this.parseFilters( getFilterMap( ctx.req ), nameMapping );

        List<List<Pair<RequestColumn, Object>>> values = this.parseValues( ctx, gson, nameMapping );

        return new ResourcePatchRequest( tables, requestColumns, values, nameMapping, filters, false );
    }


    public ResourcePatchRequest parsePatchMultipartRequest( String resourceName, String[] projections, Map<String, String[]> filterMap, List<Object> insertValues ) {
        List<CatalogTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( projections, tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        Filters filters = this.parseFilters( filterMap, nameMapping );
        List<List<Pair<RequestColumn, Object>>> values = parseInsertStatementBody( insertValues, nameMapping );
        return new ResourcePatchRequest( tables, requestColumns, values, nameMapping, filters, true );
    }


    public ResourceDeleteRequest parseDeleteResourceRequest( HttpServletRequest request, String resourceName ) throws ParserException {
        // TODO js: make sure it's only a single resource
        List<CatalogTable> tables = this.parseTables( resourceName );

        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( request ), tables );

        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );

        Filters filters = this.parseFilters( getFilterMap( request ), nameMapping );

        return new ResourceDeleteRequest( tables, requestColumns, nameMapping, filters );
    }


    /**
     * Parse the list of tables for a request.
     *
     * @param tableList list of tables
     * @return parsed list of tables
     * @throws ParserException thrown if unable to parse table list
     */
    @VisibleForTesting
    List<CatalogTable> parseTables( String tableList ) throws ParserException {
        log.debug( "Starting to parse table list: {}", tableList );
        if ( tableList == null ) {
            throw new ParserException( ParserErrorCode.TABLE_LIST_GENERIC, "null" );
        }
        String[] tableNameList = tableList.split( "," );

        List<CatalogTable> tables = new ArrayList<>();
        for ( String tableName : tableNameList ) {
            CatalogTable temp = this.parseCatalogTableName( tableName );
            tables.add( temp );
            log.debug( "Added table \"{}\" to table list.", tableName );
        }

        log.debug( "Finished parsing table list: {}", tableList );
        return tables;
    }


    /**
     * Parse individual table name.
     *
     * @param tableName table name
     * @return parsed table name
     * @throws ParserException thrown if unable to parse table name
     */
    @VisibleForTesting
    CatalogTable parseCatalogTableName( String tableName ) throws ParserException {
        String[] tableElements = tableName.split( "\\." );
        if ( tableElements.length != 2 ) {
            log.warn( "Table name \"{}\" not possible to parse.", tableName );
            throw new ParserException( ParserErrorCode.TABLE_LIST_MALFORMED_TABLE, tableName );
        }

        try {
            CatalogTable table = this.catalog.getTable( this.databaseName, tableElements[0], tableElements[1] );
            if ( log.isDebugEnabled() ) {
                log.debug( "Finished parsing table \"{}\".", tableName );
            }
            return table;
        } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
            log.error( "Unable to fetch table: {}.", tableName, e );
            throw new ParserException( ParserErrorCode.TABLE_LIST_UNKNOWN_TABLE, tableName );
        }
    }


    @VisibleForTesting
    List<RequestColumn> newParseProjectionsAndAggregations( String[] possibleProjectionValues, List<CatalogTable> tables ) throws ParserException {
        // Helper structures & data
        Map<Long, Integer> tableOffsets = new HashMap<>();
        Set<Long> validColumns = new HashSet<>();
        int columnOffset = 0;
        for ( CatalogTable table : tables ) {
            tableOffsets.put( table.id, columnOffset );
            validColumns.addAll( table.columnIds );
            columnOffset += table.columnIds.size();
        }

        List<RequestColumn> columns;
        if ( possibleProjectionValues == null ) {
            columns = this.generateRequestColumnsWithoutProject( tables, tableOffsets );
        } else {
            String possibleProjectionsString = possibleProjectionValues[0];
            columns = this.generateRequestColumnsWithProject( possibleProjectionsString, tableOffsets, validColumns );
        }

        return columns;
    }


    private String[] getProjectionsValues( HttpServletRequest request ) {
        if ( !request.getParameterMap().containsKey( "_project" ) ) {
            return null;
        }
        return request.getParameterMap().get( "_project" );
    }


    @VisibleForTesting
    List<RequestColumn> generateRequestColumnsWithoutProject( List<CatalogTable> tables, Map<Long, Integer> tableOffsets ) {
        List<RequestColumn> columns = new ArrayList<>();
        long internalPosition = 0L;
        for ( CatalogTable table : tables ) {
            for ( long columnId : table.columnIds ) {
                CatalogColumn column = this.catalog.getColumn( columnId );
                int calculatedPosition = tableOffsets.get( table.id ) + column.position - 1;
                RequestColumn requestColumn = new RequestColumn( column, calculatedPosition, calculatedPosition, null, null, true );
                columns.add( requestColumn );
            }
        }

        return columns;
    }


    @VisibleForTesting
    List<RequestColumn> generateRequestColumnsWithProject( String projectionString, Map<Long, Integer> tableOffsets, Set<Long> validColumns ) throws ParserException {
        List<RequestColumn> columns = new ArrayList<>();
        int internalPosition = 0;
        Set<Long> projectedColumns = new HashSet<>();

        String[] possibleProjections = projectionString.split( "," );

        for ( String projectionToParse : possibleProjections ) {
            Matcher matcher = PROJECTION_ENTRY_PATTERN.matcher( projectionToParse );
            if ( matcher.find() ) {
                String columnName = matcher.group( "column" );
                CatalogColumn catalogColumn;

                try {
                    catalogColumn = this.getCatalogColumnFromString( columnName );
                    log.debug( "Fetched catalog column for projection key: {}.", columnName );
                } catch ( UnknownColumnException | UnknownDatabaseException | UnknownSchemaException | UnknownTableException e ) {
                    log.warn( "Unable to fetch column: {}.", columnName, e );
                    throw new ParserException( ParserErrorCode.PROJECTION_MALFORMED, columnName );
                }

                if ( !validColumns.contains( catalogColumn.id ) ) {
                    log.warn( "Column isn't valid. Column: {}.", columnName );
                    throw new ParserException( ParserErrorCode.PROJECTION_INVALID_COLUMN, columnName );
                }

                projectedColumns.add( catalogColumn.id );
                int calculatedPosition = tableOffsets.get( catalogColumn.tableId ) + catalogColumn.position - 1;
                RequestColumn requestColumn = new RequestColumn( catalogColumn, calculatedPosition, internalPosition, matcher.group( "alias" ), this.decodeAggregateFunction( matcher.group( "agg" ) ) );
                internalPosition++;

                columns.add( requestColumn );
            } else {
                log.warn( "Malformed request: {}.", projectionToParse );
                throw new ParserException( ParserErrorCode.PROJECTION_MALFORMED, projectionToParse );
            }
        }

        Set<Long> notYetAdded = new HashSet<>( validColumns );
        notYetAdded.removeAll( projectedColumns );
        for ( long columnId : notYetAdded ) {
            CatalogColumn column = this.catalog.getColumn( columnId );
            int calculatedPosition = tableOffsets.get( column.tableId ) + column.position - 1;
            RequestColumn requestColumn = new RequestColumn( column, calculatedPosition, calculatedPosition, null, null, false );
            columns.add( requestColumn );
        }

        return columns;
    }


    @VisibleForTesting
    Map<String, RequestColumn> newGenerateNameMapping( List<RequestColumn> requestColumns ) {
        Map<String, RequestColumn> nameMapping = new HashMap<>();
        for ( RequestColumn requestColumn : requestColumns ) {
            nameMapping.put( requestColumn.getFullyQualifiedName(), requestColumn );
            nameMapping.put( requestColumn.getAlias(), requestColumn );
        }

        return nameMapping;
    }


    private List<RequestColumn> getAggregateColumns( List<RequestColumn> requestColumns ) {
        return requestColumns.stream().filter( RequestColumn::isAggregateColumn ).collect( Collectors.toList() );
    }


    @VisibleForTesting
    AggFunction decodeAggregateFunction( String function ) {
        if ( function == null ) {
            return null;
        }

        switch ( function ) {
            case "COUNT":
                return OperatorRegistry.getAgg( OperatorName.COUNT );
            case "MAX":
                return OperatorRegistry.getAgg( OperatorName.MAX );
            case "MIN":
                return OperatorRegistry.getAgg( OperatorName.MIN );
            case "AVG":
                return OperatorRegistry.getAgg( OperatorName.AVG );
            case "SUM":
                return OperatorRegistry.getAgg( OperatorName.SUM );
            default:
                return null;
        }
    }


    private CatalogColumn getCatalogColumnFromString( String name ) throws ParserException, UnknownColumnException, UnknownDatabaseException, UnknownSchemaException, UnknownTableException {
        String[] splitString = name.split( "\\." );
        if ( splitString.length != 3 ) {
            log.warn( "Column name is not 3 fields long. Got: {}", name );
            throw new ParserException( ParserErrorCode.PROJECTION_MALFORMED, name );
        }

        return this.catalog.getColumn( this.databaseName, splitString[0], splitString[1], splitString[2] );

    }


    @VisibleForTesting
    List<Pair<RequestColumn, Boolean>> parseSorting( HttpServletRequest request, Map<String, RequestColumn> nameAndAliasMapping ) throws ParserException {
        if ( !request.getParameterMap().containsKey( "_sort" ) ) {
            log.debug( "Request does not contain a sort. Returning null." );
            return null;
        }

        String[] possibleSortValues = request.getParameterMap().get( "_sort" );
        String possibleSortString = possibleSortValues[0];
        log.debug( "Starting to parse sort: {}", possibleSortString );

        List<Pair<RequestColumn, Boolean>> sortingColumns = new ArrayList<>();
        String[] possibleSorts = possibleSortString.split( "," );

        for ( String sortEntry : possibleSorts ) {
            Matcher matcher = SORTING_ENTRY_PATTERN.matcher( sortEntry );

            if ( matcher.find() ) {
                RequestColumn catalogColumn = nameAndAliasMapping.get( matcher.group( "column" ) );

                if ( catalogColumn == null ) {
                    throw new ParserException( ParserErrorCode.SORT_UNKNOWN_COLUMN, matcher.group( "column" ) );
                }

                boolean inverted = false;
                if ( matcher.group( "dir" ) != null ) {
                    String direction = matcher.group( "dir" );
                    inverted = "DESC".equals( direction );
                }

                sortingColumns.add( new Pair<>( catalogColumn, inverted ) );

            } else {
                // FIXME: Proper error handling.
                throw new ParserException( ParserErrorCode.SORT_MALFORMED, sortEntry );
//                throw new IllegalArgumentException( "The following provided sort instruction is not proper: '" + sortEntry + "'." );
            }
        }

        return sortingColumns;
    }


    @VisibleForTesting
    List<RequestColumn> parseGroupings( HttpServletRequest request, Map<String, RequestColumn> nameAndAliasMapping ) throws ParserException {
        if ( !request.getParameterMap().containsKey( "_groupby" ) ) {
            log.debug( "Request does not contain a grouping. Returning null." );
            return new ArrayList<>();
        }

        String[] possibleGroupbyValues = request.getParameterMap().get( "_groupby" );
        String possibleGroupbyString = possibleGroupbyValues[0];
        log.debug( "Starting to parse grouping: {}", possibleGroupbyString );

        List<RequestColumn> groupingColumns = new ArrayList<>();
        String[] possibleGroupings = possibleGroupbyString.split( "," );
        for ( String groupbyEntry : possibleGroupings ) {
            if ( nameAndAliasMapping.containsKey( groupbyEntry ) ) {
                groupingColumns.add( nameAndAliasMapping.get( groupbyEntry ) );
            } else {
                throw new ParserException( ParserErrorCode.GROUPING_UNKNOWN, groupbyEntry );
            }
        }

        return groupingColumns;
    }


    @VisibleForTesting
    Integer parseLimit( HttpServletRequest request ) throws ParserException {
        if ( !request.getParameterMap().containsKey( "_limit" ) ) {
            log.debug( "Request does not contain a limit. Returning -1." );
            return -1;
        }
        String[] possibleLimitValues = request.getParameterMap().get( "_limit" );

        try {
            log.debug( "Parsed limit value: {}.", possibleLimitValues[0] );
            return Integer.valueOf( possibleLimitValues[0] );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse limit value: {}", possibleLimitValues[0] );
            throw new ParserException( ParserErrorCode.LIMIT_MALFORMED, possibleLimitValues[0] );
        }
    }


    @VisibleForTesting
    Integer parseOffset( HttpServletRequest request ) throws ParserException {
        if ( !request.getParameterMap().containsKey( "_offset" ) ) {
            log.debug( "Request does not contain an offset. Returning -1." );
            return -1;
        }
        String[] possibleOffsetValues = request.getParameterMap().get( "_offset" );

        return this.parseOffset( possibleOffsetValues[0] );
    }


    @VisibleForTesting
    Integer parseOffset( String offsetString ) throws ParserException {
        try {
            log.debug( "Parsed offset value: {}.", offsetString );
            return Integer.valueOf( offsetString );
        } catch ( NumberFormatException e ) {
            log.warn( "Unable to parse offset value: {}", offsetString );
            throw new ParserException( ParserErrorCode.OFFSET_MALFORMED, offsetString );
        }
    }


    @VisibleForTesting
    Filters parseFilters( Map<String, String[]> filterMap, Map<String, RequestColumn> nameAndAliasMapping ) throws ParserException {
        Map<RequestColumn, List<Pair<Operator, Object>>> literalFilters = new HashMap<>();
        Map<RequestColumn, List<Pair<Operator, RequestColumn>>> columnFilters = new HashMap<>();

        for ( String possibleFilterKey : filterMap.keySet() ) {
            if ( possibleFilterKey.startsWith( "_" ) ) {
                log.debug( "Not a filter: {}", possibleFilterKey );
                continue;
            }
            log.debug( "Attempting to parse filters for key: {}.", possibleFilterKey );

            /*if ( ! nameAndAliasMapping.containsKey( possibleFilterKey ) ) {
                throw new IllegalArgumentException( "Not a valid column for filtering: '" + possibleFilterKey + "'." );
            }*/

            RequestColumn catalogColumn = nameAndAliasMapping.get( possibleFilterKey );

            if ( catalogColumn == null ) {
                log.warn( "Unknown column: {}", possibleFilterKey );
                throw new ParserException( ParserErrorCode.FILTER_UNKNOWN_COLUMN, possibleFilterKey );
            }

            List<Pair<Operator, Object>> literalFilterOperators = new ArrayList<>();
            List<Pair<Operator, RequestColumn>> columnFilterOperators = new ArrayList<>();

            for ( String filterString : filterMap.get( possibleFilterKey ) ) {
                Pair<Operator, String> rightHandSide = this.parseFilterOperation( filterString );
                Object literal = this.parseLiteralValue( catalogColumn.getColumn().type, rightHandSide.right );
                // TODO: add column filters here
                literalFilterOperators.add( new Pair<>( rightHandSide.left, literal ) );
            }
            // TODO: Add If Size != 0 checks for both put
            if ( !literalFilterOperators.isEmpty() ) {
                literalFilters.put( catalogColumn, literalFilterOperators );
            }
            //noinspection ConstantConditions
            if ( !columnFilterOperators.isEmpty() ) {
                // This statement is currently never run! This will change once column filters are added to the interface.
                columnFilters.put( catalogColumn, columnFilterOperators );
            }
            log.debug( "Finished parsing filters for key: {}.", possibleFilterKey );
        }

        return new Filters( literalFilters, columnFilters );
    }


    private Map<String, String[]> getFilterMap( HttpServletRequest request ) {
        HashMap<String, String[]> filterMap = new HashMap<>();
        for ( String filterKey : request.getParameterMap().keySet() ) {
            filterMap.put( filterKey, request.getParameterMap().get( filterKey ) );
        }
        return filterMap;
    }


    @VisibleForTesting
    Pair<Operator, String> parseFilterOperation( String filterString ) throws ParserException {
        log.debug( "Starting to parse filter operation. Value: {}.", filterString );

        Operator callOperator;
        String rightHandSide;
        if ( filterString.startsWith( "<=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "<" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.LESS_THAN );
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( ">=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( ">" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.GREATER_THAN );
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.EQUALS );
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.NOT_EQUALS );
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else if ( filterString.startsWith( "%" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.LIKE );
            rightHandSide = filterString.substring( 1, filterString.length() );
        } else if ( filterString.startsWith( "!%" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.NOT_LIKE );
            rightHandSide = filterString.substring( 2, filterString.length() );
        } else {
            log.warn( "Unable to parse filter operation comparator. Returning null." );
            throw new ParserException( ParserErrorCode.FILTER_GENERIC, filterString );
        }

        return new Pair<>( callOperator, rightHandSide );
    }


    // TODO: REWRITE THIS METHOD
    @VisibleForTesting
    Object parseLiteralValue( PolyType type, Object objectLiteral ) throws ParserException {
        if ( !(objectLiteral instanceof String) ) {
            if ( objectLiteral instanceof Double && type == PolyType.DECIMAL ) {
                return BigDecimal.valueOf( (Double) objectLiteral );
            }
            return objectLiteral;
        } else {
            Object parsedLiteral;
            String literal = (String) objectLiteral;
            if ( PolyType.BOOLEAN_TYPES.contains( type ) ) {
                parsedLiteral = Boolean.valueOf( literal );
            } else if ( PolyType.INT_TYPES.contains( type ) ) {
                parsedLiteral = Long.valueOf( literal );
            } else if ( PolyType.NUMERIC_TYPES.contains( type ) ) {
                if ( type == PolyType.DECIMAL ) {
                    parsedLiteral = new BigDecimal( literal );
                } else {
                    parsedLiteral = Double.valueOf( literal );
                }
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
                        long millisecondsSinceEpoch = instant.getEpochSecond() * 1000L + instant.getNano() / 1000000L;
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
            } else if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                //the binary data will be fetched later
                return null;
            } else {
                // TODO js: error handling.
                log.warn( "Unable to convert literal value. Returning null. Type: {}, Value: {}.", type, literal );
                throw new ParserException( ParserErrorCode.FILTER_GENERIC, objectLiteral.toString() );
            }

            return parsedLiteral;
        }
    }


    @VisibleForTesting
    List<List<Pair<RequestColumn, Object>>> parseValues( Context ctx, Gson gson, Map<String, RequestColumn> nameMapping ) throws ParserException {
        // FIXME: Verify stuff like applications/json, so on, so forth
        Object bodyObject = ctx.bodyAsClass( Object.class );
        Map bodyMap = (Map) bodyObject;
        List valuesList = (List) bodyMap.get( "data" );
        if ( valuesList == null ) {
            log.warn( "Missing values statement. Body: {}", bodyMap.toString() );
            throw new ParserException( ParserErrorCode.VALUE_MISSING, "" );
        }
        return this.parseInsertStatementBody( valuesList, nameMapping );
    }


    @VisibleForTesting
    List<List<Pair<RequestColumn, Object>>> parseInsertStatementBody( List<Object> bodyInsertValues, Map<String, RequestColumn> nameMapping ) throws ParserException {
        List<List<Pair<RequestColumn, Object>>> returnValue = new ArrayList<>();

        for ( Object rowObject : bodyInsertValues ) {
            Map rowMap = (Map) rowObject;
            List<Pair<RequestColumn, Object>> rowValue = this.parseInsertStatementValues( rowMap, nameMapping );
            returnValue.add( rowValue );
        }

        return returnValue;
    }


    private List<Pair<RequestColumn, Object>> parseInsertStatementValues( Map rowValuesMap, Map<String, RequestColumn> nameMapping ) throws ParserException {
        List<Pair<RequestColumn, Object>> result = new ArrayList<>();

        for ( Object objectColumnName : rowValuesMap.keySet() ) {
            String stringColumnName = (String) objectColumnName;
            /*if ( possibleValue.startsWith( "_" ) ) {
                log.debug( "FIX THIS MESSAGE: {}", possibleValue );
                continue;
            }*/

            // Make sure we actually have a column
            RequestColumn column = nameMapping.get( stringColumnName );
            if ( column == null ) {
                log.error( "Unable to fetch catalog column for filter key: {}.", stringColumnName );
                throw new ParserException( ParserErrorCode.VALUE_UNKNOWN_COLUMN, stringColumnName );
            }
            log.debug( "Fetched catalog column for filter key: {}", stringColumnName );

            Object litVal = rowValuesMap.get( objectColumnName );
            Object parsedValue = this.parseLiteralValue( column.getColumn().type, litVal );
            result.add( new Pair<>( column, parsedValue ) );
        }

        // TODO js: Do I need logical or table scan indices here?
        result.sort( Comparator.comparingInt( p -> p.left.getLogicalIndex() ) );

        return result;
    }


    public Map<String, CatalogColumn> generateNameMapping( List<CatalogTable> tables ) {
        Map<String, CatalogColumn> nameMapping = new HashMap<>();
        for ( CatalogTable table : tables ) {
            for ( CatalogColumn column : this.catalog.getColumns( table.id ) ) {
                nameMapping.put( column.getSchemaName() + "." + column.getTableName() + "." + column.name, column );
            }
        }

        return nameMapping;
    }


    @AllArgsConstructor
    public static class Filters {

        public final Map<RequestColumn, List<Pair<Operator, Object>>> literalFilters;
        public final Map<RequestColumn, List<Pair<Operator, RequestColumn>>> columnFilters;

    }


}
