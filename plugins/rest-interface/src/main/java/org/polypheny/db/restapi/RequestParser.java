/*
 * Copyright 2019-2024 The Polypheny Project
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
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
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
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;


@Slf4j
public class RequestParser {

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;
    private final String databaseName;
    private final String userName;


    private final static Pattern PROJECTION_ENTRY_PATTERN = Pattern.compile(
            "^(?<column>[a-zA-Z]\\w*\\.[a-zA-Z]\\w*\\.[a-zA-Z]\\w*)(?:@(?<alias>[a-zA-Z]\\w*)(?:\\((?<agg>[A-Z]+)\\))?)?$" );

    private final static Pattern SORTING_ENTRY_PATTERN = Pattern.compile(
            "^(?<column>[a-zA-Z]\\w*(?:\\.[a-zA-Z]\\w*\\.[a-zA-Z]\\w*)?)(?:@(?<dir>ASC|DESC))?$" );
    private final Catalog catalog;


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
    public LogicalUser parseBasicAuthentication( Context ctx ) throws UnauthorizedAccessException {
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

        List<LogicalTable> tables = this.parseTables( resourceName );
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
        List<LogicalTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( ctx.req ), tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        List<List<Pair<RequestColumn, PolyValue>>> values = this.parseValues( ctx, gson, nameMapping );

        return new ResourcePostRequest( tables, requestColumns, nameMapping, values, false );
    }


    public ResourcePostRequest parsePostMultipartRequest( String resourceName, String[] projections, List<Object> insertValues ) throws ParserException {
        List<LogicalTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( projections, tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        List<List<Pair<RequestColumn, PolyValue>>> values = parseInsertStatementBody( insertValues, nameMapping );

        return new ResourcePostRequest( tables, requestColumns, nameMapping, values, true );
    }


    public ResourcePatchRequest parsePatchResourceRequest( Context ctx, String resourceName, Gson gson ) throws ParserException {
        // TODO js: make sure it's only a single resource
        List<LogicalTable> tables = this.parseTables( resourceName );
        // TODO js: make sure there are no actual projections
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( getProjectionsValues( ctx.req ), tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );

        Filters filters = this.parseFilters( getFilterMap( ctx.req ), nameMapping );

        List<List<Pair<RequestColumn, PolyValue>>> values = this.parseValues( ctx, gson, nameMapping );

        return new ResourcePatchRequest( tables, requestColumns, values, nameMapping, filters, false );
    }


    public ResourcePatchRequest parsePatchMultipartRequest( String resourceName, String[] projections, Map<String, String[]> filterMap, List<Object> insertValues ) {
        List<LogicalTable> tables = this.parseTables( resourceName );
        List<RequestColumn> requestColumns = this.newParseProjectionsAndAggregations( projections, tables );
        Map<String, RequestColumn> nameMapping = this.newGenerateNameMapping( requestColumns );
        Filters filters = this.parseFilters( filterMap, nameMapping );
        List<List<Pair<RequestColumn, PolyValue>>> values = parseInsertStatementBody( insertValues, nameMapping );
        return new ResourcePatchRequest( tables, requestColumns, values, nameMapping, filters, true );
    }


    public ResourceDeleteRequest parseDeleteResourceRequest( HttpServletRequest request, String resourceName ) throws ParserException {
        // TODO js: make sure it's only a single resource
        List<LogicalTable> tables = this.parseTables( resourceName );

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
    List<LogicalTable> parseTables( String tableList ) throws ParserException {
        log.debug( "Starting to parse table list: {}", tableList );
        if ( tableList == null ) {
            throw new ParserException( ParserErrorCode.TABLE_LIST_GENERIC, "null" );
        }
        String[] tableNameList = tableList.split( "," );

        List<LogicalTable> tables = new ArrayList<>();
        for ( String tableName : tableNameList ) {
            LogicalTable temp = this.parseCatalogTableName( tableName );
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
    LogicalTable parseCatalogTableName( String tableName ) throws ParserException {
        String[] tableElements = tableName.split( "\\." );
        if ( tableElements.length != 2 ) {
            log.warn( "Table name \"{}\" not possible to parse.", tableName );
            throw new ParserException( ParserErrorCode.TABLE_LIST_MALFORMED_TABLE, tableName );
        }

        LogicalTable table = catalog.getSnapshot().rel().getTable( tableElements[0], tableElements[1] ).orElseThrow();
        if ( log.isDebugEnabled() ) {
            log.debug( "Finished parsing table \"{}\".", tableName );
        }
        return table;
    }


    @VisibleForTesting
    List<RequestColumn> newParseProjectionsAndAggregations( String[] possibleProjectionValues, List<LogicalTable> tables ) throws ParserException {
        // Helper structures & data
        Map<Long, Integer> tableOffsets = new HashMap<>();
        Set<Long> validColumns = new HashSet<>();
        int columnOffset = 0;
        for ( LogicalTable table : tables ) {
            tableOffsets.put( table.id, columnOffset );
            validColumns.addAll( table.getColumnIds() );
            columnOffset += table.getColumns().size();
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
    List<RequestColumn> generateRequestColumnsWithoutProject( List<LogicalTable> tables, Map<Long, Integer> tableOffsets ) {
        List<RequestColumn> columns = new ArrayList<>();
        long internalPosition = 0L;
        for ( LogicalTable table : tables ) {
            for ( LogicalColumn column : table.getColumns() ) {
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
                LogicalColumn logicalColumn = this.getCatalogColumnFromString( columnName );
                log.debug( "Fetched catalog column for projection key: {}.", columnName );

                if ( !validColumns.contains( logicalColumn.id ) ) {
                    log.warn( "Column isn't valid. Column: {}.", columnName );
                    throw new ParserException( ParserErrorCode.PROJECTION_INVALID_COLUMN, columnName );
                }

                projectedColumns.add( logicalColumn.id );
                int calculatedPosition = tableOffsets.get( logicalColumn.tableId ) + logicalColumn.position - 1;
                RequestColumn requestColumn = new RequestColumn( logicalColumn, calculatedPosition, internalPosition, matcher.group( "alias" ), this.decodeAggregateFunction( matcher.group( "agg" ) ) );
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
            LogicalColumn column = catalog.getSnapshot().getNamespaces( null ).stream().map( n -> catalog.getSnapshot().rel().getColumn( columnId ).orElseThrow() ).findFirst().orElseThrow();
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
        return requestColumns.stream().filter( RequestColumn::isAggregateColumn ).toList();
    }


    @VisibleForTesting
    AggFunction decodeAggregateFunction( String function ) {
        if ( function == null ) {
            return null;
        }

        return switch ( function ) {
            case "COUNT" -> OperatorRegistry.getAgg( OperatorName.COUNT );
            case "MAX" -> OperatorRegistry.getAgg( OperatorName.MAX );
            case "MIN" -> OperatorRegistry.getAgg( OperatorName.MIN );
            case "AVG" -> OperatorRegistry.getAgg( OperatorName.AVG );
            case "SUM" -> OperatorRegistry.getAgg( OperatorName.SUM );
            default -> null;
        };
    }


    private LogicalColumn getCatalogColumnFromString( String name ) throws ParserException {
        String[] splitString = name.split( "\\." );
        if ( splitString.length != 3 ) {
            log.warn( "Column name is not 3 fields long. Got: {}", name );
            throw new ParserException( ParserErrorCode.PROJECTION_MALFORMED, name );
        }

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( splitString[0] ).orElseThrow();

        return Catalog.snapshot().rel().getColumn( namespace.id, splitString[1], splitString[2] ).orElseThrow();

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
        Map<RequestColumn, List<Pair<Operator, PolyValue>>> literalFilters = new HashMap<>();
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

            List<Pair<Operator, PolyValue>> literalFilterOperators = new ArrayList<>();
            List<Pair<Operator, RequestColumn>> columnFilterOperators = new ArrayList<>();

            for ( String filterString : filterMap.get( possibleFilterKey ) ) {
                Pair<Operator, String> rightHandSide = this.parseFilterOperation( filterString );
                PolyValue literal = this.parseLiteralValue( catalogColumn.getColumn().type, rightHandSide.right );
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
            rightHandSide = filterString.substring( 2 );
        } else if ( filterString.startsWith( "<" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.LESS_THAN );
            rightHandSide = filterString.substring( 1 );
        } else if ( filterString.startsWith( ">=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            rightHandSide = filterString.substring( 2 );
        } else if ( filterString.startsWith( ">" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.GREATER_THAN );
            rightHandSide = filterString.substring( 1 );
        } else if ( filterString.startsWith( "=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.EQUALS );
            rightHandSide = filterString.substring( 1 );
        } else if ( filterString.startsWith( "!=" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.NOT_EQUALS );
            rightHandSide = filterString.substring( 2 );
        } else if ( filterString.startsWith( "%" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.LIKE );
            rightHandSide = filterString.substring( 1 );
        } else if ( filterString.startsWith( "!%" ) ) {
            callOperator = OperatorRegistry.get( OperatorName.NOT_LIKE );
            rightHandSide = filterString.substring( 2 );
        } else {
            log.warn( "Unable to parse filter operation comparator. Returning null." );
            throw new ParserException( ParserErrorCode.FILTER_GENERIC, filterString );
        }

        return new Pair<>( callOperator, rightHandSide );
    }


    // TODO: REWRITE THIS METHOD
    @VisibleForTesting
    PolyValue parseLiteralValue( PolyType type, Object objectLiteral ) throws ParserException {
        if ( !(objectLiteral instanceof String literal) ) {
            if ( PolyType.FRACTIONAL_TYPES.contains( type ) ) {
                if ( objectLiteral instanceof Number ) {
                    return PolyBigDecimal.of( BigDecimal.valueOf( ((Number) objectLiteral).doubleValue() ) );
                }
            } else if ( PolyType.NUMERIC_TYPES.contains( type ) ) {
                if ( objectLiteral instanceof Number ) {
                    return PolyBigDecimal.of( BigDecimal.valueOf( ((Number) objectLiteral).longValue() ) );
                }
            } else if ( PolyType.BOOLEAN_TYPES.contains( type ) ) {
                if ( objectLiteral instanceof Boolean ) {
                    return PolyBoolean.of( (Boolean) objectLiteral );
                }
            }
            throw new NotImplementedException( "Rest to Poly: " + objectLiteral );
        } else {
            PolyValue parsedLiteral;
            if ( PolyType.BOOLEAN_TYPES.contains( type ) ) {
                parsedLiteral = PolyBoolean.of( Boolean.valueOf( literal ) );
            } else if ( PolyType.INT_TYPES.contains( type ) ) {
                parsedLiteral = PolyLong.of( Long.valueOf( literal ) );
            } else if ( PolyType.NUMERIC_TYPES.contains( type ) ) {
                if ( type == PolyType.DECIMAL ) {
                    parsedLiteral = PolyBigDecimal.of( new BigDecimal( literal ) );
                } else {
                    parsedLiteral = PolyDouble.of( Double.valueOf( literal ) );
                }
            } else if ( PolyType.CHAR_TYPES.contains( type ) ) {
                parsedLiteral = PolyString.of( literal );
            } else if ( PolyType.DATETIME_TYPES.contains( type ) ) {
                switch ( type ) {
                    case DATE:
                        parsedLiteral = PolyDate.of( new DateString( literal ).getMillisSinceEpoch() );
                        break;
                    case TIMESTAMP:
                        Instant instant = LocalDateTime.parse( literal ).toInstant( ZoneOffset.UTC );
                        long millisecondsSinceEpoch = instant.toEpochMilli();
                        parsedLiteral = PolyTimestamp.of( millisecondsSinceEpoch );
                        break;
                    case TIME:
                        parsedLiteral = PolyTime.of( new TimeString( literal ).getMillisOfDay() );//- TimeZone.getDefault().getRawOffset() );
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
    List<List<Pair<RequestColumn, PolyValue>>> parseValues( Context ctx, Gson gson, Map<String, RequestColumn> nameMapping ) throws ParserException {
        // FIXME: Verify stuff like applications/json, so on, so forth
        Object bodyObject = ctx.bodyAsClass( Object.class );
        Map<?, ?> bodyMap = (Map<?, ?>) bodyObject;
        List<Object> valuesList = (List<Object>) bodyMap.get( "data" );
        if ( valuesList == null ) {
            log.warn( "Missing values statement. Body: {}", bodyMap );
            throw new ParserException( ParserErrorCode.VALUE_MISSING, "" );
        }
        return this.parseInsertStatementBody( valuesList, nameMapping );
    }


    @VisibleForTesting
    List<List<Pair<RequestColumn, PolyValue>>> parseInsertStatementBody( List<Object> bodyInsertValues, Map<String, RequestColumn> nameMapping ) throws ParserException {
        List<List<Pair<RequestColumn, PolyValue>>> returnValue = new ArrayList<>();

        for ( Object rowObject : bodyInsertValues ) {
            Map rowMap = (Map) rowObject;
            List<Pair<RequestColumn, PolyValue>> rowValue = this.parseInsertStatementValues( rowMap, nameMapping );
            returnValue.add( rowValue );
        }

        return returnValue;
    }


    private List<Pair<RequestColumn, PolyValue>> parseInsertStatementValues( Map rowValuesMap, Map<String, RequestColumn> nameMapping ) throws ParserException {
        List<Pair<RequestColumn, PolyValue>> result = new ArrayList<>();

        for ( Object objectColumnName : rowValuesMap.keySet() ) {
            String stringColumnName = (String) objectColumnName;

            // Make sure we actually have a column
            RequestColumn column = nameMapping.get( stringColumnName );
            if ( column == null ) {
                log.error( "Unable to fetch catalog column for filter key: {}.", stringColumnName );
                throw new ParserException( ParserErrorCode.VALUE_UNKNOWN_COLUMN, stringColumnName );
            }
            log.debug( "Fetched catalog column for filter key: {}", stringColumnName );

            Object litVal = rowValuesMap.get( objectColumnName );
            PolyValue parsedValue = this.parseLiteralValue( column.getColumn().type, litVal );
            result.add( new Pair<>( column, parsedValue ) );
        }

        // TODO js: Do I need logical or table relScan indices here?
        result.sort( Comparator.comparingInt( p -> p.left.getLogicalIndex() ) );

        return result;
    }


    public Map<String, LogicalColumn> generateNameMapping( List<LogicalTable> tables ) {
        Map<String, LogicalColumn> nameMapping = new HashMap<>();
        for ( LogicalTable table : tables ) {
            for ( LogicalColumn column : Catalog.snapshot().rel().getColumns( table.id ) ) {
                nameMapping.put( column.getNamespaceName() + "." + column.getTableName() + "." + column.name, column );
            }
        }

        return nameMapping;
    }


    @AllArgsConstructor
    public static class Filters {

        public final Map<RequestColumn, List<Pair<Operator, PolyValue>>> literalFilters;
        public final Map<RequestColumn, List<Pair<Operator, RequestColumn>>> columnFilters;

    }


}
