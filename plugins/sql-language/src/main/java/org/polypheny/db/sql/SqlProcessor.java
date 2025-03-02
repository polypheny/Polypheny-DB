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

package org.polypheny.db.sql;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.sql.sql2alg.StandardConvertletTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.util.Casing;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Setter
@Slf4j
public class SqlProcessor extends Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger( SqlProcessor.class );

    private static final ParserConfig parserConfig;

    private PolyphenyDbSqlValidator validator;


    static {
        SqlParser.ConfigBuilder configConfigBuilder = Parser.configBuilder();
        configConfigBuilder.setUnquotedCasing( Casing.UNCHANGED );
        configConfigBuilder.setQuotedCasing( Casing.UNCHANGED );
        parserConfig = configConfigBuilder.build();
    }


    public SqlProcessor() {

    }


    @Override
    public List<? extends Node> parse( String query ) {
        // todo we should not split the query here, but rather in the parser
        return Arrays.stream( query.split( ";\n" ) ).filter( s -> !s.trim().isEmpty() ).map( this::parseSingle ).toList();
    }


    public Node parseSingle( String query ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing SQL statement ..." );
        }
        stopWatch.start();
        Node parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "SQL: {}", query );
        }

        try {
            SqlAbstractParserImpl parserImpl = (SqlAbstractParserImpl) parserConfig.parserFactory().getParser( new SourceStringReader( query ) );
            final Parser parser = new SqlParser( parserImpl, parserConfig );
            parsed = parser.parseStmt();
        } catch ( NodeParseException e ) {
            log.error( "Caught exception", e );
            throw new GenericRuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing SQL statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public Pair<Node, AlgDataType> validate(boolean isMvccInternal, Transaction transaction, Node parsed, boolean addDefaultValues ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SQL ..." );
        }
        stopWatch.start();

        // Add default values for unset fields
        if ( addDefaultValues ) {
            if ( parsed.getKind() == Kind.INSERT ) {
                addDefaultValues( transaction, (SqlInsert) parsed );
            }
        }

        final Conformance conformance = parserConfig.conformance();
        final Snapshot snapshot = transaction.getSnapshot();
        validator = new PolyphenyDbSqlValidator( SqlStdOperatorTable.instance(), snapshot, transaction.getTypeFactory(), conformance );
        validator.setIdentifierExpansion( true );
        validator.setIsMvccInternal( isMvccInternal );

        Node validated;
        AlgDataType type;
        try {
            validated = validator.validate( parsed );
            type = validator.getValidatedNodeType( validated );
        } catch ( Exception e ) {
            log.error( "Exception while validating query", e );
            String message = e.getLocalizedMessage();
            throw new GenericRuntimeException( "Exception while validating query:" + (message == null ? "unrecoverable error" : message) );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Validated query: [{}]", validated );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SELECT Statement ... done. [{}]", stopWatch );
        }

        return new Pair<>( validated, type );
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        Config sqlToAlgConfig = NodeToAlgConverter.configBuilder().build();
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );

        final AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, statement.getDataContext().getSnapshot() );
        final Config config =
                NodeToAlgConverter.configBuilder()
                        .config( sqlToAlgConfig )
                        .trimUnusedFields( false )
                        .convertTableAccess( false )
                        .build();
        final SqlToAlgConverter sqlToAlgConverter = new SqlToAlgConverter( validator, statement.getTransaction().getSnapshot(), cluster, StandardConvertletTable.INSTANCE, config );
        AlgRoot logicalRoot = sqlToAlgConverter.convertQuery( context.getQueryNode().orElseThrow(), false, true );

        // Decorrelate
        final AlgBuilder algBuilder = config.algBuilderFactory().create( cluster, null );
        logicalRoot = logicalRoot.withAlg( AlgDecorrelator.decorrelateQuery( logicalRoot.alg, algBuilder ) );

        // Trim unused fields.
        if ( RuntimeConfig.TRIM_UNUSED_FIELDS.getBoolean() ) {
            logicalRoot = trimUnusedFields( logicalRoot, sqlToAlgConverter );
        }

        if ( log.isTraceEnabled() ) {
            log.trace( "Logical query plan: [{}]", AlgOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
    }


    @Override
    protected void lock( Transaction transaction, ParsedQueryContext context ) throws DeadlockException {
        // exclusive lock
        //TODO: TH check what this is used for...
        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( context.getNamespaceId() ).orElseThrow();
        transaction.acquireLockable( LockablesRegistry.INSTANCE.getOrCreateLockable( namespace ), LockType.EXCLUSIVE );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return ((SqlNode) parsed).toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql();
    }


    @Override
    public AlgDataType getParameterRowType( Node sqlNode ) {
        return validator.getParameterRowType( sqlNode );
    }


    public List<String> splitStatements( String statements ) {
        return SqlUtil.splitStatements( statements );
    }


    private void addDefaultValues( Transaction transaction, SqlInsert insert ) {
        //TODO: please fix me. Not all sources are SqlBasicCalls
        if (!(insert.getSource() instanceof SqlBasicCall)) {
            LOGGER.warn( "Non SqlBasicCall input encountered. Default values not added." );
            return;
        }

        SqlNodeList oldColumnList = insert.getTargetColumnList();

        if ( oldColumnList != null ) {
            LogicalTable catalogTable = getTable( transaction, (SqlIdentifier) insert.getTargetTable() );
            DataModel dataModel = Catalog.snapshot().getNamespace( catalogTable.namespaceId )
                    .orElseThrow()
                    .dataModel;

            SqlNodeList newColumnList = new SqlNodeList( ParserPos.ZERO );
            int size = computeTargetSize( catalogTable, oldColumnList, dataModel );

            SqlNode[][] newValues = new SqlNode[((SqlBasicCall) insert.getSource()).getOperands().length][size];
            processColumns( catalogTable, oldColumnList, newColumnList, newValues, insert );

            if ( dataModel == DataModel.DOCUMENT ) {
                addDocumentValues( oldColumnList, newColumnList, newValues, insert );
            }

            insert.setColumns( newColumnList );
            updateSourceValues( insert, newValues );
        }
    }


    public static void updateSourceValues( SqlInsert insert, SqlNode[][] newValues ) {
        for ( int i = 0; i < newValues.length; i++ ) {
            SqlBasicCall call = (SqlBasicCall) ((SqlBasicCall) insert.getSource()).getOperands()[i];
            ((SqlBasicCall) insert.getSource()).getOperands()[i] = (SqlNode) call.getOperator().createCall(
                    call.getFunctionQuantifier(),
                    call.getPos(),
                    newValues[i]
            );
        }
    }


    private void processColumns(
            LogicalTable catalogTable, SqlNodeList oldColumnList, SqlNodeList newColumnList,
            SqlNode[][] newValues, SqlInsert insert ) {
        int pos = 0;

        for ( LogicalColumn column : catalogTable.getColumns(false) ) {
            newColumnList.add( new SqlIdentifier( column.name, ParserPos.ZERO ) );

            for ( int i = 0; i < ((SqlBasicCall) insert.getSource()).getOperands().length; i++ ) {
                SqlNode sqlNode = ((SqlBasicCall) insert.getSource()).getOperands()[i];
                SqlBasicCall call = (SqlBasicCall) sqlNode;

                int position = getPositionInSqlNodeList( oldColumnList, column.name );
                if ( position >= 0 ) {
                    IdentifierUtils.throwIfIsDisallowedFieldName( column.name );
                    newValues[i][pos] = call.getOperands()[position];
                } else {
                    newValues[i][pos] = resolveDefaultValue( column );
                }
            }
            pos++;
        }
    }


    public static int computeTargetSize( LogicalTable catalogTable, SqlNodeList oldColumnList, DataModel dataModel ) {
        int size = catalogTable.getColumns(false).size();
        if ( dataModel == DataModel.DOCUMENT ) {
            List<String> columnNames = catalogTable.getColumnNames(false);
            size += (int) oldColumnList.getSqlList()
                    .stream()
                    .filter( column -> !columnNames.contains( ((SqlIdentifier) column).names.get( 0 ) ) )
                    .count();
        }
        return size;
    }


    public static LogicalTable getTable( Transaction transaction, SqlIdentifier tableName ) {
        Snapshot snapshot = transaction.getSnapshot();
        long namespaceId;
        String tableOldName;
        if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
            namespaceId = snapshot.getNamespace( tableName.names.get( 1 ) ).orElseThrow().id;
            tableOldName = tableName.names.get( 2 );
        } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
            namespaceId = snapshot.getNamespace( tableName.names.get( 0 ) ).orElseThrow().id;
            tableOldName = tableName.names.get( 1 );
        } else { // TableName
            namespaceId = snapshot.getNamespace( transaction.getDefaultNamespace().name ).orElseThrow().id;
            tableOldName = tableName.names.get( 0 );
        }
        return snapshot.rel().getTable( namespaceId, tableOldName ).orElseThrow( () -> new GenericRuntimeException( "Could not find table with name " + tableName.names ) );
    }


    private SqlNode resolveDefaultValue( LogicalColumn column ) {
        if ( column.defaultValue != null ) {
            LogicalDefaultValue defaultValue = column.defaultValue;
            // TODO NH handle arrays
            return createDefaultValueLiteral( column, defaultValue );
        } else if ( column.nullable ) {
            return SqlLiteral.createNull( ParserPos.ZERO );
        } else {
            throw new PolyphenyDbException(
                    "The not nullable field '" + column.name + "' is missing in the insert statement and has no default value defined."
            );
        }
    }


    private SqlNode createDefaultValueLiteral( LogicalColumn column, LogicalDefaultValue defaultValue ) {
        return switch ( column.type ) {
            case BOOLEAN -> SqlLiteral.createBoolean(
                    Boolean.parseBoolean( defaultValue.value.toJson() ),
                    ParserPos.ZERO
            );
            case INTEGER, DECIMAL, BIGINT -> SqlLiteral.createExactNumeric(
                    defaultValue.value.toJson(),
                    ParserPos.ZERO
            );
            case REAL, DOUBLE -> SqlLiteral.createApproxNumeric(
                    defaultValue.value.toJson(),
                    ParserPos.ZERO
            );
            case VARCHAR -> SqlLiteral.createCharString(
                    defaultValue.value.toJson(),
                    ParserPos.ZERO
            );
            default -> throw new PolyphenyDbException( "Unsupported default value type: " + defaultValue.type );
        };
    }


    private void addDocumentValues(
            SqlNodeList oldColumnList, SqlNodeList newColumnList,
            SqlNode[][] newValues, SqlInsert insert ) {
        List<SqlIdentifier> documentColumns = new ArrayList<>();

        for ( SqlNode column : oldColumnList.getSqlList() ) {
            if ( newColumnList.getSqlList()
                    .stream()
                    .filter( c -> c instanceof SqlIdentifier )
                    .map( c -> ((SqlIdentifier) c).names.get( 0 ) )
                    .noneMatch( c -> c.equals( ((SqlIdentifier) column).names.get( 0 ) ) ) ) {
                documentColumns.add( (SqlIdentifier) column );
            }
        }

        int pos = newColumnList.getSqlList().size();
        for ( SqlIdentifier doc : documentColumns ) {
            newColumnList.add( doc );
            for ( int i = 0; i < ((SqlBasicCall) insert.getSource()).getOperands().length; i++ ) {
                int position = getPositionInSqlNodeList( oldColumnList, doc.getSimple() );
                newValues[i][pos] = ((SqlBasicCall) ((SqlBasicCall) insert.getSource()).getOperands()[i])
                        .getOperands()[position];
            }
        }
    }


    private int getPositionInSqlNodeList( SqlNodeList columnList, String name ) {
        int i = 0;
        for ( Node node : columnList.getList() ) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            if ( identifier.getSimple().equalsIgnoreCase( name ) ) {
                return i;
            }
            i++;
        }
        return -1;
    }


    /**
     * Walks over a tree of relational expressions, replacing each {@link AlgNode} with a 'slimmed down' relational expression
     * that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected AlgRoot trimUnusedFields( AlgRoot root, SqlToAlgConverter sqlToAlgConverter ) {
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = Kind.DML.contains( root.kind );
        return root.withAlg( sqlToAlgConverter.trimUnusedFields( dml || ordered, root.alg ) );
    }

}
