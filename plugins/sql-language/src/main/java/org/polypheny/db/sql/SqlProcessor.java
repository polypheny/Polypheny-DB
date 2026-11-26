/*
 * Copyright 2019-2026 The Polypheny Project
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
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
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
import org.polypheny.db.runtime.SqlStateException;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlUpdate;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.sql.sql2alg.StandardConvertletTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Statement.GeneratedKeySelection;
import org.polypheny.db.transaction.Statement.InsertReturning;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.util.Casing;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;


@Setter
@Slf4j
public class SqlProcessor extends Processor {

    private static final ParserConfig parserConfig;


    static {
        SqlParser.ConfigBuilder configConfigBuilder = Parser.configBuilder();
        configConfigBuilder.setUnquotedCasing( Casing.UNCHANGED );
        configConfigBuilder.setQuotedCasing( Casing.UNCHANGED );
        parserConfig = configConfigBuilder.build();
    }


    private PolyphenyDbSqlValidator validator;


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
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SQL ..." );
        }
        stopWatch.start();

        final Snapshot snapshot = transaction.getSnapshot();
        // Add default values for unset fields
        if ( addDefaultValues ) {
            if ( parsed.getKind() == Kind.INSERT ) {
                addDefaultValues( transaction, snapshot, (SqlInsert) parsed );
            }
        }

        final Conformance conformance = parserConfig.conformance();
        validator = new PolyphenyDbSqlValidator( SqlStdOperatorTable.instance(), snapshot, transaction.getTypeFactory(), conformance );
        validator.setIdentifierExpansion( true );

        Node validated;
        AlgDataType type;
        try {
            validated = validator.validate( parsed );
            type = validator.getValidatedNodeType( validated );
            if ( validated instanceof SqlInsert insert && insert.getReturningColumns() != null ) {
                type = insertReturning( transaction, snapshot, insert ).rowType();
            }
        } catch ( Exception e ) {
            log.error( "Exception while validating query", e );
            String message = e.getLocalizedMessage();
            throw new GenericRuntimeException( "Exception while validating query: " + (message == null ? "unrecoverable error" : message), e );
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

        final Snapshot snapshot = validator.getSnapshot();
        final AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, snapshot );
        final Config config =
                NodeToAlgConverter.configBuilder()
                        .config( sqlToAlgConfig )
                        .trimUnusedFields( false )
                        .convertTableAccess( false )
                        .build();
        final SqlToAlgConverter sqlToAlgConverter = new SqlToAlgConverter( validator, snapshot, cluster, StandardConvertletTable.INSTANCE, config );
        Node query = context.getQueryNode().orElseThrow();
        if ( query instanceof SqlInsert insert ) {
            GeneratedKeySelection keySelection = statement.getGeneratedKeySelection();
            if ( insert.getReturningColumns() != null ) {
                if ( keySelection != null ) {
                    throw new GenericRuntimeException( "Use either SQL RETURNING or the generated keys request for this INSERT." );
                }
                statement.setInsertReturning( insertReturning( statement.getTransaction(), snapshot, insert ) );
            } else if ( keySelection != null ) {
                statement.setGeneratedKeysProjection( generatedKeys( statement.getTransaction(), snapshot, insert, keySelection ) );
            }
        }
        AlgRoot logicalRoot = sqlToAlgConverter.convertQuery( query, false, true );

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
    public boolean isUpdateOnly( Node node ) {
        return !(node instanceof SqlInsert insert && insert.getReturningColumns() != null) && super.isUpdateOnly( node );
    }


    @Override
    public boolean requiresSequentialBatchExecution( Node statement, AlgDataType parameterRowType ) {
        // A zero-width parameter matrix loses the number of update commands.
        if ( parameterRowType.getFieldCount() == 0 && Kind.DML.contains( statement.getKind() ) ) {
            return true;
        }
        if ( statement instanceof SqlInsert insert && insert.getSchemaType() == DataModel.RELATIONAL ) {
            // SELECT sources must observe preceding writes, including when reading the target table.
            return insert.getSource().getKind() != Kind.VALUES;
        }
        // Identity defaults resolve the row and its other assignments before write routing.
        return statement instanceof SqlUpdate update
                && update.getSourceExpressionList().getSqlList().stream().anyMatch( expression -> expression.getKind() == Kind.DEFAULT );
    }


    private InsertReturning insertReturning( Transaction transaction, Snapshot snapshot, SqlInsert insert ) {
        LogicalTable table = getValidatedInsertTarget( snapshot, insert );
        List<LogicalColumn> columns = snapshot.rel().getColumns( table.id );
        LogicalNamespace namespace = snapshot.getNamespace( table.namespaceId ).orElseThrow();
        List<LogicalColumn> selected = new ArrayList<>();
        for ( SqlNode node : insert.getReturningColumns().getSqlList() ) {
            if ( !(node instanceof SqlIdentifier identifier) ) {
                throw new GenericRuntimeException( "INSERT RETURNING expects target column names or *." );
            }
            if ( identifier.isStar() && identifier.names.size() == 1 ) {
                selected.addAll( columns );
            } else if ( identifier.isSimple() ) {
                selected.add( findReturningColumn( columns, namespace, identifier.getSimple() ) );
            } else {
                throw new GenericRuntimeException( "INSERT RETURNING expects unqualified target column names." );
            }
        }
        return returningProjection( transaction, snapshot, table, columns, selected );
    }


    private InsertReturning generatedKeys( Transaction transaction, Snapshot snapshot, SqlInsert insert, GeneratedKeySelection selection ) {
        LogicalTable table = getValidatedInsertTarget( snapshot, insert );
        List<LogicalColumn> columns = snapshot.rel().getColumns( table.id );
        LogicalNamespace namespace = snapshot.getNamespace( table.namespaceId ).orElseThrow();
        List<LogicalColumn> selected;
        if ( !selection.names().isEmpty() ) {
            selected = selection.names().stream().map( name -> findReturningColumn( columns, namespace, name ) ).toList();
        } else if ( !selection.indexes().isEmpty() ) {
            selected = selection.indexes().stream().map( index -> {
                if ( index < 1 || index > columns.size() ) {
                    throw new GenericRuntimeException( "Generated key column index is out of range: " + index );
                }
                return columns.get( index - 1 );
            } ).toList();
        } else {
            selected = columns.stream().filter( column -> column.getIdentity() != null ).toList();
        }
        return returningProjection( transaction, snapshot, table, columns, selected );
    }


    private LogicalTable getValidatedInsertTarget( Snapshot snapshot, SqlInsert insert ) {
        // The validator can normalize the target identifier to an unqualified name.
        return SqlValidatorUtil.getLogicalEntity( validator.getSqlNamespace( insert ), snapshot, null, null ).unwrapOrThrow( LogicalTable.class );
    }


    private LogicalColumn findReturningColumn( List<LogicalColumn> columns, LogicalNamespace namespace, String name ) {
        return columns.stream()
                .filter( field -> namespace.caseSensitive ? field.name.equals( name ) : field.name.equalsIgnoreCase( name ) )
                .findFirst().orElseThrow( () -> new GenericRuntimeException( "Unknown result column: " + name ) );
    }


    private InsertReturning returningProjection( Transaction transaction, Snapshot snapshot, LogicalTable table, List<LogicalColumn> columns, List<LogicalColumn> selected ) {
        AlgDataTypeFactory factory = transaction.getTypeFactory();
        AlgDataTypeFactory.Builder type = factory.builder();
        List<Integer> ordinals = new ArrayList<>();
        List<List<String>> origins = new ArrayList<>();
        String namespace = snapshot.getNamespace( table.namespaceId ).orElseThrow().name;
        for ( LogicalColumn column : selected ) {
            ordinals.add( columns.indexOf( column ) );
            type.add( column.id, column.name, null, column.getAlgDataType( factory ) );
            origins.add( List.of( "", namespace, table.name, column.name ) );
        }
        return new InsertReturning( type.build(), List.copyOf( ordinals ), List.copyOf( origins ) );
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


    // Add default values for unset fields
    private void addDefaultValues( Transaction transaction, Snapshot snapshot, SqlInsert insert ) {
        SqlNodeList oldColumnList = insert.getTargetColumnList();

        if ( oldColumnList != null ) {
            LogicalTable catalogTable = getTable( transaction, snapshot, (SqlIdentifier) insert.getTargetTable() );
            List<LogicalColumn> catalogColumns = snapshot.rel().getColumns( catalogTable.id );
            DataModel dataModel = snapshot.getNamespace( catalogTable.namespaceId ).orElseThrow().dataModel;

            if ( insert.isDefaultValues()
                    || catalogColumns.stream().anyMatch( column -> column.getIdentity() != null )
                    || !(insert.getSource() instanceof SqlBasicCall values)
                    || values.getKind() != Kind.VALUES ) {
                return;
            }

            SqlNodeList newColumnList = new SqlNodeList( ParserPos.ZERO );
            int size = catalogColumns.size();
            if ( dataModel == DataModel.DOCUMENT ) {
                List<String> columnNames = catalogColumns.stream().map( column -> column.name ).toList();
                size += (int) oldColumnList.getSqlList().stream().filter( column -> !columnNames.contains( ((SqlIdentifier) column).names.get( 0 ) ) ).count();
            }

            SqlNode[][] newValues = new SqlNode[((SqlBasicCall) insert.getSource()).getOperands().length][size];
            int pos = 0;
            for ( LogicalColumn column : catalogColumns ) {

                // Add column
                newColumnList.add( new SqlIdentifier( column.name, ParserPos.ZERO ) );

                // Add value (loop because it can be a multi insert (insert into test(id) values (1),(2),(3))
                int i = 0;
                for ( SqlNode sqlNode : ((SqlBasicCall) insert.getSource()).getOperands() ) {
                    SqlBasicCall call = (SqlBasicCall) sqlNode;
                    int position = getPositionInSqlNodeList( oldColumnList, column.name );
                    if ( position >= 0 ) {
                        newValues[i][pos] = call.getOperands()[position];
                    } else {
                        // Add value
                        if ( column.defaultValue != null ) {
                            LogicalDefaultValue defaultValue = column.defaultValue;
                            //TODO NH handle arrays
                            switch ( column.type ) {
                                case BOOLEAN:
                                    newValues[i][pos] = SqlLiteral.createBoolean(
                                            Boolean.parseBoolean( column.defaultValue.value.toJson() ),
                                            ParserPos.ZERO );
                                    break;
                                case INTEGER:
                                case DECIMAL:
                                case BIGINT:
                                    newValues[i][pos] = SqlLiteral.createExactNumeric(
                                            column.defaultValue.value.toJson(),
                                            ParserPos.ZERO );
                                    break;
                                case REAL:
                                case DOUBLE:
                                    newValues[i][pos] = SqlLiteral.createApproxNumeric(
                                            column.defaultValue.value.toJson(),
                                            ParserPos.ZERO );
                                    break;
                                case VARCHAR:
                                    newValues[i][pos] = SqlLiteral.createCharString(
                                            column.defaultValue.value.toJson(),
                                            ParserPos.ZERO );
                                    break;
                                default:
                                    throw new PolyphenyDbException( "Not yet supported default value type: " + defaultValue.type, null, "0A000" );
                            }
                        } else if ( column.nullable ) {
                            newValues[i][pos] = SqlLiteral.createNull( ParserPos.ZERO );
                        } else {
                            throw new PolyphenyDbException( "The not nullable field '" + column.name + "' is missing in the insert statement and has no default value defined.", null, "23502" );
                        }
                    }
                    i++;
                }
                pos++;
            }

            // add doc values back TODO DL: change
            if ( dataModel == DataModel.DOCUMENT ) {
                List<SqlIdentifier> documentColumns = new ArrayList<>();
                for ( Node column : oldColumnList.getSqlList() ) {
                    if ( newColumnList.getSqlList()
                            .stream()
                            .filter( c -> c instanceof SqlIdentifier )
                            .map( c -> ((SqlIdentifier) c).names.get( 0 ) )
                            .noneMatch( c -> c.equals( ((SqlIdentifier) column).names.get( 0 ) ) ) ) {
                        documentColumns.add( (SqlIdentifier) column );
                    }
                }

                for ( SqlIdentifier doc : documentColumns ) {
                    int i = 0;
                    newColumnList.add( doc );

                    for ( SqlNode sqlNode : ((SqlBasicCall) insert.getSource()).getOperands() ) {
                        int position = getPositionInSqlNodeList( oldColumnList, doc.getSimple() );
                        newValues[i][pos] = ((SqlBasicCall) sqlNode).getOperands()[position];
                    }
                    pos++;
                }

            }

            // Add new column list
            insert.setColumns( newColumnList );
            // Replace value in parser tree
            for ( int i = 0; i < newValues.length; i++ ) {
                SqlBasicCall call = ((SqlBasicCall) ((SqlBasicCall) insert.getSource()).getOperands()[i]);
                ((SqlBasicCall) insert.getSource()).getOperands()[i] = (SqlNode) call.getOperator().createCall(
                        call.getFunctionQuantifier(),
                        call.getPos(),
                        newValues[i] );
            }
        }
    }


    private LogicalTable getTable( Transaction transaction, Snapshot snapshot, SqlIdentifier tableName ) {
        long namespaceId;
        String tableOldName;
        if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
            namespaceId = snapshot.getNamespace( tableName.names.get( 1 ) ).orElseThrow( () -> new SqlStateException( "No value present", "3F000" ) ).id;
            tableOldName = tableName.names.get( 2 );
        } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
            namespaceId = snapshot.getNamespace( tableName.names.get( 0 ) ).orElseThrow( () -> new SqlStateException( "No value present", "3F000" ) ).id;
            tableOldName = tableName.names.get( 1 );
        } else { // TableName
            namespaceId = snapshot.getNamespace( transaction.getDefaultNamespace().name ).orElseThrow().id;
            tableOldName = tableName.names.get( 0 );
        }
        return snapshot.rel().getTable( namespaceId, tableOldName ).orElseThrow( () -> new SqlStateException( "Could not find table with name " + tableName.names, "42P01" ) );
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
        final Config config = NodeToAlgConverter.configBuilder()
                .trimUnusedFields( shouldTrim( root.alg ) )
                .expand( false )
                .build();
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = Kind.DML.contains( root.kind );
        return root.withAlg( sqlToAlgConverter.trimUnusedFields( dml || ordered, root.alg ) );
    }


    private boolean shouldTrim( AlgNode rootAlg ) {
        // For now, don't trim if there are more than 3 joins. The projects near the leaves created by trim migrate past
        // joins and seem to prevent join-reordering.
        return AlgOptUtil.countJoins( rootAlg ) < 2;
    }

}
