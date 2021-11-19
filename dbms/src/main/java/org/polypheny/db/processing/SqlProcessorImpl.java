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

package org.polypheny.db.processing;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.util.Casing;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.CoreUtil;
import org.polypheny.db.core.ExecutableStatement;
import org.polypheny.db.core.ExplainFormat;
import org.polypheny.db.core.ExplainLevel;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.NodeParseException;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.QueryParameters;
import org.polypheny.db.core.RelDecorrelator;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.languages.NodeToRelConverter;
import org.polypheny.db.languages.NodeToRelConverter.Config;
import org.polypheny.db.languages.NodeToRelConverter.ConfigBuilder;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.sql.SqlBasicCall;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlInsert;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlNodeList;
import org.polypheny.db.languages.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.parser.SqlParser;
import org.polypheny.db.languages.sql.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql2rel.SqlToRelConverter;
import org.polypheny.db.languages.sql2rel.StandardConvertletTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;


@Slf4j
public class SqlProcessorImpl implements Processor, ViewExpander {

    private static final ParserConfig parserConfig;
    @Setter
    private PolyphenyDbSqlValidator validator;


    static {
        SqlParser.ConfigBuilder configConfigBuilder = Parser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        parserConfig = configConfigBuilder.build();
    }


    public SqlProcessorImpl() {

    }


    @Override
    public Node parse( String query ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolySQL statement ..." );
        }
        stopWatch.start();
        Node parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "SQL: {}", query );
        }

        try {
            final Parser parser = Parser.create( new SourceStringReader( query ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( NodeParseException e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolySQL statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public Pair<Node, RelDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
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
        final PolyphenyDbCatalogReader catalogReader = transaction.getCatalogReader();
        validator = new PolyphenyDbSqlValidator( SqlStdOperatorTable.instance(), catalogReader, transaction.getTypeFactory(), conformance );
        validator.setIdentifierExpansion( true );

        Node validated;
        RelDataType type;
        try {
            validated = validator.validate( parsed );
            type = validator.getValidatedNodeType( validated );
        } catch ( RuntimeException e ) {
            log.error( "Exception while validating query", e );
            String message = e.getLocalizedMessage();
            throw new AvaticaRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
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
    public RelRoot translate( Statement statement, Node query, QueryParameters parameters ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        ConfigBuilder sqlToRelConfigBuilder = NodeToRelConverter.configBuilder();
        Config sqlToRelConfig = sqlToRelConfigBuilder.build();
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );

        final RelOptCluster cluster = RelOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );
        final Config config =
                NodeToRelConverter.configBuilder()
                        .withConfig( sqlToRelConfig )
                        .withTrimUnusedFields( false )
                        .withConvertTableAccess( false )
                        .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter( this, (SqlValidator) validator, statement.getTransaction().getCatalogReader(), cluster, StandardConvertletTable.INSTANCE, config );
        RelRoot logicalRoot = sqlToRelConverter.convertQuery( (SqlNode) query, false, true );

        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Logical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.rel, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        // Decorrelate
        final RelBuilder relBuilder = config.getRelBuilderFactory().create( cluster, null );
        logicalRoot = logicalRoot.withRel( RelDecorrelator.decorrelateQuery( logicalRoot.rel, relBuilder ) );

        // Trim unused fields.
        if ( RuntimeConfig.TRIM_UNUSED_FIELDS.getBoolean() ) {
            logicalRoot = trimUnusedFields( logicalRoot, sqlToRelConverter );
        }

        if ( log.isTraceEnabled() ) {
            log.trace( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.rel, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
    }


    @Override
    public PolyphenyDbSignature<?> prepareDdl( Statement statement, Node parsed, QueryParameters parameters ) {
        if ( parsed instanceof ExecutableStatement ) {
            try {
                // Acquire global schema lock
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), LockMode.EXCLUSIVE );
                // Execute statement
                ((ExecutableStatement) parsed).execute( statement.getPrepareContext(), statement, null );
                statement.getTransaction().commit();
                Catalog.getInstance().commit();
                return new PolyphenyDbSignature<>(
                        ((SqlNode) parsed).toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        statement.getTransaction().getSchema(),
                        ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL,
                        new ExecutionTimeMonitor() );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "Exception while acquiring global schema lock", e );
            } catch ( TransactionException | NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            } finally {
                // Release lock
                LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction() );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits ExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    @Override
    public RelDataType getParameterRowType( Node sqlNode ) {
        return validator.getParameterRowType( sqlNode );
    }


    // Add default values for unset fields
    private void addDefaultValues( Transaction transaction, SqlInsert insert ) {
        SqlNodeList oldColumnList = insert.getTargetColumnList();

        if ( oldColumnList != null ) {
            CatalogTable catalogTable = getCatalogTable( transaction, (SqlIdentifier) insert.getTargetTable() );
            SchemaType schemaType = Catalog.getInstance().getSchema( catalogTable.schemaId ).schemaType;

            catalogTable = getCatalogTable( transaction, (SqlIdentifier) insert.getTargetTable() );

            SqlNodeList newColumnList = new SqlNodeList( ParserPos.ZERO );
            int size = (int) catalogTable.columnIds.size();
            if ( schemaType == SchemaType.DOCUMENT ) {
                List<String> columnNames = catalogTable.getColumnNames();
                size += oldColumnList.getSqlList().stream().filter( column -> !columnNames.contains( ((SqlIdentifier) column).names.get( 0 ) ) ).count();
            }

            SqlNode[][] newValues = new SqlNode[((SqlBasicCall) insert.getSource()).getOperands().length][size];
            int pos = 0;
            List<CatalogColumn> columns = Catalog.getInstance().getColumns( catalogTable.id );
            for ( CatalogColumn column : columns ) {

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
                            CatalogDefaultValue defaultValue = column.defaultValue;
                            //TODO NH handle arrays
                            switch ( column.type ) {
                                case BOOLEAN:
                                    newValues[i][pos] = SqlLiteral.createBoolean(
                                            Boolean.parseBoolean( column.defaultValue.value ),
                                            ParserPos.ZERO );
                                    break;
                                case INTEGER:
                                case DECIMAL:
                                case BIGINT:
                                    newValues[i][pos] = SqlLiteral.createExactNumeric(
                                            column.defaultValue.value,
                                            ParserPos.ZERO );
                                    break;
                                case REAL:
                                case DOUBLE:
                                    newValues[i][pos] = SqlLiteral.createApproxNumeric(
                                            column.defaultValue.value,
                                            ParserPos.ZERO );
                                    break;
                                case VARCHAR:
                                    newValues[i][pos] = SqlLiteral.createCharString(
                                            column.defaultValue.value,
                                            ParserPos.ZERO );
                                    break;
                                default:
                                    throw new PolyphenyDbException( "Not yet supported default value type: " + defaultValue.type );
                            }
                        } else if ( column.nullable ) {
                            newValues[i][pos] = SqlLiteral.createNull( ParserPos.ZERO );
                        } else {
                            throw new PolyphenyDbException( "The not nullable field '" + column.name + "' is missing in the insert statement and has no default value defined." );
                        }
                    }
                    i++;
                }
                pos++;
            }

            // add doc values back TODO DL: change
            if ( schemaType == SchemaType.DOCUMENT ) {
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
            insert.setColumnList( newColumnList );
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


    private CatalogTable getCatalogTable( Transaction transaction, SqlIdentifier tableName ) {
        CatalogTable catalogTable;
        try {
            long schemaId;
            String tableOldName;
            if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = Catalog.getInstance().getSchema( tableName.names.get( 0 ), tableName.names.get( 1 ) ).id;
                tableOldName = tableName.names.get( 2 );
            } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
                schemaId = Catalog.getInstance().getSchema( transaction.getDefaultSchema().databaseId, tableName.names.get( 0 ) ).id;
                tableOldName = tableName.names.get( 1 );
            } else { // TableName
                schemaId = Catalog.getInstance().getSchema( transaction.getDefaultSchema().databaseId, transaction.getDefaultSchema().name ).id;
                tableOldName = tableName.names.get( 0 );
            }
            catalogTable = Catalog.getInstance().getTable( schemaId, tableOldName );
        } catch ( UnknownDatabaseException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.databaseNotFound( tableName.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.schemaNotFound( tableName.toString() ) );
        } catch ( UnknownTableException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.tableNotFound( tableName.toString() ) );
        }
        return catalogTable;
    }


    private int getPositionInSqlNodeList( SqlNodeList columnList, String name ) {
        int i = 0;
        for ( Node node : columnList.getList() ) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            if ( RuntimeConfig.CASE_SENSITIVE.getBoolean() ) {
                if ( identifier.getSimple().equals( name ) ) {
                    return i;
                }
            } else {
                if ( identifier.getSimple().equalsIgnoreCase( name ) ) {
                    return i;
                }
            }
            i++;
        }
        return -1;
    }


    /**
     * Returns a relational expression that is to be substituted for an access to a SQL view.
     *
     * @param rowType Row type of the view
     * @param queryString Body of the view
     * @param schemaPath Path of a schema wherein to find referenced tables
     * @param viewPath Path of the view, ending with its name; may be null
     * @return Relational expression
     */
    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null; // TODO: Implement
    }


    /**
     * Walks over a tree of relational expressions, replacing each {@link RelNode} with a 'slimmed down' relational expression
     * that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected RelRoot trimUnusedFields( RelRoot root, SqlToRelConverter sqlToRelConverter ) {
        final Config config = NodeToRelConverter.configBuilder()
                .withTrimUnusedFields( shouldTrim( root.rel ) )
                .withExpand( false )
                .build();
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = Kind.DML.contains( root.kind );
        return root.withRel( sqlToRelConverter.trimUnusedFields( dml || ordered, root.rel ) );
    }


    private boolean shouldTrim( RelNode rootRel ) {
        // For now, don't trim if there are more than 3 joins. The projects near the leaves created by trim migrate past
        // joins and seem to prevent join-reordering.
        return RelOptUtil.countJoins( rootRel ) < 2;
    }

}
