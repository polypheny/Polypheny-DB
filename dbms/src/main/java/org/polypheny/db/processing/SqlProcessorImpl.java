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

package org.polypheny.db.processing;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.SqlProcessor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.PolyphenyDbSqlValidator;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlBasicCall;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlInsert;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.parser.SqlParseException;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.validate.SqlConformance;
import org.polypheny.db.sql2rel.RelDecorrelator;
import org.polypheny.db.sql2rel.SqlToRelConverter;
import org.polypheny.db.sql2rel.StandardConvertletTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;


@Slf4j
public class SqlProcessorImpl implements SqlProcessor, ViewExpander {

    private final Transaction transaction;
    private final SqlParserConfig parserConfig;
    private PolyphenyDbSqlValidator validator;


    public SqlProcessorImpl( Transaction transaction, SqlParserConfig parserConfig ) {
        this.transaction = transaction;
        this.parserConfig = parserConfig;
    }


    @Override
    public SqlNode parse( String sql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolySQL statement ..." );
        }
        stopWatch.start();
        SqlNode parsed;
        try {
            final SqlParser parser = SqlParser.create( new SourceStringReader( sql ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( SqlParseException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
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
    public Pair<SqlNode, RelDataType> validate( SqlNode parsed ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SQL ..." );
        }
        stopWatch.start();

        // Add default values for unset fields
        if ( RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ) {
            if ( parsed.getKind() == SqlKind.INSERT ) {
                addDefaultValues( (SqlInsert) parsed );
            }
        }

        final SqlConformance conformance = parserConfig.conformance();
        final PolyphenyDbCatalogReader catalogReader = transaction.getCatalogReader();
        validator = new PolyphenyDbSqlValidator( SqlStdOperatorTable.instance(), catalogReader, transaction.getTypeFactory(), conformance );
        validator.setIdentifierExpansion( true );

        SqlNode validated;
        RelDataType type;
        try {
            validated = validator.validate( parsed );
            type = validator.getValidatedNodeType( validated );
        } catch ( RuntimeException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
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
    public RelRoot translate( SqlNode sql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder();
        SqlToRelConverter.Config sqlToRelConfig = sqlToRelConfigBuilder.build();
        final RexBuilder rexBuilder = new RexBuilder( transaction.getTypeFactory() );

        final RelOptCluster cluster = RelOptCluster.create( transaction.getQueryProcessor().getPlanner(), rexBuilder );
        final SqlToRelConverter.Config config =
                SqlToRelConverter.configBuilder()
                        .withConfig( sqlToRelConfig )
                        .withTrimUnusedFields( false )
                        .withConvertTableAccess( false )
                        .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter( this, validator, transaction.getCatalogReader(), cluster, StandardConvertletTable.INSTANCE, config );
        RelRoot logicalRoot = sqlToRelConverter.convertQuery( sql, false, true );

        if ( transaction.isAnalyze() ) {
            InformationManager queryAnalyzer = transaction.getQueryAnalyzer();
            InformationPage page = new InformationPage( "informationPageLogicalQueryPlan", "Logical Query Plan" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
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
            log.trace( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
    }


    @Override
    public PolyphenyDbSignature prepareDdl( SqlNode parsed ) {
        if ( parsed instanceof SqlExecutableStatement ) {
            try {
                // Acquire global schema lock
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction, LockMode.EXCLUSIVE );
                // Execute statement
                ((SqlExecutableStatement) parsed).execute( transaction.getPrepareContext(), transaction );
                Catalog.getInstance().commit();
                return new PolyphenyDbSignature<>(
                        parsed.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        transaction.getSchema(),
                        ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "Exception while acquiring global schema lock", e );
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            } finally {
                // Release lock
                // TODO: This can be removed when auto-commit of ddls is implemented
                LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits SqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    // Add default values for unset fields
    private void addDefaultValues( SqlInsert insert ) {
        Context prepareContext = transaction.getPrepareContext();
        SqlNodeList oldColumnList = insert.getTargetColumnList();
        if ( oldColumnList != null ) {
            CatalogTable catalogTable = getCatalogTable( prepareContext, transaction, (SqlIdentifier) insert.getTargetTable() );
            SqlNodeList newColumnList = new SqlNodeList( SqlParserPos.ZERO );
            SqlNode[][] newValues = new SqlNode[((SqlBasicCall) insert.getSource()).getOperands().length][catalogTable.columnIds.size()];
            int pos = 0;
            for ( CatalogColumn column : Catalog.getInstance().getColumns( catalogTable.id ) ) {
                // Add column
                newColumnList.add( new SqlIdentifier( column.name, SqlParserPos.ZERO ) );

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
                            switch ( column.type ) {
                                case BOOLEAN:
                                    newValues[i][pos] = SqlLiteral.createBoolean( Boolean.parseBoolean( column.defaultValue.value ), SqlParserPos.ZERO );
                                    break;
                                case INTEGER:
                                case DECIMAL:
                                case BIGINT:
                                    newValues[i][pos] = SqlLiteral.createExactNumeric( column.defaultValue.value, SqlParserPos.ZERO );
                                    break;
                                case REAL:
                                case DOUBLE:
                                    newValues[i][pos] = SqlLiteral.createApproxNumeric( column.defaultValue.value, SqlParserPos.ZERO );
                                    break;
                                case VARCHAR:
                                    newValues[i][pos] = SqlLiteral.createCharString( column.defaultValue.value, SqlParserPos.ZERO );
                                    break;
                                default:
                                    throw new PolyphenyDbException( "Not yet supported default value type: " + defaultValue.type );
                            }
                        } else if ( column.nullable ) {
                            newValues[i][pos] = SqlLiteral.createNull( SqlParserPos.ZERO );
                        } else {
                            throw new PolyphenyDbException( "The not nullable field '" + column.name + "' is missing in the insert statement and has no default value defined." );
                        }
                    }
                    i++;
                }
                pos++;
            }
            // Add new column list
            insert.setColumnList( newColumnList );
            // Replace value in parser tree
            for ( int i = 0; i < newValues.length; i++ ) {
                SqlBasicCall call = ((SqlBasicCall) ((SqlBasicCall) insert.getSource()).getOperands()[i]);
                ((SqlBasicCall) insert.getSource()).getOperands()[i] = call.getOperator().createCall( call.getFunctionQuantifier(), call.getParserPosition(), newValues[i] );
            }
        }
    }


    private CatalogTable getCatalogTable( Context context, Transaction transaction, SqlIdentifier tableName ) {
        CatalogTable catalogTable;
        try {
            long schemaId;
            String tableOldName;
            if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = Catalog.getInstance().getSchema( tableName.names.get( 0 ), tableName.names.get( 1 ) ).id;
                tableOldName = tableName.names.get( 2 );
            } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
                schemaId = Catalog.getInstance().getSchema( context.getDatabaseId(), tableName.names.get( 0 ) ).id;
                tableOldName = tableName.names.get( 1 );
            } else { // TableName
                schemaId = Catalog.getInstance().getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableOldName = tableName.names.get( 0 );
            }
            catalogTable = Catalog.getInstance().getTable( schemaId, tableOldName );
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.databaseNotFound( tableName.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.schemaNotFound( tableName.toString() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.tableNotFound( tableName.toString() ) );
        } catch ( UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
        return catalogTable;
    }


    private int getPositionInSqlNodeList( SqlNodeList columnList, String name ) {
        int i = 0;
        for ( SqlNode node : columnList.getList() ) {
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
     * Walks over a tree of relational expressions, replacing each {@link RelNode} with a 'slimmed down' relational expression that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected RelRoot trimUnusedFields( RelRoot root, SqlToRelConverter sqlToRelConverter ) {
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields( shouldTrim( root.rel ) )
                .withExpand( false )
                .build();
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = SqlKind.DML.contains( root.kind );
        return root.withRel( sqlToRelConverter.trimUnusedFields( dml || ordered, root.rel ) );
    }


    private boolean shouldTrim( RelNode rootRel ) {
        // For now, don't trim if there are more than 3 joins. The projects near the leaves created by trim migrate past joins and seem to prevent join-reordering.
        return RelOptUtil.countJoins( rootRel ) < 2;
    }

}
