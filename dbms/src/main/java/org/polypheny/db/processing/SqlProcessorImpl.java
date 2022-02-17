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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.util.Casing;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.sql.SqlBasicCall;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlInsert;
import org.polypheny.db.sql.sql.SqlLiteral;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlNodeList;
import org.polypheny.db.sql.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.sql.parser.SqlParser;
import org.polypheny.db.sql.sql.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.sql.sql2alg.StandardConvertletTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;


@Slf4j
public class SqlProcessorImpl extends Processor {

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
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
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
        AlgDataType type;
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
    public AlgRoot translate( Statement statement, Node query, QueryParameters parameters ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        Config sqlToAlgConfig = NodeToAlgConverter.configBuilder().build();
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );

        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );
        final Config config =
                NodeToAlgConverter.configBuilder()
                        .config( sqlToAlgConfig )
                        .trimUnusedFields( false )
                        .convertTableAccess( false )
                        .build();
        final SqlToAlgConverter sqlToAlgConverter = new SqlToAlgConverter( validator, statement.getTransaction().getCatalogReader(), cluster, StandardConvertletTable.INSTANCE, config );
        AlgRoot logicalRoot = sqlToAlgConverter.convertQuery( query, false, true );

        // Decorrelate
        final AlgBuilder algBuilder = config.getAlgBuilderFactory().create( cluster, null );
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
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( Collections.singletonList( LockManager.GLOBAL_LOCK ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    public void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( Collections.singletonList( Pair.of( LockManager.GLOBAL_LOCK, LockMode.EXCLUSIVE ) ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return ((SqlNode) parsed).toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql();
    }


    @Override
    public AlgDataType getParameterRowType( Node sqlNode ) {
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
