/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.sql.language.SqlCreate;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.view.MaterializedViewManager;

public class SqlCreateMaterializedView extends SqlCreate implements ExecutableStatement {


    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    @Getter
    private final SqlNode query;
    private final List<SqlIdentifier> store;
    private final String freshnessType;
    private final Integer freshnessTime;
    private final SqlIdentifier freshnessId;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE MATERIALIZED VIEW", Kind.CREATE_MATERIALIZED_VIEW );


    /**
     * Creates a SqlCreateMaterializedView.
     */
    SqlCreateMaterializedView(
            ParserPos pos,
            boolean replace,
            boolean ifNotExists,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlNode query,
            List<SqlIdentifier> store,
            String freshnessType,
            Integer freshnessTime,
            SqlIdentifier freshnessId ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull( query );
        this.store = store; // ON STORE [store name]; may be null
        this.freshnessType = freshnessType; // may be null, then standard values are used
        this.freshnessTime = freshnessTime;
        this.freshnessId = freshnessId;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        long schemaId;
        String viewName;

        MaterializedViewManager.getInstance().isCreatingMaterialized = true;

        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                viewName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                viewName = name.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                viewName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.databaseNotFound( name.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.schemaNotFound( name.toString() ) );
        }

        List<DataStore> stores;
        if ( store.size() > 0 ) {
            List<DataStore> storeList = new ArrayList<>();
            store.forEach( s -> storeList.add( getDataStoreInstance( s ) ) );
            stores = storeList;
        } else {
            stores = null;
        }

        PlacementType placementType = store.size() > 0 ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
        AlgRoot algRoot = sqlProcessor.translate(
                statement,
                sqlProcessor.validate(
                        statement.getTransaction(), this.query, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left, null );

        List<String> columns = null;

        if ( columnList != null ) {
            columns = getColumnInfo();
        }

        MaterializedCriteria materializedCriteria;

        // Depending on the freshness type different information is needed
        if ( freshnessType != null ) {
            switch ( freshnessType ) {
                case "UPDATE":
                    materializedCriteria = new MaterializedCriteria( CriteriaType.UPDATE, freshnessTime );
                    break;
                case "INTERVAL":
                    materializedCriteria = new MaterializedCriteria( CriteriaType.INTERVAL, freshnessTime, getFreshnessType( freshnessId.toString().toLowerCase( Locale.ROOT ) ) );
                    break;
                case "MANUAL":
                    materializedCriteria = new MaterializedCriteria( CriteriaType.MANUAL );
                    break;
                default:
                    materializedCriteria = new MaterializedCriteria();
                    break;
            }
        } else {
            materializedCriteria = new MaterializedCriteria();
        }

        boolean ordered = query.getKind().belongsTo( Kind.ORDER );

        try {
            DdlManager.getInstance().createMaterializedView(
                    viewName.replaceAll( "[^A-Za-z0-9]", "_" ),
                    schemaId,
                    algRoot,
                    replace,
                    statement,
                    stores,
                    placementType,
                    columns,
                    materializedCriteria,
                    String.valueOf( query.toSqlString( PolyphenyDbSqlDialect.DEFAULT ) ),
                    Catalog.QueryLanguage.SQL,
                    ifNotExists,
                    ordered );
        } catch ( EntityAlreadyExistsException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.tableExists( viewName ) );
        } catch ( GenericCatalogException | UnknownColumnException | ColumnNotExistsException | ColumnAlreadyExistsException e ) {
            // we just added the table/column, so it has to exist, or we have an internal problem
            throw new RuntimeException( e );
        }

        MaterializedViewManager.getInstance().isCreatingMaterialized = false;
    }


    private TimeUnit getFreshnessType( String freshnessId ) {
        TimeUnit timeUnit;
        switch ( freshnessId ) {
            case "min":
            case "minutes":
                timeUnit = TimeUnit.MINUTES;
                break;
            case "hours":
                timeUnit = TimeUnit.HOURS;
                break;
            case "sec":
            case "seconds":
                timeUnit = TimeUnit.SECONDS;
                break;
            case "days":
            case "day":
                timeUnit = TimeUnit.DAYS;
                break;
            case "millisec":
            case "milliseconds":
                timeUnit = TimeUnit.MILLISECONDS;
                break;
            default:
                timeUnit = TimeUnit.MINUTES;
                break;
        }
        return timeUnit;
    }


    private List<String> getColumnInfo() {
        List<String> columnName = new ArrayList<>();

        for ( Ord<SqlNode> c : Ord.zip( columnList.getSqlList() ) ) {
            if ( c.e instanceof SqlIdentifier ) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) c.e;
                columnName.add( sqlIdentifier.getSimple() );

            } else {
                throw new AssertionError( c.e.getClass() );
            }
        }
        return columnName;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "MATERIALIZED VIEW" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columnList.getSqlList() ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        writer.keyword( "AS" );
        writer.newlineAndIndent();
        query.unparse( writer, 0, 0 );

        if ( store.size() > 0 ) {
            writer.keyword( "ON STORE" );
            for ( SqlIdentifier s : store ) {
                s.unparse( writer, 0, 0 );
            }

        }
        if ( freshnessType != null ) {
            writer.keyword( "FRESHNESS" );

            if ( freshnessId != null ) {
                freshnessId.unparse( writer, leftPrec, rightPrec );
            }

        }
    }

}
