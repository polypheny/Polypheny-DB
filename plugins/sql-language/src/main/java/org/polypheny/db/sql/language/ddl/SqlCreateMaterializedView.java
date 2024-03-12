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

package org.polypheny.db.sql.language.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlCreate;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.view.MaterializedViewManager;

@EqualsAndHashCode(callSuper = true)
@Value
public class SqlCreateMaterializedView extends SqlCreate implements ExecutableStatement {


    SqlIdentifier name;
    @Nullable
    SqlNodeList columns;
    @Getter
    SqlNode query;
    @Nullable List<SqlIdentifier> store;
    @Nullable String freshnessType;
    Integer freshnessTime;
    SqlIdentifier freshnessId;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE MATERIALIZED VIEW", Kind.CREATE_MATERIALIZED_VIEW );
    Snapshot snapshot = Catalog.getInstance().getSnapshot();


    /**
     * Creates a SqlCreateMaterializedView.
     */
    SqlCreateMaterializedView(
            ParserPos pos,
            boolean replace,
            boolean ifNotExists,
            SqlIdentifier name,
            @Nullable SqlNodeList columns,
            SqlNode query,
            @Nullable List<SqlIdentifier> store,
            @Nullable String freshnessType,
            Integer freshnessTime,
            SqlIdentifier freshnessId ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columns = columns;
        this.query = Objects.requireNonNull( query );
        this.store = store;
        this.freshnessType = freshnessType;
        this.freshnessTime = freshnessTime;
        this.freshnessId = freshnessId;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, columns, query );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, columns, query );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        long namespaceId;
        String viewName;

        MaterializedViewManager.getInstance().isCreatingMaterialized = true;

        if ( name.names.size() == 2 ) { // NamespaceName.ViewName
            namespaceId = snapshot.getNamespace( name.names.get( 0 ) ).orElseThrow().id;
            viewName = name.names.get( 1 );
        } else if ( name.names.size() == 1 ) { // ViewName
            namespaceId = snapshot.getNamespace( context.getDefaultNamespaceName() ).orElseThrow().id;
            viewName = name.names.get( 0 );
        } else {
            throw new GenericRuntimeException( "Invalid view name: " + name );
        }

        List<DataStore<?>> stores;
        assert store != null;
        if ( !store.isEmpty() ) {
            List<DataStore<?>> storeList = new ArrayList<>();
            store.forEach( s -> storeList.add( getDataStoreInstance( s ) ) );
            stores = storeList;
        } else {
            stores = null;
        }

        PlacementType placementType = !store.isEmpty() ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

        QueryLanguage language = QueryLanguage.from( "sql" );
        Processor sqlProcessor = statement.getTransaction().getProcessor( language );
        AlgRoot algRoot = sqlProcessor.translate( statement,
                ParsedQueryContext.builder()
                        .query( query.toString() )
                        .language( language )
                        .queryNode(
                                sqlProcessor.validate(
                                        statement.getTransaction(), this.query, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left )
                        .origin( statement.getTransaction().getOrigin() )
                        .build() );

        List<String> columns = null;

        if ( this.columns != null ) {
            columns = getColumnInfo();
        }

        // Depending on the freshness type different information is needed
        MaterializedCriteria materializedCriteria = new MaterializedCriteria();

        if ( freshnessType != null ) {
            materializedCriteria = switch ( freshnessType ) {
                case "UPDATE" -> new MaterializedCriteria( CriteriaType.UPDATE, freshnessTime );
                case "INTERVAL" -> new MaterializedCriteria( CriteriaType.INTERVAL, freshnessTime, getFreshnessType( freshnessId.toString().toLowerCase( Locale.ROOT ) ) );
                case "MANUAL" -> new MaterializedCriteria( CriteriaType.MANUAL );
                default -> materializedCriteria;
            };
        }

        boolean ordered = query.getKind().belongsTo( Kind.ORDER );

        DdlManager.getInstance().createMaterializedView(
                viewName.replaceAll( "[^A-Za-z0-9]", "_" ),
                namespaceId,
                algRoot,
                replace,
                statement,
                stores,
                placementType,
                columns,
                materializedCriteria,
                String.valueOf( query.toSqlString( PolyphenyDbSqlDialect.DEFAULT ) ),
                QueryLanguage.from( "sql" ),
                ifNotExists,
                ordered );

        MaterializedViewManager.getInstance().isCreatingMaterialized = false;
    }


    private TimeUnit getFreshnessType( String freshnessId ) {
        return switch ( freshnessId ) {
            case "min", "minutes" -> TimeUnit.MINUTES;
            case "hours" -> TimeUnit.HOURS;
            case "sec", "seconds" -> TimeUnit.SECONDS;
            case "days", "day" -> TimeUnit.DAYS;
            case "millisec", "milliseconds" -> TimeUnit.MILLISECONDS;
            default -> TimeUnit.MINUTES;
        };
    }


    private List<String> getColumnInfo() {
        List<String> columnName = new ArrayList<>();

        assert columns != null;
        for ( Ord<SqlNode> c : Ord.zip( columns.getSqlList() ) ) {
            if ( c.e instanceof SqlIdentifier sqlIdentifier ) {
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
        if ( columns != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columns.getSqlList() ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        writer.keyword( "AS" );
        writer.newlineAndIndent();
        query.unparse( writer, 0, 0 );

        if ( store != null && !store.isEmpty() ) {
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
