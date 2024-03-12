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
import java.util.Objects;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
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


/**
 * Parse tree for {@code CREATE VIEW} statement.
 */
public class SqlCreateView extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    @Getter
    private final SqlNode query;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE VIEW", Kind.CREATE_VIEW );


    private final Snapshot snapshot = Catalog.getInstance().getSnapshot();


    /**
     * Creates a SqlCreateView.
     */
    SqlCreateView(
            ParserPos pos,
            boolean replace,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlNode query ) {
        super( OPERATOR, pos, replace, false );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull( query );
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        String viewName;
        long namespaceId;

        if ( name.names.size() == 2 ) { // NamespaceName.ViewName
            namespaceId = snapshot.getNamespace( name.names.get( 0 ) ).orElseThrow().id;
            viewName = name.names.get( 1 );
        } else if ( name.names.size() == 1 ) { // ViewName
            namespaceId = snapshot.getNamespace( context.getDefaultNamespaceName() ).orElseThrow().id;
            viewName = name.names.get( 0 );
        } else {
            throw new GenericRuntimeException( "Invalid view name: %s", name );
        }

        PlacementType placementType = PlacementType.AUTOMATIC;

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

        AlgNode algNode = algRoot.alg;
        AlgCollation algCollation = algRoot.collation;

        List<String> columns = null;

        if ( columnList != null ) {
            columns = getColumnInfo();
        }

        DdlManager.getInstance().createView(
                viewName,
                namespaceId,
                algNode,
                algCollation,
                replace,
                statement,
                placementType,
                columns,
                String.valueOf( query.toSqlString( PolyphenyDbSqlDialect.DEFAULT ) ),
                QueryLanguage.from( "sql" ) );

    }


    private List<String> getColumnInfo() {
        List<String> columnName = new ArrayList<>();

        for ( Ord<Node> c : Ord.zip( columnList ) ) {
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
        if ( getReplace() ) {
            writer.keyword( "CREATE OR REPLACE" );
        } else {
            writer.keyword( "CREATE" );
        }
        writer.keyword( "VIEW" );
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( Node c : columnList ) {
                writer.sep( "," );
                ((SqlNode) c).unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        writer.keyword( "AS" );
        writer.newlineAndIndent();
        query.unparse( writer, 0, 0 );
    }


}
