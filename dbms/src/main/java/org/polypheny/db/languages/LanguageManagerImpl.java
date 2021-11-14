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

package org.polypheny.db.languages;

import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.core.ChainedOperatorTable;
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.core.Validator;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.NodeToRelConverter.Config;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql2rel.SqlRexConvertletTable;
import org.polypheny.db.languages.sql2rel.SqlToRelConverter;
import org.polypheny.db.languages.sql2rel.StandardConvertletTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;

public class LanguageManagerImpl extends LanguageManager {


    @Override
    public Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader ) {
        final OperatorTable opTab0 = context.config().fun( OperatorTable.class, getInstance().getStdOperatorTable() );
        final OperatorTable opTab = ChainedOperatorTable.of( opTab0, catalogReader );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Conformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, catalogReader, typeFactory, conformance );
    }


    @Override
    public Operator createOperator( String get, Kind otherFunction ) {
        return new SqlSpecialOperator( "_get", Kind.OTHER_FUNCTION );
    }


    @Override
    public NodeToRelConverter createToRelConverter( QueryLanguage sql,
            ViewExpander polyphenyDbPreparingStmt,
            Validator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            RexConvertletTable convertletTable,
            Config config ) {
        return new SqlToRelConverter( polyphenyDbPreparingStmt, (SqlValidator) validator, catalogReader, cluster, (SqlRexConvertletTable) convertletTable, config );
    }


    @Override
    public RexConvertletTable getStandardConvertlet() {
        return StandardConvertletTable.INSTANCE;
    }


    @Override
    public OperatorTable getStdOperatorTable() {
        return SqlStdOperatorTable.instance();
    }


    @Override
    public Validator createPolyphenyValidator( QueryLanguage sql, OperatorTable operatorTable, PolyphenyDbCatalogReader catalogReader, JavaTypeFactory typeFactory, Conformance conformance ) {
        return new PolyphenyDbSqlValidator( operatorTable, catalogReader, typeFactory, conformance );
    }

}
