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
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.core.Validator;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;

public abstract class LanguageManager {

    private static LanguageManager instance;


    public static LanguageManager getInstance() {
        return instance;
    }


    public static synchronized LanguageManager setAndGetInstance( LanguageManager manager ) {
        instance = manager;
        return instance;
    }


    public abstract Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader );

    public abstract Operator createOperator( String get, Kind otherFunction );

    public abstract NodeToRelConverter createToRelConverter( QueryLanguage sql,
            ViewExpander polyphenyDbPreparingStmt,
            Validator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            RexConvertletTable convertletTable,
            NodeToRelConverter.Config config );

    public abstract RexConvertletTable getStandardConvertlet();

    public abstract OperatorTable getStdOperatorTable();

    public abstract Validator createPolyphenyValidator( QueryLanguage sql, OperatorTable operatorTable, PolyphenyDbCatalogReader catalogReader, JavaTypeFactory typeFactory, Conformance conformance );

}
