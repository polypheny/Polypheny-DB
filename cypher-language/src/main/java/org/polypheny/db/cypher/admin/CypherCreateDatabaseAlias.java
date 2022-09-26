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

package org.polypheny.db.cypher.admin;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


@Getter
public class CypherCreateDatabaseAlias extends CypherAdminCommand implements ExecutableStatement {

    private final boolean replace;
    private final String aliasName;
    private final String targetName; // db name
    private final boolean ifNotExists;


    public CypherCreateDatabaseAlias(
            ParserPos pos,
            boolean replace,
            CypherSimpleEither<String, CypherParameter> aliasName,
            CypherSimpleEither<String, CypherParameter> targetName,
            boolean ifNotExists ) {
        super( pos );
        this.replace = replace;
        this.aliasName = aliasName.getLeft() != null ? aliasName.getLeft() : aliasName.getRight().getName();
        this.targetName = targetName.getLeft() != null ? targetName.getLeft() : targetName.getRight().getName();
        this.ifNotExists = ifNotExists;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        List<CatalogGraphDatabase> graphs = Catalog.getInstance().getGraphs( Catalog.defaultDatabaseId, new Pattern( targetName ) );
        if ( graphs.size() != 1 ) {
            throw new RuntimeException( "Error while creating a new graph database alias." );
        }
        DdlManager.getInstance().addGraphAlias( graphs.get( 0 ).id, aliasName, ifNotExists );
    }


    @Override
    public boolean isDDL() {
        return true;
    }

}
