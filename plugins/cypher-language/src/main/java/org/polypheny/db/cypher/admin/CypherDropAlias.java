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

package org.polypheny.db.cypher.admin;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


@Getter
public class CypherDropAlias extends CypherAdminCommand implements ExecutableStatement {

    private final String aliasName;
    private final boolean ifExists;


    public CypherDropAlias( ParserPos pos, CypherSimpleEither<String, CypherParameter> aliasName, boolean ifExists ) {
        super( pos );
        this.aliasName = aliasName.getLeft() != null ? aliasName.getLeft() : aliasName.getRight().getName();
        this.ifExists = ifExists;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        List<LogicalNamespace> graphs = statement.getTransaction().getSnapshot().getNamespaces( new Pattern( aliasName ) );
        if ( graphs.size() != 1 ) {
            throw new GenericRuntimeException( "Error while dropping a graph database alias." );
        }
        DdlManager.getInstance().dropGraphAlias( graphs.get( 0 ).id, aliasName, ifExists );
    }


    @Override
    public boolean isDdl() {
        return true;
    }

}
