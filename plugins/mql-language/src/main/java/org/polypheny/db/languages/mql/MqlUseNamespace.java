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

package org.polypheny.db.languages.mql;

import lombok.Getter;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


@Getter
public class MqlUseNamespace extends MqlNode implements ExecutableStatement {

    private final String namespace;


    /**
     * @param pos represents the position of the statement in the parsed query
     * @param namespace the name of the namespace to use
     */
    public MqlUseNamespace( ParserPos pos, String namespace ) {
        super( pos, namespace );
        this.namespace = namespace;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        DdlManager.getInstance().createNamespace( this.namespace, DataModel.DOCUMENT, true, false, statement );
    }


    @Override
    public Type getMqlKind() {
        return Type.USE_DATABASE;
    }

}
