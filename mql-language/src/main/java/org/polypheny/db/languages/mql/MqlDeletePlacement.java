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

package org.polypheny.db.languages.mql;

import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;

public class MqlDeletePlacement extends MqlCollectionStatement implements ExecutableStatement {

    public MqlDeletePlacement( ParserPos pos, String collection, List<String> stores ) {
        super( collection, pos );
        this.stores = stores;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {

    }


    @Override
    public Type getMqlKind() {
        return Type.DELETE_PLACEMENT;
    }

}
