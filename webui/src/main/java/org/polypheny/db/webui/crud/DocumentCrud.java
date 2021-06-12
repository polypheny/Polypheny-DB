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

package org.polypheny.db.webui.crud;

import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.transaction.Statement;

public class DocumentCrud {

    public PolyphenyDbSignature anyQuery( Statement statement, String mql ) {
        PolyphenyDbSignature signature;
        MqlProcessor mqlProcessor = statement.getTransaction().getMqlProcessor();

        MqlNode parsed = mqlProcessor.parse( mql );

        /*Pair<SqlNode, RelDataType> validated = mqlProcessor.validate( statement.getTransaction(), parsed );
        RelRoot logicalRoot = mqlProcessor.translate( statement, validated.left );

        // Prepare
        signature = statement.getQueryProcessor().prepareQuery( logicalRoot );
        */
        return null;
    }

}
