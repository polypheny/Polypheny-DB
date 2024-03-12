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

package org.polypheny.db.adapter.mongodb.util;

import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.catalog.entity.logical.LogicalTable;


/**
 * Helper class, which provided multiple methods to transform PolyTypes to the correct BSON representation
 */
public class MongoTypeUtil {


    public static BsonDocument getPhysicalProjections( List<String> logicalCols, LogicalTable table ) {
        BsonDocument projections = new BsonDocument();
        List<String> names = table.getColumnNames();
        for ( String logicalCol : logicalCols ) {
            int index = names.indexOf( logicalCol );
            if ( index != -1 ) {
                projections.append( logicalCol, new BsonString( "$" + MongoStore.getPhysicalColumnName( table.getColumnIds().get( index ) ) ) );
            } else {
                projections.append( logicalCol, new BsonInt32( 1 ) );
            }
        }
        return new BsonDocument( "$project", projections );
    }


}
