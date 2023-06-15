/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.relational;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.RelationalFrame;
import org.polypheny.db.protointerface.proto.Row;
import org.polypheny.db.protointerface.utils.PolyValueSerializer;
import org.polypheny.db.type.entity.PolyValue;

public class RelationalUtils {

    public static Row serializeToRow( List<PolyValue> row ) {
        return Row.newBuilder()
                .addAllValues( row.stream().map( PolyValueSerializer::serialize ).collect( Collectors.toList() ) )
                .build();
    }


    public static List<Row> serializeToRows( List<List<PolyValue>> rows ) {
        return rows.stream().map( RelationalUtils::serializeToRow ).collect( Collectors.toList() );
    }


    public static Frame buildRelationalFrame( long offset, boolean isLast, List<List<PolyValue>> rows, List<ColumnMeta> metas ) {
        RelationalFrame relationalFrame =  RelationalFrame.newBuilder()
                .addAllColumnMeta( metas )
                .addAllRows( serializeToRows( rows ) )
                .build();
        return Frame.newBuilder()
                .setIsLast( isLast )
                .setRelationalFrame( relationalFrame )
                .setRelationalFrame( relationalFrame )
                .build();
    }
}
