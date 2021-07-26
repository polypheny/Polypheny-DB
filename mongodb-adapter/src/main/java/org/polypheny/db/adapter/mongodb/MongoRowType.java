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

package org.polypheny.db.adapter.mongodb;

import java.util.HashMap;
import java.util.List;
import org.polypheny.db.adapter.mongodb.MongoRel.Implementor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rel.type.StructKind;

public class MongoRowType extends RelRecordType {

    private final HashMap<Long, String> idToName = new HashMap<>();
    private final HashMap<String, Long> nameToId = new HashMap<>();


    public MongoRowType( StructKind kind, List<RelDataTypeField> fields, MongoTable mongoTable ) {
        super( kind, fields );
        Catalog.getInstance().getColumns( mongoTable.getCatalogTable().id ).forEach( column -> {
            idToName.put( column.id, column.name );
            nameToId.put( column.name, column.id );
        } );
    }


    public Long getId( String name ) {
        if ( name.contains( "." ) ) {
            String[] splits = name.split( "\\." );
            return nameToId.get( splits[splits.length - 1] );
        }

        return nameToId.get( name );
    }


    public String getPhysicalName( String name, Implementor implementor ) {
        String id = MongoStore.getPhysicalColumnName( getId( name ) );
        implementor.physicalMapper.add( id );
        return id;
    }


    public String getName( Long id ) {
        return idToName.get( id );
    }


    public static RelRecordType fromRecordType( RelRecordType rowType, MongoTable mongoTable ) {
        return new MongoRowType( rowType.getStructKind(), rowType.getFieldList(), mongoTable );
    }


    public boolean containsPhysicalName( String name ) {
        return nameToId.containsKey( name );
    }

}
