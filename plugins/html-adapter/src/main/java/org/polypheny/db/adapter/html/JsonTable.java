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

package org.polypheny.db.adapter.html;


import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;


/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable implements ScannableTable {

    private final Source source;


    /**
     * Creates a JsonTable.
     */
    public JsonTable( Source source ) {
        this.source = source;
    }


    public String toString() {
        return "JsonTable";
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return typeFactory.builder().add(
                "_MAP",
                null,
                typeFactory.createMapType(
                        typeFactory.createPolyType( PolyType.VARCHAR ),
                        typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.VARCHAR ), true ) ) ).build();
    }


    @Override
    public Enumerable<Object[]> scan( DataContext root ) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new JsonEnumerator( source );
            }
        };
    }

}

