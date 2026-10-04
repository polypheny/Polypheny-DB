/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.io.aggregate.page;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;


/**
 * This is a dummy no operational converter required by some Parquet API's.
 */
public class NoOpColumnConverter extends GroupConverter {

    private final Converter[] converters;


    public NoOpColumnConverter( MessageType projectionSchema ) {
        this.converters = new Converter[projectionSchema.getFieldCount()];
        for ( int i = 0; i < projectionSchema.getFieldCount(); i++ ) {
            converters[i] = new NoOpPrimitiveConverter();
        }
    }


    @Override
    public Converter getConverter( int fieldIndex ) {
        return converters[fieldIndex];
    }


    @Override
    public void start() {
    }


    @Override
    public void end() {
    }


    private static class NoOpPrimitiveConverter extends PrimitiveConverter {
    }

}
