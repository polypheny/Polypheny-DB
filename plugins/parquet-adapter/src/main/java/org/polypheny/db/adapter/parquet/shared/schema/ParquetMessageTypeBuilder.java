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

package org.polypheny.db.adapter.parquet.shared.schema;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema;
import org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueKind;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueSchema;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a Parquet MessageType from inferred workflow field schemas,
 * including primitive, repeated, and nested group fields.
 */
public class ParquetMessageTypeBuilder {

    private final SchemaState schemaState;
    private final String schemaName;


    public ParquetMessageTypeBuilder( SchemaState schemaState, String schemaName ) {
        this.schemaState = schemaState;
        this.schemaName = schemaName;
    }


    public MessageType build() {
        if ( schemaState.getFields().isEmpty() ) {
            throw new GenericRuntimeException( "Cannot write Parquet file without any fields." );
        }

        Types.MessageTypeBuilder builder = Types.buildMessage();
        for ( FieldSchema field : schemaState.getFields() ) {
            builder.addField( toParquetType( field.getParquetName(), field.getValueSchema() ) );
        }
        return builder.named( this.schemaName );
    }

    private static Type toParquetType( String name, ValueSchema schema ) {
        if ( schema.repeated() ) {
            return toRepeatedParquetType( name, schema.elementSchema() );
        }
        return toSingleParquetType( name, schema );
    }


    private static Type toRepeatedParquetType( String name, ValueSchema schema ) {
        if ( schema.kind() == ValueKind.GROUP ) {
            List<Type> children = new ArrayList<>( schema.nested().size() );
            for ( FieldSchema child : schema.nested() ) {
                children.add( toParquetType( child.getParquetName(), child.getValueSchema() ) );
            }
            return new org.apache.parquet.schema.GroupType( Type.Repetition.REPEATED, name, children );
        }
        return buildPrimitiveType( name, schema, Type.Repetition.REPEATED );
    }


    private static Type toSingleParquetType( String name, ValueSchema schema ) {
        if ( schema.kind() == ValueKind.GROUP ) {
            List<Type> children = new ArrayList<>( schema.nested().size() );
            for ( FieldSchema child : schema.nested() ) {
                children.add( toParquetType( child.getParquetName(), child.getValueSchema() ) );
            }
            return new org.apache.parquet.schema.GroupType( Type.Repetition.OPTIONAL, name, children );
        }
        return buildPrimitiveType( name, schema, Type.Repetition.OPTIONAL );
    }


    private static Type buildPrimitiveType( String name, ValueSchema schema, Type.Repetition repetition ) {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive( schema.primitiveTypeName(), repetition );
        if ( schema.logicalType() != null ) {
            builder.as( schema.logicalType() );
        }
        return builder.named( name );
    }
}
