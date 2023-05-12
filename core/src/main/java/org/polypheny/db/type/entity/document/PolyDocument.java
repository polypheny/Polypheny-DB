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

package org.polypheny.db.type.entity.document;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;

@Slf4j
@EqualsAndHashCode(callSuper = true)
public class PolyDocument extends PolyMap<PolyString, PolyValue> {


    public PolyDocument( @Deserialize("map") Map<PolyString, PolyValue> value ) {
        super( value, PolyType.DOCUMENT, true );
    }


    public PolyDocument( PolyString key, PolyValue value ) {
        this( Map.of( key, value ) );
    }


    public static PolyDocument of( Map<PolyString, PolyValue> value ) {
        return new PolyDocument( value );
    }


    @SafeVarargs
    public PolyDocument( Pair<PolyString, PolyValue>... value ) {
        this( Map.ofEntries( value ) );
    }


    public static PolyDocument parse( String string ) {
        log.warn( "todo wfwefcw" );
        return null;
    }


    @Override
    public Expression asExpression() {
        return Expressions.convert_( super.asExpression(), PolyDocument.class );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDocument.class );
    }


    public static class PolyDocumentSerializerDef extends SimpleSerializerDef<PolyDocument> {

        @Override
        protected BinarySerializer<PolyDocument> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyDocument item ) {
                    out.writeLong( item.size() );
                    for ( Entry<PolyString, PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( entry.getKey().serialize() );
                        out.writeUTF8( entry.getValue().serialize() );
                    }
                }


                @Override
                public PolyDocument decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyString, PolyValue> map = new HashMap<>();
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        map.put(
                                PolyValue.deserialize( in.readUTF8() ).asString(),
                                PolyValue.deserialize( in.readUTF8() ) );
                    }
                    return PolyDocument.of( map );
                }
            };
        }

    }


    @Override
    public String toString() {
        return "{" + map.entrySet().stream().map( e -> String.format( "%s:%s", e.getKey(), e.getValue() ) ).collect( Collectors.joining( "," ) ) + "}";
    }

}
