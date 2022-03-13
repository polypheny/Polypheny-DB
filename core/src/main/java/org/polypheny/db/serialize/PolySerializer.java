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

package org.polypheny.db.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory.PolyDirectorySerializer;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.PolyEdgeSerializer;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyNode.PolyNodeSerializer;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Collation.Coercibility;
import org.polypheny.db.util.NlsString;

public class PolySerializer {

    public static final Kryo kryo = new Kryo();


    static {
        kryo.setRegistrationRequired( false );
        kryo.setWarnUnregisteredClasses( true );

        // utility
        ImmutableListSerializer immutableListSerializer = new ImmutableListSerializer();
        kryo.register( ImmutableList.class, immutableListSerializer );
        kryo.register( ImmutableList.of().getClass(), immutableListSerializer );
        kryo.register( ImmutableList.of( "-" ).getClass(), immutableListSerializer );
        kryo.register( NlsString.class, new NlsStringSerializer() );
        kryo.register( ArrayList.class );
        kryo.register( List.class );
        kryo.register( PolyMap.class );
        kryo.register( BigDecimal.class );
        kryo.register( PolyNode.class, new PolyNodeSerializer() );
        kryo.register( PolyEdge.class, new PolyEdgeSerializer() );
        kryo.register( PolyDirectory.class, new PolyDirectorySerializer() );
    }


    public static byte[] serializeAndCompress( Object object ) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeflaterOutputStream deflate = new DeflaterOutputStream( out );

        Output output = new Output( deflate );
        kryo.writeObject( output, object );

        output.flush();
        output.close();
        return out.toByteArray();
    }


    @SuppressWarnings("unused")
    public static Map<?, ?> deserializeMap( String in ) {
        return deserializeAndCompress( ByteString.parseBase64( in ), PolyMap.class );
    }


    public static Enumerable<?> deserializeEnumerable( Object obj ) {
        System.out.println( "hi" );
        return null;
    }


    @SuppressWarnings("unused")
    public static <T> T deserializeAndCompress( String in, Class<T> clazz ) {
        return deserializeAndCompress( ByteString.parseBase64( in ), clazz );
    }


    public static <T> T deserializeAndCompress( byte[] in, Class<T> clazz ) {
        ByteArrayInputStream inp = new ByteArrayInputStream( in );
        InflaterInputStream inflate = new InflaterInputStream( inp );

        Input input = new Input( inflate );

        return kryo.readObject( input, clazz );
    }


    public static byte[] serializeAndCompress( PolyMap<RexLiteral, RexLiteral> map ) {
        Map<?, ?> obj = map.entrySet().stream().collect( Collectors.toMap( e -> e.getKey().getValue2(), e -> e.getValue().getValue() ) );

        return serializeAndCompress( obj );
    }


    public static class ImmutableListSerializer extends Serializer<ImmutableList<?>> {

        @Override
        public void write( Kryo kryo, Output output, ImmutableList<?> object ) {
            kryo.writeClassAndObject( output, Lists.newArrayList( object ) );
        }


        @Override
        public ImmutableList<?> read( Kryo kryo, Input input, Class<? extends ImmutableList<?>> type ) {
            Builder<?> builder = ImmutableList.builder();
            builder
                    .addAll( (List) kryo.readClassAndObject( input ) )
                    .build();

            return builder.build();
        }

    }


    public static class NlsStringSerializer extends Serializer<NlsString> {

        @Override
        public void write( Kryo kryo, Output output, NlsString value ) {
            output.writeString( value.getValue() );
            output.writeString( value.getCharsetName() );
            output.writeString( value.getCollation().getCoercibility().name() );
        }


        @Override
        public NlsString read( Kryo kryo, Input input, Class<? extends NlsString> type ) {
            String value = input.readString();
            String charsetName = input.readString();
            String coercibility = input.readString();
            return new NlsString( value, charsetName, new Collation( Coercibility.valueOf( coercibility ) ) );
        }

    }

}
