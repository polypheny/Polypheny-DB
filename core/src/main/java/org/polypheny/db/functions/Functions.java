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

package org.polypheny.db.functions;


import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.enumerable.JavaRowFormat;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.json.JsonConstructorNullClause;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryEmptyOrErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryWrapperBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.util.ProductPolyListEnumerator;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.runtime.Like;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Static;


/**
 * Helper methods to implement SQL functions in generated code.
 * <p>
 * Not present: and, or, not (builtin operators are better, because they use lazy evaluation.) Implementations do not check
 * for null values; the calling code must do that.
 * <p>
 * Many of the functions do not check for null values. This is intentional. If null arguments are possible, the
 * code-generation framework checks for nulls before calling the functions.
 */
@Deterministic
@Slf4j
public class Functions {

    private static final Gson gson = new Gson();

    private static final Function1<List<?>, Enumerable<?>> LIST_AS_ENUMERABLE = Linq4j::asEnumerable;

    @SuppressWarnings("unused")
    private static final Function1<Object[], Enumerable<Object[]>> ARRAY_CARTESIAN_PRODUCT =
            lists -> {
                final List<Enumerator<Object>> enumerators = new ArrayList<>();
                for ( Object list : lists ) {
                    enumerators.add( Linq4j.enumerator( (List<?>) list ) );
                }
                final Enumerator<List<Object>> product = Linq4j.product( enumerators );
                return new AbstractEnumerable<>() {
                    @Override
                    public Enumerator<Object[]> enumerator() {
                        return Linq4j.transform( product, List::toArray );
                    }
                };
            };

    /**
     * Holds, for each thread, a map from sequence name to sequence current value.
     * <p>
     * This is a straw man of an implementation whose main goal is to prove that sequences can be parsed, validated and planned. A real application will want persistent values for sequences, shared among threads.
     */
    private static final ThreadLocal<Map<String, AtomicLong>> THREAD_SEQUENCES = ThreadLocal.withInitial( HashMap::new );

    private static final Pattern JSON_PATH_BASE = Pattern.compile( "^\\s*(?<mode>strict|lax)\\s+(?<spec>.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE );

    private static final JsonProvider JSON_PATH_JSON_PROVIDER = new JacksonJsonProvider( PolyValue.JSON_WRAPPER.configure( Feature.ALLOW_UNQUOTED_FIELD_NAMES, true ) );
    private static final MappingProvider JSON_PATH_MAPPING_PROVIDER = new JacksonMappingProvider( PolyValue.JSON_WRAPPER );


    private Functions() {
        // empty on purpose
    }


    public static PolyDouble distance( List<PolyNumber> value, List<PolyNumber> target, PolyString metric, List<PolyNumber> weights ) {
        DistanceFunctions.verifyInputs( value, target, weights );
        return switch ( metric.value ) {
            case "L2" -> DistanceFunctions.l2MetricWeighted( value, target, weights );
            case "L1" -> DistanceFunctions.l1MetricWeighted( value, target, weights );
            case "L2SQUARED" -> DistanceFunctions.l2SquaredMetricWeighted( value, target, weights );
            case "CHISQUARED" -> DistanceFunctions.chiSquaredMetricWeighted( value, target, weights );
            case "COSINE" -> DistanceFunctions.cosineMetricWeighted( value, target, weights );
            default -> PolyDouble.of( 0.0 );
        };
    }


    public static PolyDouble distance( List<PolyNumber> value, List<PolyNumber> target, PolyString metric ) {
        DistanceFunctions.verifyInputs( value, target, null );
        return switch ( metric.value ) {
            case "L2" -> DistanceFunctions.l2Metric( value, target );
            case "L1" -> DistanceFunctions.l1Metric( value, target );
            case "L2SQUARED" -> DistanceFunctions.l2SquaredMetric( value, target );
            case "CHISQUARED" -> DistanceFunctions.chiSquaredMetric( value, target );
            case "COSINE" -> DistanceFunctions.cosineMetric( value, target );
            default -> PolyDouble.of( 0.0 );
        };
    }


    private static class MetadataModel {

        String name;
        String value;
        List<MetadataModel> tags = new ArrayList<>();


        MetadataModel( final Directory dir ) {
            this.name = dir.getName();
        }


        MetadataModel( final Tag tag ) {
            this.name = tag.getTagName();
            this.value = tag.getDescription();
        }


        MetadataModel addTag( Directory dir ) {
            tags.add( new MetadataModel( dir ) );
            return this;
        }


        MetadataModel addTag( Tag tag ) {
            tags.add( new MetadataModel( tag ) );
            return this;
        }


        @SuppressWarnings("unused")
        String toJson() {
            return new Gson().toJson( this );
        }

    }


    /**
     * @param mm Multimedia object
     * @param dirName Name of the metadata directory
     * @return All available tags within the directory, or null
     */
    public static String meta( final Object mm, final String dirName, final String tagName ) {
        Metadata metadata = getMetaData( mm );
        if ( metadata == null ) {
            return null;
        }
        for ( Directory dir : metadata.getDirectories() ) {
            if ( !dir.getName().equalsIgnoreCase( dirName ) ) {
                continue;
            }
            for ( Tag tag : dir.getTags() ) {
                if ( tag.getTagName().equalsIgnoreCase( tagName ) ) {
                    return tag.getDescription();
                }
            }
        }
        return null;
    }


    @SuppressWarnings("unused")
    public static Enumerable<?> batch( final DataContext context, final Enumerable<PolyValue> baz ) {
        List<Object> results = new ArrayList<>();

        List<Map<Long, PolyValue>> values = new ArrayList<>( context.getParameterValues() );

        if ( values.isEmpty() ) {
            return baz;
        }

        Set<Long> keys = values.get( 0 ).keySet();
        // due to the fact that the standard collector Collectors.toMap crashes with null values,
        // we use the old school way here
        Map<Long, AlgDataType> types = new HashMap<>();
        keys.forEach( k -> types.put( k, context.getParameterType( k ) ) );

        for ( Map<Long, PolyValue> value : values ) {
            context.resetParameterValues();
            value.forEach( ( k, v ) -> context.addParameterValues( k, types.get( k ), Collections.singletonList( v ) ) );

            Iterator<PolyValue> iter = baz.iterator();
            results.add( iter.next() );
        }
        return Linq4j.asEnumerable( results );
    }


    @SuppressWarnings("unused")
    public static <T> Enumerable<PolyValue[]> streamRight( final DataContext context, final Enumerable<PolyValue[]> baz, final Function0<Enumerable<PolyValue[]>> executorCall, final List<PolyType> polyTypes ) {
        AlgDataTypeFactory factory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        List<AlgDataType> algDataTypes = polyTypes.stream().map( typeName -> typeName == PolyType.ARRAY ? factory.createArrayType( factory.createPolyType( PolyType.ANY ), -1 ) : factory.createPolyType( typeName ) ).toList();

        boolean single = polyTypes.size() == 1;

        List<PolyValue[]> results = new ArrayList<>();
        List<Map<Long, PolyValue>> valuesBackup = context.getParameterValues();
        Map<Long, AlgDataType> typesBackup = context.getParameterTypes();

        if ( context.getParameterValues().isEmpty() ) {
            Enumerable<PolyValue[]> values = handleContextBatch( context, baz, executorCall, null, typesBackup, algDataTypes, results );
            if ( values != null ) {
                return values;
            }
        } else {
            // batches of inserts or selects do not care if they execute before the prepared side(right)
            // but updates need the full execution before the next batch
            for ( Map<Long, PolyValue> contextBatch : valuesBackup ) {
                Enumerable<PolyValue[]> values = handleContextBatch( context, baz, executorCall, contextBatch, typesBackup, algDataTypes, results );
                if ( values != null ) {
                    return values;
                }
            }
        }

        context.resetParameterValues();

        context.setParameterTypes( typesBackup );
        context.setParameterValues( valuesBackup );

        return Linq4j.asEnumerable( results );
    }


    @Nullable
    private static Enumerable<PolyValue[]> handleContextBatch(
            DataContext context,
            Enumerable<PolyValue[]> baz,
            Function0<Enumerable<PolyValue[]>> executorCall,
            @Nullable Map<Long, PolyValue> contextBatch,
            Map<Long, AlgDataType> typesBackup,
            List<AlgDataType> algDataTypes,
            List<PolyValue[]> results ) {
        context.setParameterTypes( typesBackup );
        if ( contextBatch != null ) {
            context.setParameterValues( List.of( contextBatch ) );
        }

        List<PolyValue[]> values = new ArrayList<>();
        for ( PolyValue[] o : baz ) {
            values.add( o );
        }
        if ( values.isEmpty() ) {
            // there are no updates to make, we don't execute the right executor
            values.add( new PolyValue[]{ PolyInteger.of( 0 ) } );
            return Linq4j.asEnumerable( values );
        }

        context.resetParameterValues();

        Map<Integer, List<PolyValue>> vals = new HashMap<>();
        for ( int i = 0; i < values.get( 0 ).length; i++ ) {
            vals.put( i, new ArrayList<>() );
        }
        for ( PolyValue[] value : values ) {
            int i = 0;
            for ( PolyValue o1 : value ) {
                vals.get( i ).add( o1 );
                i++;
            }
        }
        for ( int i = 0; i < values.get( 0 ).length; i++ ) {
            context.addParameterValues( i, algDataTypes.get( i ), vals.get( i ) );
        }

        Enumerable<PolyValue[]> executor = executorCall.apply();
        for ( PolyValue[] o : executor ) {
            results.add( o );
        }
        return null;
    }


    @SuppressWarnings("unused")
    public static Enumerable<?> enforceConstraint( DataContext context, Function0<Enumerable<PolyValue[]>> modify, Function0<Enumerable<PolyValue[]>> control, List<Class<? extends Exception>> exceptions, List<String> msgs ) {
        List<PolyValue> results = new ArrayList<>();
        try {
            for ( PolyValue[] object : modify.apply() ) {
                results.add( object[0] );
            }
        } catch ( Exception e ) {
            throw new ConstraintViolationException( Joiner.on( "\n" ).join( msgs ) );
        }

        List<PolyNumber> validationIndexes = new ArrayList<>();

        // constraint is not parameterized atm, as it would require asymmetric parameterization
        Map<Long, AlgDataType> backupTypes = context.getParameterTypes();
        List<Map<Long, PolyValue>> backupValues = context.getParameterValues();
        context.resetParameterValues();

        for ( PolyValue[] object : control.apply() ) {
            validationIndexes.add( object[1].asNumber() );
        }
        if ( validationIndexes.isEmpty() ) {
            return Linq4j.asEnumerable( results );
        } else {
            // force rollback
            throw new ConstraintViolationException( Joiner.on( "\n" )
                    .join( validationIndexes.stream().map( ( PolyNumber index ) -> msgs.get( index.intValue() ) ).toList() ) );
        }
    }


    /**
     * Correlates the elements of two sequences based on a predicate. // todo dl use EnumerablesDefault
     */
    @SuppressWarnings("unused")
    public static <TSource, TInner, TResult> Enumerable<TResult> thetaJoin(
            final Enumerable<TSource> outer,
            final Enumerable<TInner> inner,
            final Predicate2<TSource, TInner> predicate,
            Function2<TSource, TInner, TResult> resultSelector,
            final boolean generateNullsOnLeft,
            final boolean generateNullsOnRight ) {
        // Building the result as a list is easy but hogs memory. We should iterate.
        final List<TResult> result = new ArrayList<>();
        final Enumerator<TSource> lefts = outer.enumerator();
        final List<TInner> rightList = inner.toList();
        final Set<TInner> rightUnmatched;
        if ( generateNullsOnLeft ) {
            rightUnmatched = Sets.newIdentityHashSet();
            rightUnmatched.addAll( rightList );
        } else {
            rightUnmatched = null;
        }
        while ( lefts.moveNext() ) {
            int leftMatchCount = 0;
            final TSource left = lefts.current();
            final Enumerator<TInner> rights = Linq4j.iterableEnumerator( rightList );
            while ( rights.moveNext() ) {
                TInner right = rights.current();
                if ( predicate.apply( left, right ) ) {
                    ++leftMatchCount;
                    if ( rightUnmatched != null ) {
                        rightUnmatched.remove( right );
                    }
                    result.add( resultSelector.apply( left, right ) );
                }
            }
            if ( generateNullsOnRight && leftMatchCount == 0 ) {
                result.add( resultSelector.apply( left, null ) );
            }
        }
        if ( rightUnmatched != null ) {
            final Enumerator<TInner> rights =
                    Linq4j.iterableEnumerator( rightUnmatched );
            while ( rights.moveNext() ) {
                TInner right = rights.current();
                result.add( resultSelector.apply( null, right ) );
            }
        }
        return Linq4j.asEnumerable( result );
    }


    /**
     * @param mm Multimedia object
     * @param dirName Name of the metadata directory
     * @return All available tags within the directory, or null
     */
    public static String meta( final Object mm, final String dirName ) {
        Metadata metadata = getMetaData( mm );
        if ( metadata == null ) {
            return null;
        }
        List<MetadataModel> tags = new ArrayList<>();
        for ( Directory dir : metadata.getDirectories() ) {
            if ( !dir.getName().equalsIgnoreCase( dirName ) ) {
                continue;
            }
            for ( Tag tag : dir.getTags() ) {
                tags.add( new MetadataModel( tag ) );
            }
        }
        return new Gson().toJson( tags );
    }


    /**
     * @param mm Multimedia object
     * @return All available metadata of a multimedia object,
     * or null if the metadata cannot be derived
     */
    public static String meta( final Object mm ) {
        Metadata metadata = getMetaData( mm );
        if ( metadata == null ) {
            return null;
        }
        List<MetadataModel> tags = new ArrayList<>();

        for ( Directory dir : metadata.getDirectories() ) {
            MetadataModel dirModel = new MetadataModel( dir );
            for ( Tag tag : dir.getTags() ) {
                dirModel.addTag( tag );
            }
            tags.add( dirModel );
        }

        return new Gson().toJson( tags );
    }


    /**
     * @param mm Multimedia object that is of one of the following types:
     * byte[], InputStream, Blob, File
     * @return Derived metadata or null
     */
    private static Metadata getMetaData( final Object mm ) {
        try {
            if ( mm instanceof byte[] ) {
                return ImageMetadataReader.readMetadata( new ByteArrayInputStream( (byte[]) mm ) );
            } else if ( mm instanceof Blob || mm instanceof InputStream ) {
                InputStream is;
                if ( mm instanceof PushbackInputStream ) {
                    byte[] buffer = new byte[10240];
                    PushbackInputStream pbis = (PushbackInputStream) mm;
                    int len = pbis.read( buffer );
                    Metadata md = ImageMetadataReader.readMetadata( new ByteArrayInputStream( buffer ), len );
                    pbis.unread( buffer );
                    return md;
                } else if ( mm instanceof Blob ) {
                    is = ((Blob) mm).getBinaryStream();
                } else {
                    is = (InputStream) mm;
                }
                return ImageMetadataReader.readMetadata( is );
            } else if ( mm instanceof File ) {
                return ImageMetadataReader.readMetadata( (File) mm );
            } else {
                throw new RuntimeException( "Multimedia data in unexpected format " + mm.getClass().getSimpleName() );
            }
        } catch ( IOException | ImageProcessingException | SQLException e ) {
            log.debug( "Could not determine metadata of mm object", e );
            return null;
        }
    }


    /**
     * SQL SUBSTRING(string FROM ... FOR ...) function.
     */
    public static PolyString substring( PolyString c, PolyNumber s, PolyNumber l ) {
        int lc = c.value.length();
        int start = s.intValue();
        if ( start < 0 ) {
            start += lc + 1;
        }
        int e = start + l.intValue();
        if ( e < start ) {
            throw Static.RESOURCE.illegalNegativeSubstringLength().ex();
        }
        if ( start > lc || e < 1 ) {
            return PolyString.of( "" );
        }
        int s1 = Math.max( start, 1 );
        int e1 = Math.min( e, lc + 1 );
        return PolyString.of( c.value.substring( s1 - 1, e1 - 1 ) );
    }


    /**
     * SQL SUBSTRING(string FROM ...) function.
     */
    public static PolyString substring( PolyString c, PolyNumber s ) {
        return substring( c, s, PolyInteger.of( c.value.length() + 1 ) );
    }


    /**
     * SQL SUBSTRING(binary FROM ... FOR ...) function.
     */
    public static PolyBinary substring( PolyBinary c, PolyNumber s, PolyNumber l ) {
        int lc = c.value.length();
        int start = s.intValue();
        if ( start < 0 ) {
            start += lc + 1;
        }
        int e = start + l.intValue();
        if ( e < start ) {
            throw Static.RESOURCE.illegalNegativeSubstringLength().ex();
        }
        if ( start > lc || e < 1 ) {
            return PolyBinary.EMPTY;
        }
        int s1 = Math.max( start, 1 );
        int e1 = Math.min( e, lc + 1 );
        return PolyBinary.of( c.value.substring( s1 - 1, e1 - 1 ) );
    }


    /**
     * SQL SUBSTRING(binary FROM ...) function.
     */
    public static PolyBinary substring( PolyBinary c, PolyNumber s ) {
        return substring( c, s, PolyInteger.of( c.value.length() + 1 ) );
    }


    /**
     * SQL UPPER(string) function.
     */
    public static PolyString upper( PolyString s ) {
        return PolyString.of( s.value.toUpperCase( Locale.ROOT ) );
    }


    /**
     * SQL LOWER(string) function.
     */
    public static PolyString lower( PolyString s ) {
        return PolyString.of( s.value.toLowerCase( Locale.ROOT ) );
    }


    /**
     * SQL INITCAP(string) function.
     */
    public static String initcap( String s ) {
        // Assumes Alpha as [A-Za-z0-9] white space is treated as everything else.
        final int len = s.length();
        boolean start = true;
        final StringBuilder newS = new StringBuilder();

        for ( int i = 0; i < len; i++ ) {
            char curCh = s.charAt( i );
            final int c = curCh;
            if ( start ) {  // curCh is whitespace or first character of word.
                if ( c > 47 && c < 58 ) { // 0-9
                    start = false;
                } else if ( c > 64 && c < 91 ) {  // A-Z
                    start = false;
                } else if ( c > 96 && c < 123 ) {  // a-z
                    start = false;
                    curCh = (char) (c - 32); // Uppercase this character
                }
                // else {} whitespace
            } else {  // Inside of a word or white space after end of word.
                if ( c > 47 && c < 58 ) { // 0-9
                    // noop
                } else if ( c > 64 && c < 91 ) {  // A-Z
                    curCh = (char) (c + 32); // Lowercase this character
                } else if ( c > 96 && c < 123 ) {  // a-z
                    // noop
                } else { // whitespace
                    start = true;
                }
            }
            newS.append( curCh );
        } // for each character in s
        return newS.toString();
    }


    /**
     * SQL CHARACTER_LENGTH(string) function.
     */
    public static PolyNumber charLength( PolyString s ) {
        return PolyInteger.of( s.value.length() );
    }


    /**
     * SQL {@code string || string} operator.
     */
    public static PolyString concat( PolyString s0, PolyString s1 ) {
        return PolyString.of( s0.value + s1.value );
    }


    /**
     * SQL {@code binary || binary} operator.
     */
    public static PolyBinary concat( PolyBinary s0, PolyBinary s1 ) {
        return PolyBinary.of( s0.value.concat( s1.value ) );
    }


    /**
     * SQL {@code RTRIM} function applied to string.
     */
    public static String rtrim( String s ) {
        return trim( false, true, " ", s );
    }


    /**
     * SQL {@code LTRIM} function.
     */
    public static String ltrim( String s ) {
        return trim( true, false, " ", s );
    }


    /**
     * SQL {@code TRIM(... seek FROM s)} function.
     */
    public static String trim( boolean left, boolean right, String seek, String s ) {
        return trim( left, right, seek, s, true );
    }


    public static String trim( boolean left, boolean right, String seek, String s, boolean strict ) {
        if ( strict && seek.length() != 1 ) {
            throw Static.RESOURCE.trimError().ex();
        }
        int j = s.length();
        if ( right ) {
            for ( ; ; ) {
                if ( j == 0 ) {
                    return "";
                }
                if ( seek.indexOf( s.charAt( j - 1 ) ) < 0 ) {
                    break;
                }
                --j;
            }
        }
        int i = 0;
        if ( left ) {
            for ( ; ; ) {
                if ( i == j ) {
                    return "";
                }
                if ( seek.indexOf( s.charAt( i ) ) < 0 ) {
                    break;
                }
                ++i;
            }
        }
        return s.substring( i, j );
    }


    /**
     * SQL {@code TRIM} function applied to binary string.
     */
    public static ByteString trim( ByteString s ) {
        return trim_( s, true, true );
    }


    /**
     * Helper for CAST.
     */
    public static ByteString rtrim( ByteString s ) {
        return trim_( s, false, true );
    }


    /**
     * SQL {@code TRIM} function applied to binary string.
     */
    private static ByteString trim_( ByteString s, boolean left, boolean right ) {
        int j = s.length();
        if ( right ) {
            for ( ; ; ) {
                if ( j == 0 ) {
                    return ByteString.EMPTY;
                }
                if ( s.byteAt( j - 1 ) != 0 ) {
                    break;
                }
                --j;
            }
        }
        int i = 0;
        if ( left ) {
            for ( ; ; ) {
                if ( i == j ) {
                    return ByteString.EMPTY;
                }
                if ( s.byteAt( i ) != 0 ) {
                    break;
                }
                ++i;
            }
        }
        return s.substring( i, j );
    }


    /**
     * SQL {@code OVERLAY} function.
     */
    public static PolyString overlay( PolyString s, PolyString r, PolyNumber start ) {
        if ( s == null || r == null ) {
            return null;
        }
        return PolyString.of( s.value.substring( 0, start.intValue() - 1 ) + r.value + s.value.substring( start.intValue() - 1 + r.value.length() ) );
    }


    /**
     * SQL {@code OVERLAY} function.
     */
    public static PolyString overlay( PolyString s, PolyString r, PolyNumber start, PolyNumber length ) {
        if ( s == null || r == null ) {
            return null;
        }
        return PolyString.of( s.value.substring( 0, start.intValue() - 1 ) + r + s.value.substring( start.intValue() - 1 + length.intValue() ) );
    }


    /**
     * SQL {@code OVERLAY} function applied to binary strings.
     */
    public static ByteString overlay( ByteString s, ByteString r, int start ) {
        if ( s == null || r == null ) {
            return null;
        }
        return s.substring( 0, start - 1 )
                .concat( r )
                .concat( s.substring( start - 1 + r.length() ) );
    }


    /**
     * SQL {@code OVERLAY} function applied to binary strings.
     */
    public static ByteString overlay( ByteString s, ByteString r, int start, int length ) {
        if ( s == null || r == null ) {
            return null;
        }
        return s.substring( 0, start - 1 )
                .concat( r )
                .concat( s.substring( start - 1 + length ) );
    }


    @SuppressWarnings("unused")
    public static Enumerable<?> singleSum( Enumerable<PolyValue[]> results ) {
        long amount = 0;
        for ( PolyValue[] result : results ) {
            amount += result[0].asNumber().intValue();
        }
        return Linq4j.singletonEnumerable( new PolyValue[]{ PolyLong.of( amount ) } );
    }


    /**
     * SQL {@code LIKE} function.
     */
    public static PolyBoolean like( PolyString s, PolyString pattern ) {
        final String regex = Like.sqlToRegexLike( pattern.value, null );
        return PolyBoolean.of( Pattern.matches( regex, s.value ) );
    }


    /**
     * SQL {@code LIKE} function with escape.
     */
    public static PolyBoolean like( PolyString s, PolyString pattern, PolyString escape ) {
        final String regex = Like.sqlToRegexLike( pattern.value, escape.value );
        return PolyBoolean.of( Pattern.matches( regex, s.value ) );
    }


    /**
     * SQL {@code SIMILAR} function.
     */
    public static PolyBoolean similar( PolyString s, PolyString pattern ) {
        final String regex = Like.sqlToRegexSimilar( pattern.value, null );
        return PolyBoolean.of( Pattern.matches( regex, s.value ) );
    }

    // =


    /**
     * SQL <code>=</code> operator applied to Object values (including String; neither side may be null).
     */
    public static PolyBoolean eq( PolyNumber b0, PolyNumber b1 ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }
        return PolyBoolean.of( b0.bigDecimalValue().stripTrailingZeros().equals( b1.bigDecimalValue().stripTrailingZeros() ) );
    }


    /**
     * SQL <code>=</code> operator applied to Object values (at least one operand has ANY type; neither may be null).
     */
    public static PolyBoolean eq( PolyValue b0, PolyValue b1 ) {
        if ( b0.getClass().equals( b1.getClass() ) ) {
            // The result of SqlFunctions.eq(BigDecimal, BigDecimal) makes more sense than BigDecimal.equals(BigDecimal). So if both of types are BigDecimal, we just use SqlFunctions.eq(BigDecimal, BigDecimal).
            return PolyBoolean.of( b0.equals( b1 ) );
        } else if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return eq( (PolyNumber) b0, (PolyNumber) b1 );
        }
        // We shouldn't rely on implementation even though overridden equals can handle other types which may create worse result: for example, a.equals(b) != b.equals(a)
        return PolyBoolean.FALSE;
    }


    /**
     * Returns whether two objects can both be assigned to a given class.
     */
    @SuppressWarnings("unused")
    private static boolean allAssignable( Class<Number> clazz, Object o0, Object o1 ) {
        return clazz.isInstance( o0 ) && clazz.isInstance( o1 );
    }


    private static boolean allAssignablePoly( Class<? extends PolyValue> clazz, Object o0, Object o1 ) {
        return clazz.isInstance( o0 ) && clazz.isInstance( o1 );
    }

    // <>


    /**
     * SQL <code>&lt;gt;</code> operator applied to Object values (including String; neither side may be null).
     */
    public static PolyBoolean ne( PolyValue b0, PolyValue b1 ) {
        return PolyBoolean.of( b0.compareTo( b1 ) != 0 );
    }

    // <


    /**
     * SQL <code>&lt;</code> operator applied to Object values.
     */
    public static PolyBoolean lt( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return lt( b0.asNumber(), b1.asNumber() );
        }

        if ( b0.isSameType( b1 ) ) {
            return PolyBoolean.of( b0.compareTo( b1 ) < 0 );
        }

        throw notComparable( "<", b0, b1 );
    }


    public static PolyBoolean lt( PolyNumber b0, PolyNumber b1 ) {
        return PolyBoolean.of( PolyNumber.compareTo( b0, b1 ) < 0 );
    }


    public static PolyBoolean lt( PolyTemporal b0, PolyTemporal b1 ) {
        return lt( PolyLong.of( b0.getMillisSinceEpoch() ), PolyLong.of( b1.getMillisSinceEpoch() ) );
    }


    public static PolyBoolean lt( PolyTemporal b0, PolyNumber b1 ) {
        return lt( PolyLong.of( b0.getMillisSinceEpoch() ), b1 );
    }


    public static PolyBoolean lt( PolyNumber b0, PolyTemporal b1 ) {
        return lt( b0, PolyLong.of( b1.getMillisSinceEpoch() ) );
    }
    // <=


    /**
     * SQL <code>&le;</code> operator applied to boolean values.
     */
    public static PolyBoolean le( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return le( b0.asNumber(), b1.asNumber() );
        }

        if ( b0.isSameType( b1 ) ) {
            return PolyBoolean.of( b0.compareTo( b1 ) <= 0 );
        }

        throw notComparable( "<=", b0, b1 );
    }


    public static PolyBoolean le( PolyNumber b0, PolyNumber b1 ) {
        return PolyBoolean.of( b0.compareTo( b1 ) <= 0 );
    }

    // >


    /**
     * SQL <code>&gt;</code> operator applied to boolean values.
     */
    public static PolyBoolean gt( PolyNumber b0, PolyNumber b1 ) {
        return PolyBoolean.of( b0.bigDecimalValue().compareTo( b1.bigDecimalValue() ) > 0 );
    }


    /**
     * SQL <code>&gt;</code> operator applied to Object values (at least one
     * operand has ANY type; neither may be null).
     */
    public static PolyBoolean gt( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return gt( b0.asNumber(), b1.asNumber() );
        }

        if ( b0.isSameType( b1 ) ) {
            return PolyBoolean.of( b0.compareTo( b1 ) > 0 );
        }

        throw notComparable( ">", b0, b1 );
    }

    // >=


    /**
     * SQL <code>&ge;</code> operator applied to BigDecimal values.
     */
    public static PolyBoolean ge( PolyNumber b0, PolyNumber b1 ) {
        return PolyBoolean.of( b0.bigDecimalValue().compareTo( b1.bigDecimalValue() ) >= 0 );
    }


    public static PolyBoolean ge( PolyTemporal b0, PolyTemporal b1 ) {
        return ge( PolyLong.of( b0.getMillisSinceEpoch() ), PolyLong.of( b1.getMillisSinceEpoch() ) );
    }


    /**
     * SQL <code>&ge;</code> operator applied to Object values (at least one
     * operand has ANY type; neither may be null).
     */
    public static PolyBoolean ge( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return ge( b0.asNumber(), b1.asNumber() );
        }

        if ( allAssignablePoly( PolyTemporal.class, b0, b1 ) ) {
            return ge( b0.asTemporal(), b1.asTemporal() );
        }

        if ( b0.isSameType( b1 ) ) {
            return PolyBoolean.of( b0.compareTo( b1 ) >= 0 );
        }

        throw notComparable( ">=", b0, b1 );

    }

    // +


    /**
     * SQL <code>+</code> operator applied to BigDecimal values.
     */
    public static PolyNumber plus( PolyNumber b0, PolyNumber b1 ) {
        return (b0 == null || b1 == null) ? null : b0.plus( b1 );
    }


    /**
     * SQL <code>+</code> operator applied to Object values (at least one operand has ANY type; either may be null).
     */
    public static PolyValue plusAny( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return null;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return plus( b0.asNumber(), b1.asNumber() );
        }

        throw notArithmetic( "+", b0, b1 );
    }

    // -


    /**
     * SQL <code>-</code> operator applied to BigDecimal values.
     */
    public static PolyNumber minus( PolyNumber b0, PolyNumber b1 ) {
        return (b0 == null || b1 == null) ? null : b0.subtract( b1 );
    }


    public static PolyValue minus( PolyValue b0, PolyValue b1 ) {
        return minusAny( b0, b1 );
    }


    /**
     * SQL <code>-</code> operator applied to Object values (at least one operand has ANY type; either may be null).
     */
    public static PolyValue minusAny( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return null;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return minus( b0.asNumber(), b1.asNumber() );
        }

        throw notArithmetic( "-", b0, b1 );
    }

    // /


    /**
     * SQL <code>/</code> operator applied to Object values (at least one operand has ANY type; either may be null).
     */
    public static PolyValue divideAny( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return null;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return divide( b0.asNumber(), b1.asNumber() );
        }

        throw notArithmetic( "/", b0, b1 );
    }


    public static PolyNumber divide( PolyNumber b0, PolyNumber b1 ) {
        return (b0 == null || b1 == null)
                ? null
                : b0.divide( b1 );
    }


    /**
     * SQL <code>*</code> operator applied to BigDecimal values.
     */
    public static PolyValue multiply( PolyNumber b0, PolyNumber b1 ) {
        return (b0 == null || b1 == null) ? null : b0.multiply( b1 );
    }


    public static PolyInterval multiply( PolyInterval b0, PolyNumber b1 ) {
        return PolyInterval.of( b0.value.multiply( b1.bigDecimalValue() ), b0.qualifier );
    }


    public static PolyValue multiply( PolyValue b0, PolyValue b1 ) {
        return multiplyAny( b1, b0 );
    }


    /**
     * SQL <code>*</code> operator applied to Object values (at least one operand has ANY type; either may be null).
     */
    public static PolyValue multiplyAny( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return null;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return multiply( b0.asNumber(), b1.asNumber() );
        }

        throw notArithmetic( "*", b0, b1 );
    }


    private static RuntimeException notArithmetic(
            String op, Object b0,
            Object b1 ) {
        return Static.RESOURCE.invalidTypesForArithmetic( b0.getClass().toString(),
                op, b1.getClass().toString() ).ex();
    }


    private static RuntimeException notComparable(
            String op, Object b0,
            Object b1 ) {
        return Static.RESOURCE.invalidTypesForComparison( b0.getClass().toString(),
                op, b1.getClass().toString() ).ex();
    }

    // &


    /**
     * Helper function for implementing <code>BIT_AND</code>
     */
    @SuppressWarnings("unused")
    public static long bitAnd( long b0, long b1 ) {
        return b0 & b1;
    }

    // |


    /**
     * Helper function for implementing <code>BIT_OR</code>
     */
    @SuppressWarnings("unused")
    public static long bitOr( long b0, long b1 ) {
        return b0 | b1;
    }

    // EXP


    /**
     * SQL <code>EXP</code> operator applied to double values.
     */
    public static PolyNumber exp( PolyNumber number ) {
        return PolyBigDecimal.of( Math.exp( number.doubleValue() ) );
    }

    // POWER


    /**
     * SQL <code>POWER</code> operator applied to double values.
     */
    public static PolyNumber power( PolyNumber base, PolyNumber exp ) {
        return PolyDouble.of( Math.pow( base.doubleValue(), exp.doubleValue() ) );
    }

    // LN


    /**
     * SQL {@code LN(number)} function applied to double values.
     */
    public static PolyNumber ln( PolyNumber number ) {
        return PolyBigDecimal.of( Math.log( number.doubleValue() ) );
    }

    // LOG10


    /**
     * SQL <code>LOG10(numeric)</code> operator applied to double values.
     */
    public static PolyNumber log10( PolyNumber number ) {
        return PolyBigDecimal.of( Math.log10( number.doubleValue() ) );
    }

    // MOD


    /**
     * SQL <code>MOD</code> operator applied to byte values.
     */
    public static PolyNumber mod( PolyNumber b0, PolyNumber b1 ) {
        final BigDecimal[] bigDecimals = b0.bigDecimalValue().divideAndRemainder( b1.bigDecimalValue() );
        return PolyBigDecimal.of( bigDecimals[1] );
    }


    public static PolyNumber mod( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null ) {
            return null;
        }

        if ( allAssignablePoly( PolyNumber.class, b0, b1 ) ) {
            return mod( b0.asNumber(), b1.asNumber() );
        }

        throw notArithmetic( "mod", b0, b1 );
    }

    // FLOOR


    public static PolyNumber floor( PolyNumber b0 ) {
        log.warn( "optimize" );
        return PolyBigDecimal.of( b0.bigDecimalValue().setScale( 0, RoundingMode.FLOOR ) );
    }


    /**
     * SQL <code>FLOOR</code> operator applied to byte values.
     */
    public static PolyNumber floor( PolyNumber b0, PolyNumber b1 ) {
        return b0.floor( b1 );
    }

    // CEIL


    public static PolyNumber ceil( PolyNumber b0 ) {
        return PolyBigDecimal.of( b0.bigDecimalValue().setScale( 0, RoundingMode.CEILING ) );
    }


    /**
     * SQL <code>CEIL</code> operator applied to byte values.
     */
    public static PolyNumber ceil( PolyNumber b0, PolyNumber b1 ) {
        return b0.ceil( b1 );
    }

    // ABS


    /**
     * SQL <code>ABS</code> operator applied to byte values.
     */
    public static PolyNumber abs( PolyNumber number ) {
        return PolyBigDecimal.of( number.bigDecimalValue().abs() );
    }

    // ACOS


    /**
     * SQL <code>ACOS</code> operator applied to BigDecimal values.
     */
    public static PolyNumber acos( PolyNumber b0 ) {
        return PolyDouble.of( Math.acos( b0.doubleValue() ) );
    }


    /**
     * SQL <code>ACOS</code> operator applied to double values.
     */
    public static PolyNumber asin( PolyNumber b0 ) {
        return PolyDouble.of( Math.asin( b0.doubleValue() ) );
    }


    /**
     * SQL <code>ASIN</code> operator applied to double values.
     */
    public static PolyNumber atan( PolyNumber b0 ) {
        return PolyDouble.of( Math.atan( b0.doubleValue() ) );
    }


    /**
     * SQL <code>ATAN</code> operator applied to double values.
     */
    public static PolyNumber atan2( PolyNumber b0, PolyNumber b1 ) {
        return PolyDouble.of( Math.atan2( b0.doubleValue(), b1.doubleValue() ) );
    }


    /**
     * SQL <code>ATAN2</code> operator applied to BigDecimal/double values.
     */
    public static PolyNumber cos( PolyNumber b0 ) {
        return PolyDouble.of( Math.cos( b0.doubleValue() ) );
    }


    /**
     * SQL <code>COS</code> operator applied to double values.
     */
    public static PolyNumber cot( PolyNumber b0 ) {
        return PolyDouble.of( 1.0d / Math.tan( b0.doubleValue() ) );
    }


    /**
     * SQL <code>COT</code> operator applied to double values.
     */
    public static PolyNumber degrees( PolyNumber b0 ) {
        return PolyDouble.of( Math.toDegrees( b0.doubleValue() ) );
    }


    /**
     * SQL <code>DEGREES</code> operator applied to double values.
     */
    public static PolyDouble radians( PolyNumber b0 ) {
        return PolyDouble.of( Math.toRadians( b0.doubleValue() ) );
    }


    /**
     * SQL <code>ROUND</code> operator applied to BigDecimal values.
     */
    public static PolyNumber sround( PolyNumber b0, PolyNumber b1 ) {
        return PolyBigDecimal.of( b0.bigDecimalValue().movePointRight( b1.intValue() ).setScale( 0, RoundingMode.HALF_UP ).movePointLeft( b1.intValue() ) );
    }


    /**
     * SQL <code>ROUND</code> operator applied to double values.
     */
    @SuppressWarnings("unused")
    public static PolyNumber struncate( PolyNumber b0 ) {
        return struncate( b0, PolyInteger.ZERO );
    }


    public static PolyNumber struncate( PolyNumber b0, PolyNumber b1 ) {
        return PolyBigDecimal.of( b0.bigDecimalValue().movePointRight( b1.intValue() ).setScale( 0, RoundingMode.DOWN ).movePointLeft( b1.intValue() ) );
    }


    /**
     * SQL <code>TRUNCATE</code> operator applied to long values.
     */
    public static PolyBigDecimal sign( PolyNumber b0 ) {
        return PolyBigDecimal.of( b0.bigDecimalValue().signum() );
    }


    /**
     * SQL <code>SIGN</code> operator applied to double values.
     */
    public static PolyNumber sin( PolyNumber b0 ) {
        return PolyDouble.of( Math.sin( b0.doubleValue() ) );
    }


    /**
     * SQL <code>SIN</code> operator applied to double values.
     */
    public static PolyNumber tan( PolyNumber b0 ) {
        return PolyDouble.of( Math.tan( b0.doubleValue() ) );
    }


    /**
     * SQL <code>TAN</code> operator applied to double values.
     */
    public static <T extends Comparable<T>> T lesser( T b0, T b1 ) {
        return b0 == null || b0.compareTo( b1 ) > 0 ? b1 : b0;
    }


    /**
     * LEAST operator.
     */
    public static <T extends Comparable<T>> T least( T b0, T b1 ) {
        return b0 == null || b1 != null && b0.compareTo( b1 ) > 0 ? b1 : b0;
    }


    public static boolean greater( boolean b0, boolean b1 ) {
        return b0 || b1;
    }


    public static byte greater( byte b0, byte b1 ) {
        return b0 > b1 ? b0 : b1;
    }


    public static char greater( char b0, char b1 ) {
        return b0 > b1 ? b0 : b1;
    }


    public static char lesser( char b0, char b1 ) {
        return b0 > b1 ? b1 : b0;
    }


    public static short greater( short b0, short b1 ) {
        return b0 > b1 ? b0 : b1;
    }


    public static int greater( int b0, int b1 ) {
        return Math.max( b0, b1 );
    }


    public static long greater( long b0, long b1 ) {
        return Math.max( b0, b1 );
    }


    public static float greater( float b0, float b1 ) {
        return Math.max( b0, b1 );
    }


    public static double greater( double b0, double b1 ) {
        return Math.max( b0, b1 );
    }


    public static PolyNumber lesser( PolyNumber b0, PolyNumber b1 ) {
        return b0 == null ? b1
                : b1 == null ? null
                        : b0.bigDecimalValue().compareTo( b1.bigDecimalValue() ) < 0 ? b0 : b1;
    }


    /**
     * Helper for implementing MAX. Somewhat similar to GREATEST operator.
     */
    public static <T extends Comparable<T>> T greater( T b0, T b1 ) {
        return b0 == null || b0.compareTo( b1 ) < 0 ? b1 : b0;
    }


    /**
     * GREATEST operator.
     */
    @SuppressWarnings("unused")
    public static <T extends Comparable<T>> T greatest( T b0, T b1 ) {
        return b0 == null || b1 != null && b0.compareTo( b1 ) < 0 ? b1 : b0;
    }


    /**
     * Boolean comparison.
     */
    public static int compare( boolean x, boolean y ) {
        return x == y ? 0 : x ? 1 : -1;
    }


    /**
     * CAST(FLOAT AS VARCHAR).
     */
    public static String toString( float x ) {
        if ( x == 0 ) {
            return "0E0";
        }
        BigDecimal bigDecimal = new BigDecimal( x, MathContext.DECIMAL32 ).stripTrailingZeros();
        final String s = bigDecimal.toString();
        return s.replaceAll( "0*E", "E" ).replace( "E+", "E" );
    }


    /**
     * CAST(DOUBLE AS VARCHAR).
     */
    public static String toString( double x ) {
        if ( x == 0 ) {
            return "0E0";
        }
        BigDecimal bigDecimal = new BigDecimal( x, MathContext.DECIMAL64 ).stripTrailingZeros();
        final String s = bigDecimal.toString();
        return s.replaceAll( "0*E", "E" ).replace( "E+", "E" );
    }


    /**
     * CAST(DECIMAL AS VARCHAR).
     */
    public static String toString( BigDecimal x ) {
        final String s = x.toString();
        if ( s.startsWith( "0" ) ) {
            // we want ".1" not "0.1"
            return s.substring( 1 );
        } else if ( s.startsWith( "-0" ) ) {
            // we want "-.1" not "-0.1"
            return "-" + s.substring( 2 );
        } else {
            return s;
        }
    }


    /**
     * CAST(BOOLEAN AS VARCHAR).
     */
    public static PolyString toString( PolyBoolean x ) {
        // Boolean.toString returns lower case -- no good.
        return x.value ? PolyString.of( "TRUE" ) : PolyString.of( "FALSE" );
    }


    @NonDeterministic
    private static Object cannotConvert( Object o, Class<?> toType ) {
        throw Static.RESOURCE.cannotConvert( o.toString(), toType.toString() ).ex();
    }


    /**
     * CAST(VARCHAR AS BOOLEAN).
     */
    @SuppressWarnings("unused")
    public static PolyBoolean toBoolean( PolyString s ) {
        s = PolyString.of( trim( true, true, " ", s.toString() ) );
        if ( s.value.equalsIgnoreCase( "TRUE" ) ) {
            return PolyBoolean.TRUE;
        } else if ( s.value.equalsIgnoreCase( "FALSE" ) ) {
            return PolyBoolean.FALSE;
        } else {
            throw Static.RESOURCE.invalidCharacterForCast( s.value ).ex();
        }
    }


    @SuppressWarnings("unused")
    public static PolyBoolean toBoolean( PolyNumber number ) {
        return number.asBoolean();
    }


    @SuppressWarnings("unused")
    public static boolean toBoolean( Object o ) {
        return o instanceof Boolean ? (Boolean) o
                : o instanceof Number ? toBoolean( (Number) o )
                        : o instanceof String ? toBoolean( (String) o )
                                : (Boolean) cannotConvert( o, boolean.class );
    }

    // Don't need parseByte etc. - Byte.parseByte is sufficient.


    @SuppressWarnings("unused")
    public static byte toByte( Object o ) {
        return o instanceof Byte ? (Byte) o
                : o instanceof Number ? toByte( (Number) o )
                        : Byte.parseByte( o.toString() );
    }


    public static byte toByte( Number number ) {
        return number.byteValue();
    }


    @SuppressWarnings("unused")
    public static char toChar( String s ) {
        return s.charAt( 0 );
    }


    @SuppressWarnings("unused")
    public static Character toCharBoxed( String s ) {
        return s.charAt( 0 );
    }


    public static short toShort( String s ) {
        return Short.parseShort( s.trim() );
    }


    public static short toShort( Number number ) {
        return number.shortValue();
    }


    @SuppressWarnings("unused")
    public static short toShort( Object o ) {
        return o instanceof Short ? (Short) o
                : o instanceof Number ? toShort( (Number) o )
                        : o instanceof String ? toShort( (String) o )
                                : (Short) cannotConvert( o, short.class );
    }


    public static int toInt( String s ) {
        return Integer.parseInt( s.trim() );
    }


    public static int toInt( Number number ) {
        return number.intValue();
    }


    @SuppressWarnings("unused")
    public static int toInt( Object o ) {
        return o instanceof Integer ? (Integer) o
                : o instanceof Number ? toInt( (Number) o )
                        : o instanceof String ? toInt( (String) o )
                                : o instanceof java.util.Date ? toInt( (java.util.Date) o )
                                        : o instanceof java.util.GregorianCalendar ? toInt( ((java.util.GregorianCalendar) o).getTime() ) // hack for views for now
                                                : o instanceof org.polypheny.db.util.DateString ? toInt( new Date( ((org.polypheny.db.util.DateString) o).getMillisSinceEpoch() ) ) // hack for views for now
                                                        : (Integer) cannotConvert( o, int.class );
    }


    public static long toLong( String s ) {
        if ( s.startsWith( "199" ) && s.contains( ":" ) ) {
            return Timestamp.valueOf( s ).getTime();
        }
        return Long.parseLong( s.trim() );
    }


    public static long toLong( Number number ) {
        return number.longValue();
    }


    @SuppressWarnings("unused")
    public static long toLong( Object o ) {
        return o instanceof Long ? (Long) o
                : o instanceof Number ? toLong( (Number) o )
                        : o instanceof String ? toLong( (String) o )
                                : (Long) cannotConvert( o, long.class );
    }


    public static float toFloat( String s ) {
        return Float.parseFloat( s.trim() );
    }


    public static float toFloat( Number number ) {
        return number.floatValue();
    }


    @SuppressWarnings("unused")
    public static float toFloat( Object o ) {
        return o instanceof Float ? (Float) o
                : o instanceof Number ? toFloat( (Number) o )
                        : o instanceof String ? toFloat( (String) o )
                                : (Float) cannotConvert( o, float.class );
    }


    public static double toDouble( String s ) {
        return Double.parseDouble( s.trim() );
    }


    public static double toDouble( Number number ) {
        return number.doubleValue();
    }


    @SuppressWarnings("unused")
    public static double toDouble( Object o ) {
        return o instanceof Double ? (Double) o
                : o instanceof Number ? toDouble( (Number) o )
                        : o instanceof String ? toDouble( (String) o )
                                : (Double) cannotConvert( o, double.class );
    }


    public static BigDecimal toBigDecimal( String s ) {
        return new BigDecimal( s.trim() );
    }


    public static BigDecimal toBigDecimal( Number number ) {
        // There are some values of "long" that cannot be represented as "double".
        // Not so "int". If it isn't a long, go straight to double.
        return number instanceof BigDecimal ? (BigDecimal) number
                : number instanceof BigInteger ? new BigDecimal( (BigInteger) number )
                        : number instanceof Long ? new BigDecimal( number.longValue() )
                                : BigDecimal.valueOf( number.doubleValue() );
    }


    public static BigDecimal toBigDecimal( Object o ) {
        return o instanceof Number
                ? toBigDecimal( (Number) o )
                : toBigDecimal( o.toString() );
    }


    /**
     * Helper for CAST(... AS VARCHAR(maxLength)).
     */
    public static PolyString truncate( PolyString s, int maxLength ) {
        if ( s == null || s.value == null ) {
            return null;
        } else if ( s.value.length() > maxLength ) {
            return PolyString.of( s.value.substring( 0, maxLength ) );
        } else {
            return s;
        }
    }


    public static PolyBinary truncate( PolyBinary s, int maxLength ) {
        if ( s == null || s.value == null ) {
            return null;
        } else if ( s.value.length() > maxLength ) {
            return PolyBinary.of( s.value.substring( 0, maxLength ) );
        } else {
            return s;
        }
    }


    /**
     * Helper for CAST(... AS CHAR(maxLength)).
     */
    @SuppressWarnings("unused")
    public static PolyString truncateOrPad( PolyString s, int maxLength ) {
        if ( s == null || s.value == null ) {
            return null;
        } else {
            final int length = s.value.length();
            if ( length > maxLength ) {
                return PolyString.of( s.value.substring( 0, maxLength ) );
            } else {
                return length < maxLength ? PolyString.of( Spaces.padRight( s.value, maxLength ) ) : s;
            }
        }
    }


    /**
     * Helper for CAST(... AS VARBINARY(maxLength)).
     */
    public static ByteString truncate( ByteString s, int maxLength ) {
        if ( s == null ) {
            return null;
        } else if ( s.length() > maxLength ) {
            return s.substring( 0, maxLength );
        } else {
            return s;
        }
    }


    /**
     * Helper for CAST(... AS BINARY(maxLength)).
     */
    @SuppressWarnings("unused")
    public static ByteString truncateOrPad( ByteString s, int maxLength ) {
        if ( s == null ) {
            return null;
        } else {
            final int length = s.length();
            if ( length > maxLength ) {
                return s.substring( 0, maxLength );
            } else if ( length < maxLength ) {
                return s.concat( new ByteString( new byte[maxLength - length] ) );
            } else {
                return s;
            }
        }
    }


    /**
     * SQL {@code POSITION(seek IN string)} function.
     */
    public static PolyNumber position( PolyString seek, PolyString s ) {
        return PolyInteger.of( s.value.indexOf( seek.value ) + 1 );
    }


    /**
     * SQL {@code POSITION(seek IN string)} function for byte strings.
     */
    public static PolyNumber position( PolyBinary seek, PolyBinary s ) {
        return PolyInteger.of( s.value.indexOf( seek.value ) + 1 );
    }


    /**
     * SQL {@code POSITION(seek IN string FROM integer)} function.
     */
    public static PolyNumber position( PolyString seek, PolyString s, PolyNumber from ) {
        final int from0 = from.intValue() - 1; // 0-based
        if ( from0 > s.value.length() || from0 < 0 ) {
            return PolyInteger.of( 0 );
        }

        return PolyInteger.of( s.value.indexOf( seek.value, from0 ) + 1 );
    }


    /**
     * SQL {@code POSITION(seek IN string FROM integer)} function for byte strings.
     */
    public static PolyNumber position( PolyBinary seek, PolyBinary s, PolyNumber from ) {
        final int from0 = from.intValue() - 1;
        if ( from0 > s.value.length() || from0 < 0 ) {
            return PolyInteger.of( 0 );
        }

        final int p = s.value.indexOf( seek.value, from0 );
        if ( p < 0 ) {
            return PolyInteger.of( 0 );
        }
        return PolyInteger.of( p + from.intValue() );
    }


    /**
     * Helper for rounding. Truncate(12345, 1000) returns 12000.
     */
    public static long round( long v, long x ) {
        return truncate( v + x / 2, x );
    }


    /**
     * Helper for rounding. Truncate(12345, 1000) returns 12000.
     */
    public static long truncate( long v, long x ) {
        long remainder = v % x;
        if ( remainder < 0 ) {
            remainder += x;
        }
        return v - remainder;
    }


    /**
     * Helper for rounding. Truncate(12345, 1000) returns 12000.
     */
    public static int round( int v, int x ) {
        return truncate( v + x / 2, x );
    }


    /**
     * Helper for rounding. Truncate(12345, 1000) returns 12000.
     */
    public static int truncate( int v, int x ) {
        int remainder = v % x;
        if ( remainder < 0 ) {
            remainder += x;
        }
        return v - remainder;
    }


    /**
     * SQL {@code TRANSLATE(string, search_chars, replacement_chars)} function.
     */
    public static String translate3( String s, String search, String replacement ) {
        return org.apache.commons.lang3.StringUtils.replaceChars( s, search, replacement );
    }


    /**
     * SQL {@code REPLACE(string, search, replacement)} function.
     */
    public static String replace( String s, String search, String replacement ) {
        return s.replace( search, replacement );
    }


    /**
     * Helper for "array element reference". Caller has already ensured that array and index are not null. Index is 1-based, per SQL.
     */
    public static PolyValue arrayItem( List<PolyValue> list, PolyNumber item ) {
        if ( item.intValue() < 1 || item.intValue() > list.size() ) {
            return null;
        }
        return list.get( item.intValue() - 1 );
    }


    /**
     * Helper for "map element reference". Caller has already ensured that array and index are not null. Index is 1-based, per SQL.
     */
    public static PolyValue mapItem( Map<PolyValue, PolyValue> map, PolyValue item ) {
        return map.get( item );
    }


    /**
     * Implements the {@code [ ... ]} operator on an object whose type is not known until runtime.
     */
    public static PolyValue item( PolyValue object, PolyValue index ) {
        if ( object.isMap() ) {
            return mapItem( object.asMap(), index );
        }
        if ( object.isList() && index.isNumber() ) {
            return arrayItem( object.asList(), index.asNumber() );
        }
        return null;
    }


    /**
     * As {@link #arrayItem} method, but allows array to be nullable.
     */
    @SuppressWarnings("unused")
    public static PolyValue arrayItemOptional( List<PolyValue> list, PolyNumber item ) {
        if ( list == null ) {
            return null;
        }
        return arrayItem( list, item );
    }


    /**
     * As {@link #mapItem} method, but allows map to be nullable.
     */
    @SuppressWarnings("unused")
    public static PolyValue mapItemOptional( Map<PolyValue, PolyValue> map, PolyValue item ) {
        if ( map == null ) {
            return null;
        }
        return mapItem( map, item );
    }


    /**
     * As {@link #item} method, but allows object to be nullable.
     */
    @SuppressWarnings("unused")
    public static PolyValue itemOptional( Map<PolyValue, PolyValue> object, PolyValue index ) {
        if ( object == null ) {
            return null;
        }
        return item( (PolyValue) object, index );
    }


    @SuppressWarnings("unused")
    public static List<?> reparse( String value ) {
        //Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        if ( value == null ) {
            return null;
        }
        return PolyValue.fromTypedJson( value, PolyList.class );
    }


    public static PolyList<?> reparse( PolyString value ) {
        //Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        if ( value == null || value.isNull() ) {
            return null;
        }
        return PolyValue.fromTypedJson( value.value, PolyList.class );
    }


    /**
     * NULL &rarr; FALSE, FALSE &rarr; FALSE, TRUE &rarr; TRUE.
     */
    public static PolyBoolean isTrue( Boolean b ) {
        return PolyBoolean.of( b != null && b );
    }


    public static PolyBoolean isTrue( PolyBoolean b ) {
        return PolyBoolean.of( b != null && b.value );
    }


    /**
     * NULL &rarr; FALSE, FALSE &rarr; TRUE, TRUE &rarr; FALSE.
     */
    @SuppressWarnings("unused")
    public static boolean isFalse( Boolean b ) {
        return b != null && !b;
    }


    @SuppressWarnings("unused")
    public static PolyBoolean isFalse( PolyBoolean b ) {
        return PolyBoolean.of( b != null && !b.value );
    }


    /**
     * NULL &rarr; TRUE, FALSE &rarr; TRUE, TRUE &rarr; FALSE.
     */
    @SuppressWarnings("unused")
    public static boolean isNotTrue( Boolean b ) {
        return b == null || !b;
    }


    @SuppressWarnings("unused")
    public static PolyBoolean isNotTrue( PolyBoolean b ) {
        return PolyBoolean.of( b == null || !b.value );
    }


    /**
     * NULL &rarr; TRUE, FALSE &rarr; FALSE, TRUE &rarr; TRUE.
     */
    @SuppressWarnings("unused")
    public static boolean isNotFalse( Boolean b ) {
        return b == null || b;
    }


    @SuppressWarnings("unused")
    public static PolyBoolean isNotFalse( PolyBoolean b ) {
        return PolyBoolean.of( b == null || b.value );
    }


    /**
     * NULL &rarr; NULL, FALSE &rarr; TRUE, TRUE &rarr; FALSE.
     */
    public static PolyBoolean not( PolyBoolean b ) {
        return PolyBoolean.of( !b.value );
    }


    public static PolyBoolean not( boolean b ) {
        return PolyBoolean.of( !b );
    }


    public static PolyBoolean not( Boolean b ) {
        return PolyBoolean.of( !b );
    }


    @SuppressWarnings("unused")
    public static List<PolyValue> arrayToPolyList( final java.sql.Array a, Function1<Object, PolyValue> transformer, int depth ) {
        if ( a == null ) {
            return null;
        }
        Object[] array = arrayToList( a ).toArray();
        return applyToLowest( array, transformer ).asList();
    }


    private static PolyValue applyToLowest( Object o, Function1<Object, PolyValue> transformer ) {
        if ( o instanceof Object[] ) {
            return PolyList.of( Arrays.stream( ((Object[]) o) ).map( a -> applyToLowest( a, transformer ) ).toList() );
        }
        return transformer.apply( o );
    }


    /**
     * Converts a JDBC array to a list.
     */
    public static List<?> arrayToList( final java.sql.Array a ) {
        if ( a == null ) {
            return null;
        }
        try {
            return Primitive.asList( a.getArray() );
        } catch ( SQLException e ) {
            throw toUnchecked( e );
        }
    }


    public static List<?> deepArrayToList( final java.sql.Array a ) {
        if ( a == null ) {
            return null;
        }
        try {
            List<?> asList = Primitive.asList( a.getArray() );
            return deepArrayToListRecursive( asList );
        } catch ( SQLException e ) {
            throw toUnchecked( e );
        }
    }


    private static List<?> deepArrayToListRecursive( List<?> l ) {
        if ( l.isEmpty() || !Types.isAssignableFrom( Array.class, l.get( 0 ).getClass() ) ) {
            return new ArrayList<>( l );
        }

        List<Object> outer = new ArrayList<>();
        for ( Object o : l ) {
            List<?> asList;
            try {
                asList = Primitive.asList( ((Array) o).getArray() );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
            outer.add( deepArrayToListRecursive( asList ) );
        }

        return outer;
    }


    /**
     * Support the {@code CURRENT VALUE OF sequence} operator.
     */
    @SuppressWarnings("unused")
    @NonDeterministic
    public static long sequenceCurrentValue( String key ) {
        return getAtomicLong( key ).get();
    }


    /**
     * Support the {@code NEXT VALUE OF sequence} operator.
     */
    @NonDeterministic
    @SuppressWarnings("unused")
    public static long sequenceNextValue( String key ) {
        return getAtomicLong( key ).incrementAndGet();
    }


    private static AtomicLong getAtomicLong( String key ) {
        final Map<String, AtomicLong> map = THREAD_SEQUENCES.get();
        return map.computeIfAbsent( key, k -> new AtomicLong() );
    }


    /**
     * Support the SLICE function.
     */
    public static List<?> slice( List<?> list ) {
        return list;
    }


    /**
     * Support the ELEMENT function.
     */
    public static Object element( List<?> list ) {
        return switch ( list.size() ) {
            case 0 -> null;
            case 1 -> list.get( 0 );
            default -> throw Static.RESOURCE.moreThanOneValueInList( list.toString() ).ex();
        };
    }


    /**
     * Support the MEMBER OF function.
     */
    @SuppressWarnings("unused")
    public static boolean memberOf( Object object, Collection<?> collection ) {
        return collection.contains( object );
    }


    /**
     * Support the MULTISET INTERSECT DISTINCT function.
     */
    public static <E> Collection<E> multisetIntersectDistinct( Collection<E> c1, Collection<E> c2 ) {
        final Set<E> result = new HashSet<>( c1 );
        result.retainAll( c2 );
        return new ArrayList<>( result );
    }


    /**
     * Support the MULTISET INTERSECT ALL function.
     */
    public static <E> Collection<E> multisetIntersectAll( Collection<E> c1, Collection<E> c2 ) {
        final List<E> result = new ArrayList<>( c1.size() );
        final List<E> c2Copy = new ArrayList<>( c2 );
        for ( E e : c1 ) {
            if ( c2Copy.remove( e ) ) {
                result.add( e );
            }
        }
        return result;
    }


    /**
     * Support the MULTISET EXCEPT ALL function.
     */
    public static <E> Collection<E> multisetExceptAll( Collection<E> c1, Collection<E> c2 ) {
        final List<E> result = new LinkedList<>( c1 );
        for ( E e : c2 ) {
            result.remove( e );
        }
        return result;
    }


    /**
     * Support the MULTISET EXCEPT DISTINCT function.
     */
    public static <E> Collection<E> multisetExceptDistinct( Collection<E> c1, Collection<E> c2 ) {
        final Set<E> result = new HashSet<>( c1 );
        result.removeAll( c2 );
        return new ArrayList<>( result );
    }


    /**
     * Support the IS A SET function.
     */
    @SuppressWarnings("unused")
    public static boolean isASet( Collection<?> collection ) {
        if ( collection instanceof Set ) {
            return true;
        }
        // capacity calculation is in the same way like for new HashSet(Collection) however return immediately in case of duplicates
        Set<Object> set = new HashSet<>( Math.max( (int) (collection.size() / .75f) + 1, 16 ) );
        for ( Object e : collection ) {
            if ( !set.add( e ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Support the SUBMULTISET OF function.
     */
    @SuppressWarnings("unused")
    public static boolean submultisetOf( Collection<?> possibleSubMultiset, Collection<?> multiset ) {
        if ( possibleSubMultiset.size() > multiset.size() ) {
            return false;
        }
        Collection<?> multisetLocal = new LinkedList<>( multiset );
        for ( Object e : possibleSubMultiset ) {
            if ( !multisetLocal.remove( e ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Support the MULTISET UNION function.
     */
    public static Collection<String> multisetUnionDistinct( Collection<String> collection1, Collection<String> collection2 ) {
        // capacity calculation is in the same way like for new HashSet(Collection)
        Set<String> resultCollection = new HashSet<>( Math.max( (int) ((collection1.size() + collection2.size()) / .75f) + 1, 16 ) );
        resultCollection.addAll( collection1 );
        resultCollection.addAll( collection2 );
        return new ArrayList<>( resultCollection );
    }


    /**
     * Support the MULTISET UNION ALL function.
     */
    public static Collection<String> multisetUnionAll( Collection<String> collection1, Collection<String> collection2 ) {
        List<String> resultCollection = new ArrayList<>( collection1.size() + collection2.size() );
        resultCollection.addAll( collection1 );
        resultCollection.addAll( collection2 );
        return resultCollection;
    }


    @SuppressWarnings("unused")
    public static Function1<?, Enumerable<?>> flatProduct( final int[] fieldCounts, final boolean withOrdinality, final FlatProductInputType[] inputTypes ) {
        if ( fieldCounts.length == 1 ) {
            if ( !withOrdinality && inputTypes[0] == FlatProductInputType.SCALAR ) {
                return LIST_AS_ENUMERABLE;
            } else {
                return row -> p2( new Object[]{ row }, fieldCounts, withOrdinality, inputTypes );
            }
        }
        return lists -> p2( (Object[]) lists, fieldCounts, withOrdinality, inputTypes );
    }


    private static <E extends PolyValue> Enumerable<PolyList<E>> p2( Object[] lists, int[] fieldCounts, boolean withOrdinality, FlatProductInputType[] inputTypes ) {
        final List<Enumerator<PolyList<E>>> enumerators = new ArrayList<>();
        int totalFieldCount = 0;
        for ( int i = 0; i < lists.length; i++ ) {
            int fieldCount = fieldCounts[i];
            FlatProductInputType inputType = inputTypes[i];
            Object inputObject = lists[i];
            switch ( inputType ) {
                case SCALAR:
                    @SuppressWarnings("unchecked")
                    List<E> list = (List<E>) inputObject;
                    enumerators.add( Linq4j.transform( Linq4j.enumerator( list ), PolyList::of ) );
                    break;
                case LIST:
                    @SuppressWarnings("unchecked")
                    PolyList<PolyList<E>> listList = PolyList.copyOf( (List<PolyList<E>>) inputObject );
                    enumerators.add( Linq4j.enumerator( listList ) );
                    break;
                case MAP:
                    @SuppressWarnings("unchecked")
                    Map<E, E> map = (Map<E, E>) inputObject;
                    Enumerator<Entry<E, E>> enumerator = Linq4j.enumerator( map.entrySet() );

                    Enumerator<PolyList<E>> transformed = Linq4j.transform( enumerator, e -> PolyList.of( List.of( e.getKey(), e.getValue() ) ) );
                    enumerators.add( transformed );
                    break;
                default:
                    break;
            }
            if ( fieldCount < 0 ) {
                ++totalFieldCount;
            } else {
                totalFieldCount += fieldCount;
            }
        }
        if ( withOrdinality ) {
            ++totalFieldCount;
        }
        return product( enumerators, totalFieldCount, withOrdinality );
    }


    public static Object[] array( Object... args ) {
        return args;
    }


    /**
     * Similar to {@link Linq4j#product(Iterable)} but each resulting list implements {@link ComparableList}.
     */
    public static <E extends PolyValue> Enumerable<PolyList<E>> product( final List<Enumerator<PolyList<E>>> enumerators, final int fieldCount, final boolean withOrdinality ) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyList<E>> enumerator() {
                return new ProductPolyListEnumerator<>( enumerators, fieldCount, withOrdinality );
            }
        };
    }


    /**
     * Implements the {@code .} (field access) operator on an object whose type is not known until runtime.
     * <p>
     * A struct object can be represented in various ways by the runtime and depends on the
     * {@link JavaRowFormat}.
     */
    @Experimental
    @SuppressWarnings("unused")
    public static Object structAccess( Object structObject, int index, String fieldName ) {
        if ( structObject == null ) {
            return null;
        }

        if ( structObject instanceof Object[] object ) {
            return object[index];
        } else if ( structObject instanceof List<?> list ) {
            return list.get( index );
        } else if ( structObject instanceof Row<?> row ) {
            return row.getObject( index );
        } else {
            Class<?> beanClass = structObject.getClass();
            try {
                Field structField = beanClass.getDeclaredField( fieldName );
                return structField.get( structObject );
            } catch ( NoSuchFieldException | IllegalAccessException ex ) {
                throw Static.RESOURCE.failedToAccessField( fieldName, beanClass.getName() ).ex( ex );
            }
        }
    }


    private static boolean isScalarObject( Object obj ) {
        if ( obj instanceof Collection ) {
            return false;
        }
        return !(obj instanceof Map<?, ?>);
    }


    public static Object jsonValueExpression( PolyString input ) {
        try {
            return dejsonize( input );
        } catch ( Exception e ) {
            return e;
        }
    }


    @SuppressWarnings("unused")
    public static Object jsonValueExpressionExclude( PolyString input, List<PolyString> excluded ) {
        try {
            PolyList<PolyList<PolyString>> collect = PolyList.copyOf( excluded.stream().map( e -> PolyList.of( Arrays.stream( e.value.split( "\\." ) ).map( PolyString::of ).toList() ) ).toList() );

            PolyValue map = dejsonize( input );
            if ( map.isMap() ) {
                return rebuildMap( (PolyMap<PolyString, PolyValue>) map, collect );
            } else {
                return excluded.isEmpty() ? map : null;
            }
        } catch ( Exception e ) {
            return e;
        }
    }


    private static PolyMap<PolyString, PolyValue> rebuildMap( PolyMap<PolyString, PolyValue> map, PolyList<PolyList<PolyString>> collect ) {
        Map<PolyString, PolyValue> newMap = new HashMap<>();
        List<PolyValue> firsts = collect.value.stream().map( c -> c.get( 0 ) ).collect( Collectors.toList() );
        for ( Entry<PolyString, PolyValue> entry : map.entrySet() ) {
            if ( firsts.contains( entry.getKey() ) ) {
                List<PolyList<PolyString>> entries = new ArrayList<>();
                for ( PolyList<PolyString> excludes : collect ) {
                    if ( excludes.get( 0 ).equals( entry.getKey() ) && entry.getValue() instanceof Map ) {
                        // if it matches but has more child-keys we have to go deeper
                        if ( excludes.size() > 1 ) {
                            entries.add( PolyList.copyOf( excludes.subList( 1, excludes.size() ) ) );
                        }
                    }
                }
                if ( !entries.isEmpty() ) {
                    PolyMap<PolyString, PolyValue> rebuild = rebuildMap( (PolyMap) entry.getValue().asMap(), PolyList.copyOf( entries ) );
                    if ( !rebuild.isEmpty() ) {
                        newMap.put( entry.getKey(), rebuild );
                    }
                }

            } else {
                newMap.put( entry.getKey(), entry.getValue() );
            }
        }
        return PolyMap.of( newMap );
    }


    public static Object jsonStructuredValueExpression( Object input ) {
        return input;
    }


    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> singleToArray( Enumerable<? extends PolyValue> enumerable ) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return Linq4j.transform( enumerable.enumerator(), e -> new PolyValue[]{ e } );
            }
        };
    }


    public static PathContext jsonApiCommonSyntax( PolyValue input, PolyString pathSpec ) {
        try {

            Matcher matcher = JSON_PATH_BASE.matcher( pathSpec.value );
            if ( !matcher.matches() ) {
                throw Static.RESOURCE.illegalJsonPathSpec( pathSpec.value ).ex();
            }
            PathMode mode = PathMode.valueOf( matcher.group( 1 ).toUpperCase( Locale.ROOT ) );
            String pathWff = matcher.group( 2 );
            DocumentContext ctx = switch ( mode ) {
                case STRICT -> JsonPath.parse(
                        input,
                        Configuration
                                .builder()
                                .jsonProvider( JSON_PATH_JSON_PROVIDER )
                                .mappingProvider( JSON_PATH_MAPPING_PROVIDER )
                                .build() );
                case LAX -> JsonPath.parse(
                        input.toJson(),
                        Configuration
                                .builder()
                                .options( Option.SUPPRESS_EXCEPTIONS )
                                .jsonProvider( JSON_PATH_JSON_PROVIDER )
                                .mappingProvider( JSON_PATH_MAPPING_PROVIDER )
                                .build() );
                default -> throw Static.RESOURCE.illegalJsonPathModeInPathSpec( mode.toString(), pathSpec.value ).ex();
            };
            try {
                Object json = ctx.read( pathWff );
                return PathContext.withReturned( mode, json == null ? null : PolyValue.fromJson( json.toString() ) );
            } catch ( Exception e ) {
                return PathContext.withStrictException( e );
            }
        } catch ( Exception e ) {
            return PathContext.withUnknownException( e );
        }
    }


    @SuppressWarnings("unused")
    public static Boolean jsonExists( PolyValue input ) {
        return jsonExists( input, JsonExistsErrorBehavior.FALSE );
    }


    public static Boolean jsonExists( PolyValue input, JsonExistsErrorBehavior errorBehavior ) {
        PathContext context = (PathContext) input;
        if ( context.exc != null ) {
            return switch ( errorBehavior ) {
                case TRUE -> Boolean.TRUE;
                case FALSE -> Boolean.FALSE;
                case ERROR -> throw toUnchecked( context.exc );
                case UNKNOWN -> null;
            };
        } else {
            return !Objects.isNull( context.pathReturned );
        }
    }


    public static PolyValue jsonValueAny( PolyValue input, JsonValueEmptyOrErrorBehavior emptyBehavior, PolyValue defaultValueOnEmpty, JsonValueEmptyOrErrorBehavior errorBehavior, PolyValue defaultValueOnError ) {
        final PathContext context = (PathContext) input;
        final Exception exc;
        if ( context.exc != null ) {
            exc = context.exc;
        } else {
            PolyValue value = context.pathReturned;
            if ( value == null || context.mode == PathMode.LAX && !isScalarObject( value ) ) {
                return switch ( emptyBehavior ) {
                    case ERROR -> throw Static.RESOURCE.emptyResultOfJsonValueFuncNotAllowed().ex();
                    case NULL -> null;
                    case DEFAULT -> defaultValueOnEmpty;
                };
            } else if ( context.mode == PathMode.STRICT && !isScalarObject( value ) ) {
                exc = Static.RESOURCE.scalarValueRequiredInStrictModeOfJsonValueFunc( value.toString() ).ex();
            } else {
                return value;
            }
        }
        return switch ( errorBehavior ) {
            case ERROR -> throw toUnchecked( exc );
            case NULL -> null;
            case DEFAULT -> defaultValueOnError;
            default -> throw Static.RESOURCE.illegalErrorBehaviorInJsonValueFunc( errorBehavior.toString() ).ex();
        };
    }


    public static String jsonQuery( PolyValue input, JsonQueryWrapperBehavior wrapperBehavior, JsonQueryEmptyOrErrorBehavior emptyBehavior, JsonQueryEmptyOrErrorBehavior errorBehavior ) {
        final PathContext context = (PathContext) input;
        final Exception exc;
        if ( context.exc != null ) {
            exc = context.exc;
        } else {
            PolyValue value;
            if ( context.pathReturned == null ) {
                value = null;
            } else {
                switch ( wrapperBehavior ) {
                    case WITHOUT_ARRAY:
                        value = context.pathReturned;
                        break;
                    case WITH_UNCONDITIONAL_ARRAY:
                        value = PolyList.of( context.pathReturned );
                        break;
                    case WITH_CONDITIONAL_ARRAY:
                        if ( context.pathReturned instanceof Collection ) {
                            value = context.pathReturned;
                        } else {
                            value = PolyList.of( context.pathReturned );
                        }
                        break;
                    default:
                        throw Static.RESOURCE.illegalWrapperBehaviorInJsonQueryFunc( wrapperBehavior.toString() ).ex();
                }
            }
            if ( value == null || context.mode == PathMode.LAX && isScalarObject( value ) ) {
                return switch ( emptyBehavior ) {
                    case ERROR -> throw Static.RESOURCE.emptyResultOfJsonQueryFuncNotAllowed().ex();
                    case NULL -> null;
                    case EMPTY_ARRAY -> "[]";
                    case EMPTY_OBJECT -> "{}";
                };
            } else if ( context.mode == PathMode.STRICT && isScalarObject( value ) ) {
                exc = Static.RESOURCE.arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc( value.toString() ).ex();
            } else {
                try {
                    return toJson( value );
                } catch ( Exception e ) {
                    exc = e;
                }
            }
        }
        return switch ( errorBehavior ) {
            case ERROR -> throw toUnchecked( exc );
            case NULL -> null;
            case EMPTY_ARRAY -> "[]";
            case EMPTY_OBJECT -> "{}";
        };
    }


    public static String toJson( PolyValue input ) {
        return input.toJson();
    }


    public static PolyValue dejsonize( PolyString input ) {
        return mapFromBson( BsonDocument.parse( "{ \"key\":" + input.value + "}" ) ).asDocument().get( PolyString.of( "key" ) );
    }


    private static PolyValue mapFromBson( BsonDocument document ) {
        return BsonUtil.toPolyValue( document );
    }


    public static String jsonObject( JsonConstructorNullClause nullClause, PolyValue... kvs ) {
        assert kvs.length % 2 == 0;
        Map<PolyString, PolyValue> map = new HashMap<>();
        for ( int i = 0; i < kvs.length; i += 2 ) {
            PolyString k = (PolyString) kvs[i];
            PolyValue v = kvs[i + 1];
            if ( k == null ) {
                throw Static.RESOURCE.nullKeyOfJsonObjectNotAllowed().ex();
            }
            if ( v == null ) {
                if ( nullClause == JsonConstructorNullClause.NULL_ON_NULL ) {
                    map.put( k, null );
                }
            } else {
                map.put( k, v );
            }
        }
        return toJson( PolyMap.of( map ) );
    }


    public static void jsonObjectAggAdd( Map<String, Object> map, String k, Object v, JsonConstructorNullClause nullClause ) {
        if ( k == null ) {
            throw Static.RESOURCE.nullKeyOfJsonObjectNotAllowed().ex();
        }
        if ( v == null ) {
            if ( nullClause == JsonConstructorNullClause.NULL_ON_NULL ) {
                map.put( k, null );
            }
        } else {
            map.put( k, v );
        }
    }


    public static String jsonArray( JsonConstructorNullClause nullClause, PolyValue... elements ) {
        List<PolyValue> list = new ArrayList<>();
        for ( PolyValue element : elements ) {
            if ( element == null ) {
                if ( nullClause == JsonConstructorNullClause.NULL_ON_NULL ) {
                    list.add( null );
                }
            } else {
                list.add( element );
            }
        }
        return toJson( PolyList.copyOf( list ) );
    }


    public static void jsonArrayAggAdd( List list, Object element, JsonConstructorNullClause nullClause ) {
        if ( element == null ) {
            if ( nullClause == JsonConstructorNullClause.NULL_ON_NULL ) {
                list.add( null );
            }
        } else {
            list.add( element );
        }
    }


    public static boolean isJsonValue( PolyString input ) {
        try {
            dejsonize( input );
            return true;
        } catch ( Exception e ) {
            return false;
        }
    }


    public static boolean isJsonObject( PolyString input ) {
        try {
            Object o = dejsonize( input );
            return o instanceof Map;
        } catch ( Exception e ) {
            return false;
        }
    }


    public static boolean isJsonArray( PolyString input ) {
        try {
            Object o = dejsonize( input );
            return o instanceof Collection;
        } catch ( Exception e ) {
            return false;
        }
    }


    public static boolean isJsonScalar( PolyString input ) {
        try {
            Object o = dejsonize( input );
            return !(o instanceof Map) && !(o instanceof Collection);
        } catch ( Exception e ) {
            return false;
        }
    }


    @SuppressWarnings("unused")
    public static List<?> deserializeList( String parsed ) {
        return gson.fromJson( parsed, List.class );
    }


    @SuppressWarnings("unused")
    public static PolyDictionary deserializeDirectory( String parsed ) {
        return gson.fromJson( parsed, PolyDictionary.class );
    }


    private static RuntimeException toUnchecked( Exception e ) {
        if ( e instanceof RuntimeException ) {
            return (RuntimeException) e;
        }
        return new GenericRuntimeException( e );
    }


    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> singletonEnumerable( Object value ) {
        return Linq4j.singletonEnumerable( (PolyValue[]) value );
    }


    /**
     * Path spec has two different modes: lax mode and strict mode.
     * Lax mode suppresses any thrown exception and returns null, whereas strict mode throws exceptions.
     */
    public enum PathMode {
        LAX,
        STRICT,
        UNKNOWN
    }


    /**
     * Type of argument passed into {@link #flatProduct}.
     */
    public enum FlatProductInputType {
        SCALAR, LIST, MAP
    }

}

