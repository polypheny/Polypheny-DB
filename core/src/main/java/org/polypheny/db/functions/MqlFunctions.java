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

package org.polypheny.db.functions;

import static org.polypheny.db.functions.spatial.GeoDistanceFunctions.EARTH_RADIUS_M;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.spatial.GeometryTopologicalException;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.util.Pair;


/**
 * Repository class, which defines different functions used, when handling the document model
 */
@Slf4j
public class MqlFunctions {


    private MqlFunctions() {
        // empty on purpose
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docExprDocument( PolyValue fieldNames, PolyValue[] values ) {
        PolyDocument result = new PolyDocument();
        List<PolyString> names = fieldNames.asList();
        for ( int i = 0; i < values.length; i++ ) {
            PolyValue value = values[i];
            if ( !isUnsetValue( value ) ) {
                result.put( names.get( i ), value == null ? PolyNull.NULL : value );
            }
        }
        return result;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docExprArray( PolyValue[] values ) {
        List<PolyValue> elements = new ArrayList<>( values.length );
        for ( PolyValue value : values ) {
            elements.add( value == null || isUnsetValue( value ) ? PolyNull.NULL : value );
        }
        return PolyList.of( elements );
    }


    private static boolean isUnsetValue( PolyValue value ) {
        return value instanceof PolyDocument document && document.isUnset;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docQueryValue( PolyValue input, List<PolyString> filters ) {
        if ( input == null ) {
            return null;
        }
        if ( !input.isDocument() ) {
            if ( input.isString() ) {
                input = PolyDocument.fromTypedJson( input.asString().value, PolyDocument.class );
            } else {
                return null;
            }
        }
        PolyValue temp = input;
        for ( PolyString filter : filters ) {
            if ( !temp.isDocument() ) {
                return PolyDocument.ofUnset(); // all fields except _id are unset
            }
            temp = temp.asDocument().get( filter );
            if ( temp == null ) {
                return PolyDocument.ofUnset();
            }
        }

        return temp;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docExprQueryValue( PolyValue input, List<PolyString> fields ) {
        if ( input != null && input.isString() ) {
            input = PolyDocument.fromTypedJson( input.asString().value, PolyDocument.class );
        }
        return docExprQueryValue( input, fields, 0 );
    }


    private static PolyValue docExprQueryValue( PolyValue value, List<PolyString> fields, int offset ) {
        if ( value == null ) {
            return PolyDocument.ofUnset();
        }
        if ( offset == fields.size() ) {
            return value;
        }
        if ( value instanceof PolyDocument document ) {
            return docExprQueryValue( document.get( fields.get( offset ) ), fields, offset + 1 );
        }
        if ( value instanceof PolyList<?> list && list.value != null ) {
            // Aggregation evaluates the remaining path on each document in the array.
            // Missing values are omitted, and arrays returned by a document remain nested.
            List<PolyValue> values = new ArrayList<>();
            for ( PolyValue element : list ) {
                if ( element instanceof PolyDocument ) {
                    PolyValue result = docExprQueryValue( element, fields, offset );
                    if ( !isUnsetValue( result ) ) {
                        values.add( result );
                    }
                }
            }
            return PolyList.of( values );
        }
        return PolyDocument.ofUnset();
    }


    /**
     * Adds a new field to an existing object
     *
     * @param input the document, to which the new field is added
     * @param name the name of the added field
     * @param object th field value, which is added
     * @return the new object, with the value included
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docAddFields( PolyValue input, List<PolyString> name, PolyValue object ) {
        updateValue( object, input.asDocument(), name );

        return input;
    }


    /**
     * Tries to add the provided value to an array/list,
     * if the object is not in such a form it fails
     *
     * @param input the object, which should be of list/array form
     * @param value the object, which is added.
     * @return a new list, which included the new value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docAddToSet( PolyValue input, PolyValue value ) {

        if ( input.isList() ) {
            if ( input.asList().contains( value ) ) {
                return input;
            }
            input.asList().add( value );
            return input;
        } else {
            throw new GenericRuntimeException( "AddToSet can only be applied to arrays" );
        }
    }


    /**
     * Compares a specified entry in the document with the provided comparator
     * and replaces it with the smaller value
     *
     * @param input the document/object, which entry is compared and replaced
     * @param comparator the value, which is compared with
     * @return a new document/object, which holds the minimal value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docUpdateMin( PolyValue input, PolyValue comparator ) {
        return docUpdateMinMax( input, comparator ).intValue() <= 0 ? input : comparator;
    }


    /**
     * Compares a specified entry in the document with the provided comparator
     * and replaces it with the bigger value
     *
     * @param input the document/object, which entry is compared and replaced
     * @param comparator the value, which is compared with
     * @return a new document/object, which holds the maximal value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docUpdateMax( PolyValue input, PolyValue comparator ) {
        return docUpdateMinMax( input, comparator ).intValue() <= 0 ? comparator : input;
    }


    /**
     * Comparable method, which handles the comparing of two untyped elements
     *
     * @param input the original value
     * @param comparator the new value
     * @return number identifying, if comparator is bigger ( -1 ) or smaller ( 1 )
     */
    @SuppressWarnings("UnusedDeclaration")
    private static PolyNumber docUpdateMinMax( PolyValue input, PolyValue comparator ) {
        if ( input == null || comparator == null ) {
            throw new RuntimeException( "The provided values where not comparable." );
        }
        return PolyInteger.of( input.compareTo( comparator ) );
    }


    /**
     * Update method, which deletes the provided name
     *
     * @param input the full object/document, from which the values are removed
     * @param names the name in a list form <pre>key1.key2.key3 {@code ->} [key1,key2,key3]</pre>
     * @return the object/document, without the filtered name
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docRemove( PolyValue input, List<List<PolyString>> names ) {
        Map<PolyString, PolyValue> doc = input.asDocument();
        PolyString name;
        for ( List<PolyString> split : names ) {
            Iterator<PolyString> iter = split.iterator();

            while ( iter.hasNext() ) {
                name = iter.next();
                if ( doc.containsKey( name ) ) {
                    if ( !iter.hasNext() ) {
                        doc.remove( name );
                    } else {
                        if ( doc.get( name ).isDocument() ) {
                            doc = doc.get( name ).asDocument();
                        } else {
                            break;
                        }

                    }
                }
            }
        }

        return input;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument docUpdateReplace( PolyValue input, List<PolyString> names, List<PolyValue> values ) {
        if ( !input.isDocument() ) {
            return new PolyDocument();
        }
        for ( Pair<PolyString, PolyValue> pair : Pair.zip( names, values ) ) {
            updateValue( pair.right, input.asDocument(), Arrays.stream( pair.left.value.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) );
        }

        return input.asDocument();
    }


    private static void updateValue( PolyValue value, PolyDocument doc, List<PolyString> splitName ) {
        PolyString name;
        Iterator<PolyString> iter = splitName.iterator();

        while ( iter.hasNext() ) {
            name = iter.next();

            if ( !iter.hasNext() ) {
                doc.put( name, value );
            } else {
                if ( doc.containsKey( name ) ) {
                    doc.put( name, PolyDocument.ofDocument( Map.of() ) );
                }
                doc = doc.get( name ).asDocument();
            }
        }
    }


    @SuppressWarnings("unused")
    public static PolyDocument projectIncludes( PolyValue input, PolyList<PolyList<PolyString>> names, PolyValue... includes ) {
        if ( !input.isDocument() ) {
            return new PolyDocument();
        }
        PolyDocument doc = input.asDocument();
        List<Pair<PolyList<PolyString>, PolyValue>> result = new ArrayList<>();
        for ( Pair<PolyList<PolyString>, PolyValue> nameInclude : Pair.zip( names, List.of( includes ) ) ) {
            if ( MqlFunctions.docExists( input, PolyBoolean.TRUE, nameInclude.right.asList() ).value ) {
                result.add( Pair.of( nameInclude.left, docQueryValue( input, nameInclude.right.asList() ) ) );
            }
        }
        return mergeDocument( new PolyDocument(), result.stream().map( p -> p.left ).collect( Collectors.toList() ), result.stream().map( p -> p.right ).toArray( PolyValue[]::new ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docUpdateRename( PolyValue input, List<List<PolyString>> fields, List<List<PolyString>> newNames ) {
        PolyDocument doc = input.asDocument();

        PolyValue end = null;
        PolyDocument temp = doc;
        Iterator<PolyString> iter;
        outer:
        for ( int i = 0; i < fields.size(); i++ ) {
            List<PolyString> names = fields.get( i );
            iter = names.iterator();

            // search for element
            while ( iter.hasNext() ) {
                PolyString name = iter.next();
                if ( temp.containsKey( name ) ) {
                    if ( iter.hasNext() ) {
                        // we go deeper
                        if ( temp.get( name ).isDocument() ) {
                            temp = temp.get( name ).asDocument();
                        } else {
                            continue outer;
                        }
                    } else {
                        // we found it
                        end = temp.get( name );
                        temp.remove( name );
                    }
                }
            }
            // we place the element
            docAddFields( doc, newNames.get( i ), end );


        }

        return doc;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument replaceRoot( PolyValue doc ) {
        if ( !doc.isDocument() ) {
            throw new GenericRuntimeException( "Can only be replaced by document" );
        }
        return doc.asDocument();
    }


    /**
     * Tests if the provided object/document belongs to one of the provided types
     *
     * @param input the object/document, which is tested
     * @param typeNumbers the types as numbers, which return true if matched
     * @return if the object/type is of one of the provided types
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docTypeMatch( PolyValue input, List<PolyInteger> typeNumbers ) {

        if ( input == null ) {
            // if we look for nullType
            return PolyBoolean.of( typeNumbers.contains( PolyInteger.of( 10 ) ) );
        }

        List<Pair<Class<? extends BsonValue>, Class<?>>> clazzPairs = typeNumbers.stream().map( typeNumber -> DocumentUtil.getBsonClass( typeNumber.intValue() ) ).collect( Collectors.toList() );

        return PolyBoolean.of( Pair.right( clazzPairs ).stream().anyMatch( clazz -> clazz.isInstance( input ) ) );
    }


    /**
     * Tests if the object/document matches the provided regex
     *
     * @param input the object/document
     * @param regex the regex to match
     * @param isInsensitive if the matching should be case-insensitive
     * @param isMultiline if multiple lines should be considered
     * @param doesIgnoreWhitespace if whitespace should be ignored
     * @param allowsDot if dots for subfields are allowed
     * @return if the provided object/document conforms to the regex
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docRegexMatch( PolyValue input, PolyString regex, PolyBoolean isInsensitive, PolyBoolean isMultiline, PolyBoolean doesIgnoreWhitespace, PolyBoolean allowsDot ) {
        String adjusted = regex.value;
        if ( input.isString() ) {
            String comp = input.asString().value;
            int flags = 0;
            flags |= Pattern.DOTALL;
            if ( isInsensitive.value ) {
                flags |= Pattern.CASE_INSENSITIVE;
            }
            if ( isMultiline.value ) {
                flags |= Pattern.MULTILINE;
            }
            if ( doesIgnoreWhitespace.value ) {
                adjusted = adjusted.replaceAll( "\\s", "" );
                comp = comp.replaceAll( "\\s", "" );
            }
            if ( allowsDot.value ) {
                flags |= Pattern.DOTALL;
            }

            return PolyBoolean.of( Pattern.compile( ".*" + regex + ".*", flags ).matcher( comp ).matches() );
        }
        return PolyBoolean.FALSE;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docJsonMatch( PolyValue input, String json ) {
        // TODO use schema validator library
        throw new GenericRuntimeException( "NOT IMPLEMENTED" );
    }


    /**
     * Retrieves and transforms an array from the provided object/document
     *
     * @param input the unparsed and typed array
     * @return the array
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyList<PolyValue> docGetArray( PolyValue input ) {
        if ( input.isList() ) {
            return input.asList();
        }
        return PolyList.of();
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to relScan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    private static PolyValue docItemAny( List<PolyValue> input, int index ) {
        // mongo starts at 0 and allows retrieving from behind with negative
        if ( input.size() > Math.abs( index ) ) {
            if ( index < 0 ) {
                index = input.size() + index;
            }
            return input.get( index );
        }
        return null;
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to relScan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docItemAny( PolyValue input, Object index ) {
        return docItem( input, ((Number) index).intValue() );
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to relScan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docItem( PolyValue input, int index ) {
        if ( input.isList() ) {
            return input.asList().get( index );
        }

        return null;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument mergeDocument( PolyValue value, List<PolyList<PolyString>> names, PolyValue... documents ) {
        assert names.size() == documents.length;
        Map<PolyString, PolyValue> doc = new HashMap<>();
        addFieldsToDoc( doc, names, documents );
        return PolyDocument.ofDocument( doc );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument mergeDocumentAdd( PolyValue value, List<PolyList<PolyString>> names, PolyValue... documents ) {
        assert names.size() == documents.length;
        Map<PolyString, PolyValue> doc = new HashMap<>();
        if ( value.isDocument() ) {
            doc.putAll( value.asDocument() );
        }
        addFieldsToDoc( doc, names, documents );
        return PolyDocument.ofDocument( doc );
    }


    private static void addFieldsToDoc( Map<PolyString, PolyValue> doc, List<PolyList<PolyString>> names, PolyValue... documents ) {
        Iterator<PolyString> iter;
        Map<PolyString, PolyValue> temp;
        for ( int i = 0; i < documents.length; i++ ) {
            if ( documents[i].isDocument() && documents[i].asDocument().isUnset ) {
                continue;
            }
            iter = names.get( i ).iterator();
            temp = doc;
            while ( iter.hasNext() ) {
                PolyString name = iter.next();
                if ( iter.hasNext() ) {
                    // we are not yet at the end, need next document
                    if ( !temp.containsKey( name ) ) {
                        temp.put( name, PolyDocument.ofDocument( Map.of() ) );
                    }
                    temp = temp.get( name ).asDocument();
                } else {
                    // we are at the end and place value
                    temp.put( name, documents[i] );
                }
            }

        }
    }


    /**
     * If the provided object/document is of type array and matches the specified size
     *
     * @param input the unparsed object/document
     * @param size the size to compare
     * @return if the size matches
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docSizeMatch( PolyValue input, PolyValue size ) {
        if ( !size.isNumber() ) {
            return PolyBoolean.FALSE;
        }
        if ( input.isList() ) {
            return PolyBoolean.of( input.asList().size() == size.asNumber().intValue() );
        }
        return PolyBoolean.FALSE;
    }


    /**
     * Special equal operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the elements are equal
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docEq( PolyValue b0, PolyValue b1 ) {
        if ( b0 instanceof PolyList<?> list && list.value != null ) {
            return PolyBoolean.of( wholeValueEquals( b0, b1 )
                    || list.stream().anyMatch( element -> wholeValueEquals( element, b1 ) ) );
        }
        return docExprEq( b0, b1 );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docNe( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( !docEq( left, right ).value );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprEq( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( wholeValueEquals( left, right ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprNe( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( !wholeValueEquals( left, right ) );
    }


    private static boolean wholeValueEquals( PolyValue left, PolyValue right ) {
        boolean leftNull = left == null || left.isNull();
        boolean rightNull = right == null || right.isNull();
        if ( leftNull || rightNull ) {
            return leftNull == rightNull;
        }
        if ( left instanceof PolyNumber l && right instanceof PolyNumber r ) {
            Object leftValue = l.toJava();
            Object rightValue = r.toJava();
            if ( leftValue == null || rightValue == null ) {
                return leftValue == rightValue;
            }
            return compareNumericValues( l, r ) == 0;
        }
        if ( left instanceof PolyList<?> l && right instanceof PolyList<?> r ) {
            if ( l.value == null || r.value == null ) {
                return l.value == r.value;
            }
            return l.size() == r.size()
                    && Pair.zip( l.value, r.value ).stream().allMatch( pair -> wholeValueEquals( pair.left, pair.right ) );
        }
        if ( left instanceof PolyDocument l && right instanceof PolyDocument r ) {
            if ( l.isUnset != r.isUnset || l.size() != r.size() ) {
                return false;
            }
            Iterator<Entry<PolyString, PolyValue>> rightEntries = r.entrySet().iterator();
            for ( Entry<PolyString, PolyValue> leftEntry : l.entrySet() ) {
                Entry<PolyString, PolyValue> rightEntry = rightEntries.next();
                if ( !Objects.equals( leftEntry.getKey(), rightEntry.getKey() )
                        || !wholeValueEquals( leftEntry.getValue(), rightEntry.getValue() ) ) {
                    return false;
                }
            }
            return true;
        }
        if ( left instanceof PolyMap<?, ?> l && right instanceof PolyMap<?, ?> r && left.isSameType( right ) ) {
            return l.keySet().equals( r.keySet() )
                    && l.entrySet().stream().allMatch( entry -> wholeValueEquals( entry.getValue(), r.get( entry.getKey() ) ) );
        }
        return left.compareTo( right ) == 0;
    }


    private static int compareNumericValues( PolyNumber left, PolyNumber right ) {
        boolean leftApproximate = PolyType.APPROX_TYPES.contains( left.getType() );
        boolean rightApproximate = PolyType.APPROX_TYPES.contains( right.getType() );
        if ( leftApproximate && rightApproximate ) {
            double leftValue = left.doubleValue();
            double rightValue = right.doubleValue();
            return leftValue == rightValue ? 0 : Double.compare( leftValue, rightValue );
        }
        if ( leftApproximate ) {
            return compareDoubleWithExact( left.doubleValue(), right.bigDecimalValue() );
        }
        if ( rightApproximate ) {
            return -compareDoubleWithExact( right.doubleValue(), left.bigDecimalValue() );
        }
        return left.bigDecimalValue().compareTo( right.bigDecimalValue() );
    }


    private static int compareDoubleWithExact( double number, BigDecimal exact ) {
        if ( !Double.isFinite( number ) ) {
            return number == Double.POSITIVE_INFINITY ? 1 : -1;
        }
        // Compare the represented IEEE 754 value at full precision.
        return new BigDecimal( number ).compareTo( exact );
    }


    /**
     * Special greater operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is smaller than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGt( PolyValue b0, PolyValue b1 ) {
        return compareNumbers( b0, b1, cmp -> cmp > 0 );
    }


    /**
     * Special greater than operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is smaller equal than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGte( PolyValue b0, PolyValue b1 ) {
        return compareNumbers( b0, b1, cmp -> cmp >= 0 );
    }


    @SuppressWarnings("unused")
    public static PolyValue notUnset( PolyValue value ) {
        if ( value.isDocument() && value.asDocument().isEmpty() ) {
            return PolyNull.NULL;
        }
        return value;
    }


    /**
     * Special less than operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is bigger than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docLt( PolyValue b0, PolyValue b1 ) {
        return compareNumbers( b0, b1, cmp -> cmp < 0 );
    }


    /**
     * Special less than equal operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is bigger equal than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docLte( PolyValue b0, PolyValue b1 ) {
        return compareNumbers( b0, b1, cmp -> cmp <= 0 );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprLt( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( compareBsonValues( left, right ) < 0 );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprLte( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( compareBsonValues( left, right ) <= 0 );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprGt( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( compareBsonValues( left, right ) > 0 );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExprGte( PolyValue left, PolyValue right ) {
        return PolyBoolean.of( compareBsonValues( left, right ) >= 0 );
    }


    private static int compareBsonValues( PolyValue left, PolyValue right ) {
        BsonComparisonType leftType = bsonComparisonType( left );
        BsonComparisonType rightType = bsonComparisonType( right );
        if ( leftType != rightType ) {
            return leftType.compareTo( rightType );
        }

        return switch ( leftType ) {
            case MISSING, NULL -> 0;
            case NUMBER -> compareBsonNumbers( left.asNumber(), right.asNumber() );
            case STRING -> compareBsonStrings( left.asString().value, right.asString().value );
            case DOCUMENT -> compareBsonDocuments( left.asDocument(), right.asDocument() );
            case ARRAY -> compareBsonArrays( left.asList(), right.asList() );
            case BINARY -> compareBsonBinary( left.asBinary().value, right.asBinary().value );
            case BOOLEAN -> Boolean.compare( left.asBoolean().value, right.asBoolean().value );
            case DATE, TIMESTAMP -> Long.compare( left.asTemporal().getMillisSinceEpoch(), right.asTemporal().getMillisSinceEpoch() );
        };
    }


    private static BsonComparisonType bsonComparisonType( PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return BsonComparisonType.NULL;
        }
        if ( value instanceof PolyDocument document && document.isUnset ) {
            return BsonComparisonType.MISSING;
        }
        return switch ( value.getType() ) {
            case INTEGER, TINYINT, SMALLINT, BIGINT, DECIMAL, REAL, FLOAT, DOUBLE ->
                    value.toJava() == null ? BsonComparisonType.NULL : BsonComparisonType.NUMBER;
            case VARCHAR -> BsonComparisonType.STRING;
            case DOCUMENT -> BsonComparisonType.DOCUMENT;
            case ARRAY -> value.asList().value == null ? BsonComparisonType.NULL : BsonComparisonType.ARRAY;
            case BINARY -> value.asBinary().value == null ? BsonComparisonType.NULL : BsonComparisonType.BINARY;
            case BOOLEAN -> BsonComparisonType.BOOLEAN;
            case DATE -> value.asTemporal().getMillisSinceEpoch() == null ? BsonComparisonType.NULL : BsonComparisonType.DATE;
            case TIMESTAMP -> value.asTemporal().getMillisSinceEpoch() == null ? BsonComparisonType.NULL : BsonComparisonType.TIMESTAMP;
            default -> throw new GenericRuntimeException( "Unsupported value type in MQL expression comparison: " + value.getType() );
        };
    }


    private static int compareBsonNumbers( PolyNumber left, PolyNumber right ) {
        boolean leftNaN = isNaN( left );
        boolean rightNaN = isNaN( right );
        if ( leftNaN || rightNaN ) {
            // BSON places NaN before all other numbers, including negative infinity.
            return Boolean.compare( rightNaN, leftNaN );
        }
        return compareNumericValues( left, right );
    }


    private static int compareBsonStrings( String left, String right ) {
        return Arrays.compareUnsigned( left.getBytes( StandardCharsets.UTF_8 ), right.getBytes( StandardCharsets.UTF_8 ) );
    }


    private static int compareBsonDocuments( PolyDocument left, PolyDocument right ) {
        Iterator<Entry<PolyString, PolyValue>> leftFields = left.entrySet().iterator();
        Iterator<Entry<PolyString, PolyValue>> rightFields = right.entrySet().iterator();
        while ( leftFields.hasNext() && rightFields.hasNext() ) {
            Entry<PolyString, PolyValue> leftField = leftFields.next();
            Entry<PolyString, PolyValue> rightField = rightFields.next();
            int cmp = bsonComparisonType( leftField.getValue() ).compareTo( bsonComparisonType( rightField.getValue() ) );
            if ( cmp == 0 ) {
                cmp = compareBsonStrings( leftField.getKey().value, rightField.getKey().value );
            }
            if ( cmp == 0 ) {
                cmp = compareBsonValues( leftField.getValue(), rightField.getValue() );
            }
            if ( cmp != 0 ) {
                return cmp;
            }
        }
        return Boolean.compare( leftFields.hasNext(), rightFields.hasNext() );
    }


    private static int compareBsonArrays( PolyList<?> left, PolyList<?> right ) {
        Iterator<? extends PolyValue> leftElements = left.iterator();
        Iterator<? extends PolyValue> rightElements = right.iterator();
        while ( leftElements.hasNext() && rightElements.hasNext() ) {
            int cmp = compareBsonValues( leftElements.next(), rightElements.next() );
            if ( cmp != 0 ) {
                return cmp;
            }
        }
        return Boolean.compare( leftElements.hasNext(), rightElements.hasNext() );
    }


    private static int compareBsonBinary( byte[] left, byte[] right ) {
        int size = Integer.compare( left.length, right.length );
        return size == 0 ? Arrays.compareUnsigned( left, right ) : size;
    }


    // Declaration order follows BSON aggregation comparison order.
    private enum BsonComparisonType {
        MISSING, NULL, NUMBER, STRING, DOCUMENT, ARRAY, BINARY, BOOLEAN, DATE, TIMESTAMP
    }


    private static PolyBoolean compareNumbers( PolyValue field, PolyValue operand, IntPredicate comparison ) {
        if ( !(operand instanceof PolyNumber right) || right.toJava() == null ) {
            return PolyBoolean.FALSE;
        }
        if ( field instanceof PolyList<?> list && list.value != null ) {
            return PolyBoolean.of( list.stream().anyMatch( element -> matchesNumericBound( element, right, comparison ) ) );
        }
        return PolyBoolean.of( matchesNumericBound( field, right, comparison ) );
    }


    private static boolean matchesNumericBound( PolyValue value, PolyNumber bound, IntPredicate comparison ) {
        if ( !(value instanceof PolyNumber number) || number.toJava() == null ) {
            return false;
        }
        // A range with a NaN operand can match only another NaN.
        return isNaN( number ) == isNaN( bound ) && comparison.test( compareNumericValues( number, bound ) );
    }


    private static boolean isNaN( PolyNumber number ) {
        return PolyType.APPROX_TYPES.contains( number.getType() ) && Double.isNaN( number.doubleValue() );
    }


    /**
     * Extracts the specified range of elements from a provided array/list.
     * If the provided object/document is not a list/array, null is returned
     *
     * @param input the object/document, which should have type array
     * @param skip the elements to skip
     * @param elements how many elements are returned
     * @return the sliced elements
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docSlice( PolyValue input, PolyNumber skip, PolyNumber elements ) {
        if ( !(input.isList()) ) {
            return null;
        } else {
            PolyList<PolyValue> list = input.asList();
            // if elements is negative the selection starts from the end
            int end;
            int start;
            if ( elements.intValue() > 0 ) {
                start = Math.min( skip.intValue(), list.size() );
                end = Math.min( list.size(), skip.intValue() + elements.intValue() );
            } else {
                end = Math.max( 0, list.size() - skip.intValue() );
                start = Math.max( 0, end + elements.intValue() );
            }
            return PolyList.copyOf( list.subList( start, end ) );
        }
    }


    /**
     * Removes the provided filter from the doc
     *
     * @param doc the document to relScan
     * @param excluded the element to exclude
     */
    private static void excludeBson( PolyValue doc, List<List<PolyString>> excluded ) {
        if ( doc.isDocument() ) {
            List<String> firsts = excluded.stream().map( e -> e.get( 0 ).value ).collect( Collectors.toList() );
            List<PolyString> toRemove = new ArrayList<>();
            doc.asDocument().forEach( ( key, value ) -> {
                int pos = 0;
                List<Integer> matches = new ArrayList<>();
                boolean remove = false;
                for ( String first : firsts ) {
                    if ( key.value.equals( first ) ) {
                        if ( excluded.get( pos ).size() > 1 ) {
                            // the matching goes deeper
                            matches.add( pos );
                        } else {
                            // it an exact match and we exclude all underlying
                            remove = true;
                        }
                    }
                    pos++;
                }
                if ( !remove && matches.size() > 0 ) {
                    excludeBson( value.asDocument(), matches.stream().map( i -> excluded.get( i ).subList( 1, excluded.get( i ).size() ) ).collect( Collectors.toList() ) );
                } else if ( remove ) {
                    toRemove.add( key );
                }
            } );
            toRemove.forEach( r -> doc.asDocument().remove( r ) );
        }
    }


    /**
     * Transforms a provided element into it primitive form if it is not already
     *
     * @param obj the object to transform
     * @return the object as primitive, map or list
     */
    private static Object deserializeBsonIfNecessary( Object obj ) {
        if ( obj instanceof String ) {
            try {
                return transformBsonToPrimitive( BsonDocument.parse( (String) obj ) );
            } catch ( Exception e ) {
                return obj;
            }
        } else if ( obj instanceof BsonValue ) {
            return transformBsonToPrimitive( (BsonValue) obj );
        } else {
            return obj;
        }
    }


    /**
     * Tests if a specified path exists in the provided object/document
     *
     * @param obj the object/document to check
     * @param path the path, which is test in the form key1.key2.key3 {@code ->} [key1. key2, key3]
     * @return if the path exists
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExists( PolyValue obj, PolyValue opIfExists, List<PolyString> path ) {
        if ( !opIfExists.isBoolean() ) {
            throw new GenericRuntimeException( "The second parameter must be a boolean" );
        }
        boolean ifExists = opIfExists.asBoolean().value;
        if ( obj == null || !obj.isDocument() ) {
            return PolyBoolean.FALSE;
        }
        PolyDocument map = obj.asDocument();
        Iterator<PolyString> iter = path.iterator();
        PolyString current = iter.next();

        while ( map.containsKey( current ) ) {
            obj = map.get( current );
            if ( !iter.hasNext() ) {
                return ifExists ? PolyBoolean.TRUE : PolyBoolean.FALSE;
            }
            if ( !(obj instanceof Map) ) {
                return ifExists ? PolyBoolean.FALSE : PolyBoolean.TRUE;
            }
            map = map.get( current ).asDocument();
            current = iter.next();
        }

        return ifExists ? PolyBoolean.FALSE : PolyBoolean.TRUE;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyList<?> getAsList( PolyValue value ) {
        return value != null && value.isList() ? value.asList() : PolyList.of( value );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyNumber plus( PolyValue a, PolyValue b ) {
        if ( a.isNumber() && b.isNumber() ) {
            return Functions.plus( a.asNumber(), b.asNumber() );
        }
        throw new NotImplementedException();
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyNumber minus( PolyValue a, PolyValue b ) {
        if ( a.isNumber() && b.isNumber() ) {
            return Functions.minus( a.asNumber(), b.asNumber() );
        }
        throw new NotImplementedException();
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGeoIntersects( PolyValue input, PolyValue geometry ) {
        PolyGeometry geometryFilter = geometry.asGeometry();
        PolyGeometry inputGeometry = convertInputToPolyGeometry( input, geometryFilter.getSRID() );
        return inputGeometry.intersects( geometryFilter ) ? PolyBoolean.TRUE : PolyBoolean.FALSE;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGeoWithin( PolyValue input, PolyValue geometry, PolyValue distance ) {
        try {
            PolyGeometry geometryFilter;
            if ( geometry.isString() ) {
                geometryFilter = PolyGeometry.of( geometry.asString().getValue() );
                if ( geometryFilter == null ) {
                    throw new GenericRuntimeException( "Cannot parse geometry string %s to type Geometry", distance );
                }
            } else if ( geometry.isGeometry() ) {
                geometryFilter = geometry.asGeometry();
            } else {
                throw new GenericRuntimeException( "Cannot parse geometry %s to type Geometry", distance );
            }
            PolyGeometry inputGeometry = convertInputToPolyGeometry( input, geometryFilter.getSRID() );
            double distanceValue;
            if ( distance.isBigDecimal() ) {
                distanceValue = distance.asBigDecimal().doubleValue();
            } else if ( distance.isDouble() ) {
                distanceValue = distance.asDouble().doubleValue();
            } else {
                throw new GenericRuntimeException( "Cannot parse distance %s to type double", distance );
            }

            if ( distanceValue > 0 ) {
                if ( geometryFilter.getSRID() != 0 ) {
                    // In the case of $centerSphere, we first have to convert radians to meters.
                    distanceValue = EARTH_RADIUS_M * distanceValue;
                }
                return inputGeometry.isWithinDistance( geometryFilter, distanceValue ) ? PolyBoolean.TRUE : PolyBoolean.FALSE;
            }
            // coveredBy also works if the input geometry lies along the edges of the filter geometry.
            // For example: A point [0,0] is inside a box [0,0 to 1,1], because it lies on a corner / edge.
            return inputGeometry.coveredBy( geometryFilter ) ? PolyBoolean.TRUE : PolyBoolean.FALSE;
        } catch ( GeometryTopologicalException e ) {
            throw new GenericRuntimeException( "$geometry could not be parsed as GeoJSON" );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyNumber docGeoDistance( PolyValue input, PolyValue geometry, PolyValue distanceMultiplier ) {
        PolyGeometry geometryFilter = geometry.asGeometry();
        PolyGeometry inputGeometry = convertInputToPolyGeometry( input, geometryFilter.getSRID() );
        PolyNumber distance = GeoFunctions.stDistance( inputGeometry, geometryFilter );
        float distanceFloat = distance.asFloat().floatValue();

        if ( distanceMultiplier.isInteger() ) {
            int distanceMultiplierInt = distanceMultiplier.asInteger().intValue();
            return PolyFloat.of( distance.asFloat().floatValue() * distanceMultiplierInt );
        } else if ( distanceMultiplier.isFloat() ) {
            float distanceMultiplierFloat = distanceMultiplier.asFloat().floatValue();
            return PolyFloat.of( distance.asFloat().floatValue() * distanceMultiplierFloat );
        } else if ( distanceMultiplier.isDouble() ) {
            double distanceMultiplierDouble = distanceMultiplier.asDouble().doubleValue();
            return PolyFloat.of( distance.asFloat().floatValue() * distanceMultiplierDouble );
        }
        // TODO: Is this exhaustive? Is there a better way?
        throw new GenericRuntimeException( "Probably forgot to implement conersion for distanceMultiplier in docGeoDistance." );
    }

//    @SuppressWarnings("UnusedDeclaration")
//    public static PolyBoolean docNear( PolyValue input, PolyValue geometry, PolyValue minDistance, PolyValue maxDistance ) {
//        throw new GenericRuntimeException( " Something went wrong in the DocumentNearUnwrap conversion rule. This method should not be called. " );
//    }
//
//    @SuppressWarnings("UnusedDeclaration")
//    public static PolyBoolean docNearSphere( PolyValue input, PolyValue geometry, PolyValue minDistance, PolyValue maxDistance ) {
//        throw new GenericRuntimeException( " Something went wrong in the DocumentNearUnwrap conversion rule. This method should not be called. " );
//    }


    /**
     * Converts a PolyValue into a PolyGeometry type. We support the following cases:
     * 1. Legacy Coordinates:
     * - Array: The input value is a list of 2 numbers (integer, double), which represent the
     * x and y values.
     * - Embedded Document: The input value is a document with two key-value pairs. The first
     * will be the x and the second will be the y value.
     * Points are created without an SRID, which means that calculations will be done in the cartesian coordinate system by default.
     * 2. GeoJSON:
     * - A document that adheres to the GeoJSON specification.
     */
    public static PolyGeometry convertInputToPolyGeometry( PolyValue input, Integer srid ) {
        if ( input.isGeometry() ) {
            return input.asGeometry();
        }

        GeometryFactory geoFactory = new GeometryFactory( new PrecisionModel(), srid );

        // Legacy Coordinates
        if ( input.isList() ) {
            PolyList<PolyValue> inputList = input.asList();

            Function<PolyValue, Double> getDouble = ( PolyValue polyValue ) -> {
                Double result = null;
                if ( polyValue.isDouble() ) {
                    result = polyValue.asDouble().value;
                }
                if ( polyValue.isInteger() ) {
                    Integer intValue = polyValue.asInteger().value;
                    if ( intValue != null ) {
                        result = (double) intValue;
                    }
                }
                if ( result == null ) {
                    throw new GenericRuntimeException( "Legacy Coordinates needs to be of type INTEGER or DOUBLE." );
                }
                return result;
            };

            if ( inputList.size() != 2 ) {
                throw new GenericRuntimeException( "Legacy Coordinate Pairs stored as Array can only be of the form [x,y]" );
            }

            Double x = getDouble.apply( inputList.get( 0 ) );
            Double y = getDouble.apply( inputList.get( 1 ) );
            Coordinate coordinate = new Coordinate( x, y );
            Point point = geoFactory.createPoint( coordinate );
            return new PolyGeometry( point );
        }

        // Embedded Document
        if ( input.isDocument() ) {
            PolyDocument inputDocument = input.asDocument();

            if ( inputDocument.keySet().isEmpty() ) {
                // TODO: Should we detect this in each spatial function, and then just return false, if the document
                //       does not contain the field / document?
                throw new GenericRuntimeException( "Field selected in query is empty or does not exist." );
            }

            try {
                // In GeoJSON, WGS84 is assumed by default. This is also the case for MongoDB.
                return PolyGeometry.fromGeoJson( inputDocument.toJson() );
            } catch ( InvalidGeometryException e ) {

                // If the documents contain two fields that are of numeric type, then the user wanted to
                // use legacy coordinates embedded inside a document. This is currently not supported, because
                // document fields are by definition unordered, and supporting this would require a lot of changes.
                if ( inputDocument.size() == 2 && inputDocument.values().stream().allMatch( value -> PolyType.NUMERIC_TYPES.contains( value.getType() ) ) ) {
                    throw new GenericRuntimeException( "Legacy coordinates inside an embedded document is currently not supported, as document fields are not ordered.", e );
                }
                throw new GenericRuntimeException( "$geometry operand of $geoIntersects could not be parsed as GeoJSON.", e );
            }
        }

        throw new GenericRuntimeException( "Geometry type could not be determined." );
    }


    /**
     * Transforms a provided Bson object into a primitive from
     *
     * @param doc the document to transform
     * @return the document as primitive
     */
    private static Object transformBsonToPrimitive( BsonValue doc ) {
        if ( doc == null ) {
            return null;
        }
        switch ( doc.getBsonType() ) {
            case INT32:
                return doc.asInt32().getValue();
            case INT64:
                return doc.asInt64().getValue();
            case DOUBLE:
                return doc.asDouble().getValue();
            case STRING:
                return doc.asString().getValue();
            case DECIMAL128:
                return doc.asDecimal128().decimal128Value().bigDecimalValue();
            case DOCUMENT:
                return doc.asDocument().entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> transformBsonToPrimitive( e.getValue() ) ) );
            case ARRAY:
                return doc.asArray().stream().map( MqlFunctions::transformBsonToPrimitive ).collect( Collectors.toList() );
            default:
                return null;
        }
    }

}
