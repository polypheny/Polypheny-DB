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

package org.polypheny.db.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
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
    public static PolyValue docQueryValue( PolyValue input, List<PolyString> filters ) {
        if ( input == null || !input.isDocument() ) {
            return null;
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
        return PolyDocument.ofDocument( doc );
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
        if ( b0 instanceof List && !(b1 instanceof List) ) {
            return PolyBoolean.of( ((List<?>) b0).contains( b1 ) );
        }
        if ( b0 == null || b1 == null ) {
            if ( b0 == null && b1 == null ) {
                return PolyBoolean.TRUE;
            } else {
                return PolyBoolean.FALSE;
            }
        }

        return PolyBoolean.of( b0.compareTo( b1 ) == 0 );
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
        return compNullExecute(
                b0,
                b1,
                () -> Functions.gt( b0, b1 ).value );
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
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.gt( b0, b1 ).value || Functions.eq( b0, b1 ).value) );
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
        return compNullExecute(
                b0,
                b1,
                () -> Functions.lt( b0, b1 ).value );
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
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.lt( b0, b1 ).value || Functions.eq( b0, b1 ).value) );
    }


    /**
     * Helper function, which executes different provided suppliers and compares the two elements
     *
     * @param b0 the left element
     * @param b1 the right element
     * @param predicate the predicate, which returns if the condition of left and right match
     * @return if the provided elements fit the provided condition
     */
    @SuppressWarnings("UnusedDeclaration")
    private static PolyBoolean compNullExecute( PolyValue b0, PolyValue b1, Supplier<Boolean> predicate ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }
        if ( !(b0 instanceof PolyNumber) || !(b1 instanceof PolyNumber) ) {
            return PolyBoolean.FALSE;
        }
        return PolyBoolean.of( predicate.get() );
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
