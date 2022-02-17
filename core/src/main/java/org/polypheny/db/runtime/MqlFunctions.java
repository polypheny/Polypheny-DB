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

package org.polypheny.db.runtime;

import com.google.gson.Gson;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.util.Pair;


/**
 * Repository class, which defines different functions used, when handling the document model
 */
public class MqlFunctions {

    /**
     * This method extracts the provided the filter from the input.
     *
     * @param input an arbitrary object, from which the value is extracted
     * @param filters a filter, in the form key1.key2.key3 {@code ->} [key1, key2, key3]
     * @return the extracted value or null if no value was matched
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docQueryValue( Object input, List<String> filters ) {
        input = deserializeBsonIfNecessary( input );
        ArrayList<String> filtersCopy = new ArrayList<>( filters );
        while ( filtersCopy.size() != 0 && input != null ) {
            if ( input instanceof Map ) {
                if ( ((Map<?, ?>) input).containsKey( filtersCopy.get( 0 ) ) ) {
                    input = ((Map<?, ?>) input).get( filtersCopy.get( 0 ) );
                    filtersCopy.remove( 0 );
                } else {
                    input = null;
                }
            } else if ( input instanceof List && filtersCopy.get( 0 ).matches( "[0-9]*" ) ) {
                int pos = Integer.parseInt( filtersCopy.get( 0 ) );
                if ( ((List<?>) input).size() >= pos ) {
                    input = ((List<?>) input).get( pos );
                    filtersCopy.remove( 0 );
                } else {
                    input = null;
                }
            } else {
                input = null;
            }
        }
        if ( filtersCopy.size() > 0 ) {
            return null;
        }

        return input;
    }


    public static Collection docUpdate( Collection sink ) {
        return null;
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
    public static Object docAddFields( Object input, String name, Object object ) {
        if ( input instanceof String ) {
            BsonDocument document = BsonDocument.parse( (String) input );
            document.put( name, DocumentUtil.getBson( object ) );

            return document.toJson();
        }

        return null;
    }


    /**
     * Transformer methode, which outputs a JSON representation of the provided object
     *
     * @param input the untransformed object
     * @return a transformed object as JSON string
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docJsonify( Object input ) {
        if ( input instanceof BsonDocument ) {
            return ((BsonDocument) input).toJson();
        } else if ( input instanceof Map ) {
            Gson gson = new Gson();
            return gson.toJson( input );
        } else if ( input instanceof List ) {
            Gson gson = new Gson();
            return gson.toJson( input );
        } else {
            return input;
        }
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
    public static Object docAddToSet( Object input, Object value ) {
        input = deserializeBsonIfNecessary( input );
        value = deserializeBsonIfNecessary( value );

        if ( input instanceof List ) {
            if ( ((List<Object>) input).contains( value ) ) {
                return input;
            }
            ((List<Object>) input).add( value );
            return input;
        } else {
            throw new RuntimeException( "AddToSet can only be applied to arrays" );
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
    public static Object docUpdateMin( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? input : comparator;
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
    public static Object docUpdateMax( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? comparator : input;
    }


    /**
     * Comparable method, which handles the comparing of two untyped elements
     *
     * @param input the original value
     * @param comparator the new value
     * @return number identifying, if comparator is bigger ( -1 ) or smaller ( 1 )
     */
    @SuppressWarnings("UnusedDeclaration")
    private static int docUpdateMinMax( Object input, Object comparator ) {
        if ( input instanceof Comparable && comparator instanceof Comparable ) {
            return ((Comparable) input).compareTo( comparator );
        } else {
            throw new RuntimeException( "The provided values where not comparable." );
        }
    }


    /**
     * Update method, which deletes the provided name
     *
     * @param input the full object/document, from which the values are removed
     * @param names the name in a list form <pre>key1.key2.key3 {@code ->} [key1,key2,key3]</pre>
     * @return the object/document, without the filtered name
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateRemove( Object input, List names ) {
        // TODO enable as soon as pushing down of TableModify is possible
        Map<String, ?> initial = (Map) deserializeBsonIfNecessary( input );
        Map<String, ?> doc = initial;
        String name;
        Iterator<String> iter = names.iterator();
        while ( iter.hasNext() ) {
            name = iter.next();
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    doc.remove( name );
                } else {
                    if ( doc.get( name ) instanceof Map ) {
                        doc = (Map<String, ?>) doc.get( name );
                    } else {
                        return docJsonify( initial );
                    }

                }
            }
        }

        return docJsonify( initial );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateReplace( Object input, List names, List values ) {
        // TODO enable as soon as pushing down of TableModify is possible
        Map<String, Object> initial = (Map) deserializeBsonIfNecessary( input );
        Map<String, Object> doc = initial;
        String name;
        int count = -1;
        Iterator<String> iter = names.iterator();
        while ( iter.hasNext() ) {
            name = iter.next();
            count++;
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    doc.put( name, values.get( count ) );
                } else {
                    if ( doc.get( name ) instanceof Map ) {
                        doc = (Map<String, Object>) doc.get( name );
                    } else {
                        return docJsonify( initial );
                    }

                }
            }
        }

        return docJsonify( initial );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateRename( Object input, List names, List newNames ) {
        // TODO enable as soon as pushing down of TableModify is possible
        Map<String, Object> initial = (Map) deserializeBsonIfNecessary( input );
        Map<String, Object> doc = initial;
        String name;
        int count = -1;
        Iterator<String> iter = names.iterator();
        while ( iter.hasNext() ) {
            name = iter.next();
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    Object obj = doc.get( name );
                    doc.put( String.join( ".", (List) newNames.get( count ) ), obj );
                } else {
                    if ( doc.get( name ) instanceof Map ) {
                        doc = (Map<String, Object>) doc.get( name );
                    } else {
                        return docJsonify( initial );
                    }

                }
            }
        }

        return docJsonify( initial );
    }


    /**
     * Scans the object/document and removes matching filters
     *
     * @param input the object/document, form which the filters are removed
     * @param excluded multiple filters, group in collections [key1.key2.key3, key1.key2] {@code ->} [[key1, key2, key3],[key1, key2]]
     * @return a filtered object/document
     */
    @SuppressWarnings("UnusedDeclaration")
    public static BsonValue docQueryExclude( Object input, List<List<String>> excluded ) {
        if ( !(input instanceof String) ) {
            return null;
        }

        BsonValue doc = BsonDocument.parse( (String) input );

        if ( excluded.size() == 0 ) {
            return doc;
        }

        excludeBson( doc, excluded );
        return doc;
    }


    /**
     * Scans the object/document and removes matching filters
     *
     * @param input the object/document, form which the filters are removed
     * @param excluded multiple filters, group in collections [key1.key2.key3, key1.key2] {@code ->} [[key1, key2, key3],[key1, key2]]
     * @return a filtered object/document
     */
    @SuppressWarnings("UnusedDeclaration")
    public static BsonValue docQueryExclude( BsonValue input, List<List<String>> excluded ) {
        if ( !input.isDocument() ) {
            return input;
        }

        if ( excluded.size() == 0 ) {
            return input;
        }

        excludeBson( input, excluded );
        return input;
    }


    /**
     * Tests if the provided object/document belongs to one of the provided types
     *
     * @param input the object/document, which is tested
     * @param typeNumbers the types as numbers, which return true if matched
     * @return if the object/type is of one of the provided types
     */
    @SuppressWarnings("UnusedDeclaration")
    public static boolean docTypeMatch( Object input, List<Integer> typeNumbers ) {

        if ( input == null ) {
            // if we look for nullType
            return typeNumbers.contains( 10 );
        }

        List<Pair<Class<? extends BsonValue>, Class<?>>> clazzPairs = typeNumbers.stream().map( DocumentUtil::getBsonClass ).collect( Collectors.toList() );

        return Pair.right( clazzPairs ).stream().anyMatch( clazz -> clazz.isInstance( input ) );
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
    public static boolean docRegexMatch( Object input, String regex, boolean isInsensitive, boolean isMultiline, boolean doesIgnoreWhitespace, boolean allowsDot ) {
        if ( input instanceof String ) {
            String comp = (String) input;
            int flags = 0;
            flags |= Pattern.DOTALL;
            if ( isInsensitive ) {
                flags |= Pattern.CASE_INSENSITIVE;
            }
            if ( isMultiline ) {
                flags |= Pattern.MULTILINE;
            }
            if ( doesIgnoreWhitespace ) {
                regex = regex.replaceAll( "\\s", "" );
                comp = comp.replaceAll( "\\s", "" );
            }
            if ( allowsDot ) {
                flags |= Pattern.DOTALL;
            }

            return Pattern.compile( ".*" + regex + ".*", flags ).matcher( comp ).matches();
        }
        return false;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static boolean docJsonMatch( Object input, String json ) {
        // TODO use schema validator library
        throw new RuntimeException( "NOT IMPLEMENTED" );
    }


    /**
     * Retrieves and transforms an array from the provided object/document
     *
     * @param input the unparsed and typed array
     * @return the array
     */
    @SuppressWarnings("UnusedDeclaration")
    public static List docGetArray( Object input ) {
        input = deserializeBsonIfNecessary( input );
        if ( input instanceof List ) {
            return (List) input;
        }
        return Collections.emptyList();
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    private static Object docItemAny( List input, int index ) {
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
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docItemAny( Object input, Object index ) {
        if ( index instanceof BigDecimal ) {
            return docItem( input, ((BigDecimal) index).intValue() );
        }
        return docItem( input, (Integer) index );
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docItem( Object input, int index ) {
        if ( input instanceof String ) {
            BsonDocument doc = BsonDocument.parse( (String) input );
            if ( doc.isArray() ) {
                return docItemAny( doc.asArray(), index );
            }
        } else if ( input instanceof List ) {
            return docItemAny( (List<?>) input, index );
        }

        return null;
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docItem( BsonArray input, int index ) {
        return docItemAny( input, index );
    }


    /**
     * If the provided object/document is of type array and matches the specified size
     *
     * @param input the unparsed object/document
     * @param size the size to compare
     * @return if the size matches
     */
    @SuppressWarnings("UnusedDeclaration")
    public static boolean docSizeMatch( Object input, int size ) {
        if ( input instanceof List ) {
            return ((List<?>) input).size() == size;
        }
        return false;
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
    public static boolean docEq( Object b0, Object b1 ) {
        if ( b0 instanceof List && !(b1 instanceof List) ) {
            return ((List<?>) b0).contains( b1 );
        }
        if ( b0 == null || b1 == null ) {
            if ( b0 == null && b1 == null ) {
                return true;
            } else {
                return false;
            }
        }

        return Functions.eqAny( b0, b1 );
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
    public static boolean docGt( Object b0, Object b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> Functions.gtAny( b0, b1 ) );
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
    public static boolean docGte( Object b0, Object b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.gtAny( b0, b1 ) || Functions.eqAny( b0, b1 )) );
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
    public static boolean docLt( Object b0, Object b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> Functions.ltAny( b0, b1 ) );
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
    public static boolean docLte( Object b0, Object b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.ltAny( b0, b1 ) || Functions.eqAny( b0, b1 )) );
    }


    /**
     * Helper function, which exectues different provided suppliers and compares the two elements
     *
     * @param b0 the left element
     * @param b1 the right element
     * @param predicate the predicate, which returns if the condition of left and right match
     * @return if the provided elements fit the provided condition
     */
    @SuppressWarnings("UnusedDeclaration")
    private static boolean compNullExecute( Object b0, Object b1, Supplier<Boolean> predicate ) {
        if ( b0 == null || b1 == null ) {
            return false;
        }
        if ( !(b0 instanceof Number) || !(b1 instanceof Number) ) {
            return false;
        }
        return predicate.get();
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
    public static Object docSlice( Object input, int skip, int elements ) {
        if ( !(input instanceof List) ) {
            return null;
        } else {
            List<?> list = ((List<?>) input);
            // if elements is negative the selection starts from the end
            int end;
            int start;
            if ( elements > 0 ) {
                start = Math.min( skip, list.size() );
                end = Math.min( list.size(), skip + elements );
            } else {
                end = Math.max( 0, list.size() - skip );
                start = Math.max( 0, end + elements );
            }
            return list.subList( start, end );
        }
    }


    /**
     * Removes the provided filter from the doc
     *
     * @param doc the document to scan
     * @param excluded the element to exclude
     */
    private static void excludeBson( BsonValue doc, List<List<String>> excluded ) {
        if ( doc.isDocument() ) {
            List<String> firsts = excluded.stream().map( e -> e.get( 0 ) ).collect( Collectors.toList() );
            List<String> toRemove = new ArrayList<>();
            doc.asDocument().forEach( ( key, value ) -> {
                int pos = 0;
                List<Integer> matches = new ArrayList<>();
                boolean remove = false;
                for ( String first : firsts ) {
                    if ( key.equals( first ) ) {
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
                    excludeBson( value, matches.stream().map( i -> excluded.get( i ).subList( 1, excluded.get( i ).size() ) ).collect( Collectors.toList() ) );
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
    public static boolean docExists( Object obj, List<String> path ) {
        obj = deserializeBsonIfNecessary( obj );
        if ( !(obj instanceof Map) ) {
            return false;
        }
        Map<String, ?> map = ((Map<String, ?>) obj);
        Iterator<String> iter = path.iterator();
        String current = iter.next();

        while ( map.containsKey( current ) ) {
            obj = map.get( current );
            if ( !iter.hasNext() ) {
                return true;
            }
            if ( !(obj instanceof Map) ) {
                return false;
            }
            map = (Map<String, ?>) map.get( current );
            current = iter.next();
        }

        return false;
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
