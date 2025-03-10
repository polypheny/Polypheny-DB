/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class FilterValue implements SettingValue {

    List<Condition> conditions;
    SelectMode targetMode;
    boolean combineWithOr; // if false, the default is AND, meaning all predicates have to match


    @JsonIgnore
    Map<String, List<Predicate<PolyValue>>> cache = new HashMap<>(); // maps a field name to all applicable values

    @JsonIgnore
    static final int MAX_CACHE_SIZE = 100_000;


    public void validate( List<Operator> allowedOperators ) throws IllegalArgumentException {
        for ( Condition condition : conditions ) {
            if ( !allowedOperators.contains( condition.getOperator() ) ) {
                throw new IllegalArgumentException( "Operator " + condition.getOperator() + " is not allowed" );
            }
            condition.initialize( targetMode );
        }
    }


    /**
     * Returns a predicate that filters a given tuple.
     */
    public Predicate<List<PolyValue>> getRelPredicate( List<String> columns ) {
        List<List<Predicate<PolyValue>>> predicates = new ArrayList<>(); // outer list: column-idx, inner list: all predicates for that column
        for ( int i = 0; i < columns.size(); i++ ) {
            String name = columns.get( i );
            int idx = i; // make effectively final for stream
            predicates.add( switch ( targetMode ) {
                case EXACT -> conditions.stream().filter( c -> c.appliesTo( name, false ) )
                        .map( Condition::getPredicate ).toList();
                case REGEX -> conditions.stream().filter( c -> c.appliesTo( name, true ) )
                        .map( Condition::getPredicate ).toList();
                case INDEX -> conditions.stream().filter( c -> c.appliesTo( idx ) )
                        .map( Condition::getPredicate ).toList();
            } );
        }

        if ( predicates.stream().allMatch( List::isEmpty ) ) {
            return v -> true; // no condition is applicable
        }

        return ( row ) -> {
            for ( int i = 0; i < row.size(); i++ ) {
                Boolean tested = test( predicates.get( i ), row.get( i ) );
                if ( tested != null ) {
                    return tested;
                }
            }
            return !combineWithOr;
        };
    }


    /**
     * Returns a predicate that filters a given document.
     *
     * @param includeSubfields whether to scan subfields if mode is Regex.
     * @param pointer a pointer that makes all target fields act relative to it.
     */
    public Predicate<PolyDocument> getDocPredicate( boolean includeSubfields, String pointer ) {
        assert targetMode != SelectMode.INDEX;
        if ( conditions.isEmpty() ) {
            return d -> true;
        }

        if ( targetMode == SelectMode.REGEX ) {
            return ( doc ) -> {
                PolyValue root;
                try {
                    root = ActivityUtils.getSubValue( doc, pointer );
                } catch ( Exception e ) {
                    return true;
                }
                Boolean tested = recursiveTest( root, includeSubfields );
                return Objects.requireNonNullElse( tested, !combineWithOr );
            };
        }

        assert targetMode == SelectMode.EXACT;
        return doc -> {
            PolyValue root;
            try {
                root = ActivityUtils.getSubValue( doc, pointer );
            } catch ( Exception e ) {
                return true;
            }
            Boolean tested = recursiveTest( "", root );
            return Objects.requireNonNullElse( tested, !combineWithOr );
        };
    }


    @JsonIgnore
    public Predicate<PolyDictionary> getLpgPredicate() {
        assert targetMode != SelectMode.INDEX;
        return dict -> {
            Boolean tested = recursiveTest( dict, false );
            return Objects.requireNonNullElse( tested, !combineWithOr );
        };
    }


    /**
     * Returns true if the (entire) tuple matches for certain, false if it is rejected for certain
     * and null if both is still possible after evaluating this value.
     */
    private Boolean test( List<Predicate<PolyValue>> ps, PolyValue value ) {
        for ( Predicate<PolyValue> predicate : ps ) {
            if ( predicate.test( value ) ) {
                if ( combineWithOr ) {
                    return true;
                }
            } else {
                if ( !combineWithOr ) {
                    return false;
                }
            }
        }
        return null;
    }


    private List<Predicate<PolyValue>> getPredicates( String field ) {
        if ( cache.containsKey( field ) ) {
            return cache.get( field );
        }
        boolean isRegex = targetMode == SelectMode.REGEX;
        List<Predicate<PolyValue>> list = conditions.stream()
                .filter( c -> c.appliesTo( field, isRegex ) )
                .map( Condition::getPredicate ).toList();

        if ( cache.size() < MAX_CACHE_SIZE ) {
            cache.put( field, list );
        }
        return list;
    }


    private Boolean recursiveTest( PolyValue value, boolean nested ) {
        Boolean tested;
        return switch ( value.getType() ) {
            case DOCUMENT, MAP -> {
                Set<Entry<PolyString, PolyValue>> entries = (value.isDocument() ? value.asDocument() : value.asMap().asDictionary()).entrySet();
                for ( Entry<PolyString, PolyValue> entry : entries ) {
                    tested = test( getPredicates( entry.getKey().value ), entry.getValue() );
                    if ( tested != null ) {
                        yield tested;
                    }
                    if ( nested ) {
                        tested = recursiveTest( entry.getValue(), true );
                        if ( tested != null ) {
                            yield tested;
                        }
                    }
                }
                yield null;
            }
            case ARRAY -> {
                for ( PolyValue polyValue : value.asList() ) {
                    tested = recursiveTest( polyValue, nested );
                    if ( tested != null ) {
                        yield tested;
                    }
                }
                yield null;
            }
            default -> null;
        };
    }


    private Boolean recursiveTest( String parentPath, PolyValue value ) {
        Boolean tested;
        return switch ( value.getType() ) {
            case DOCUMENT -> {
                for ( Entry<PolyString, PolyValue> entry : value.asDocument().entrySet() ) {
                    String path = (parentPath.isEmpty() ? "" : (parentPath + ".")) + entry.getKey().value;
                    tested = test( getPredicates( path ), entry.getValue() );
                    if ( tested != null ) {
                        yield tested;
                    }
                    tested = recursiveTest( path, entry.getValue() );
                    if ( tested != null ) {
                        yield tested;
                    }
                }
                yield null;
            }
            case ARRAY -> {
                PolyList<?> list = value.asList();
                for ( int i = 0; i < list.size(); i++ ) {
                    PolyValue polyValue = list.get( i );
                    String path = (parentPath.isEmpty() ? "" : (parentPath + ".")) + i;
                    tested = test( getPredicates( path ), polyValue );
                    if ( tested != null ) {
                        yield tested;
                    }
                    tested = recursiveTest( path, polyValue );
                    if ( tested != null ) {
                        yield tested;
                    }
                }
                yield null;
            }
            default -> null;
        };
    }


    @JsonIgnore
    public RexNode getRexNode( AlgDataType type, RexBuilder builder ) {
        List<RexNode> conditionNodes = new ArrayList<>();

        for ( int i = 0; i < type.getFieldCount(); i++ ) {
            AlgDataTypeField field = type.getFields().get( i );
            String name = field.getName();
            int idx = i; // make effectively final for stream
            conditionNodes.addAll( switch ( targetMode ) {
                case EXACT -> conditions.stream().filter( c -> c.appliesTo( name, false ) )
                        .map( c -> c.getRexNode( builder, field.getType(), idx ) ).toList();
                case REGEX -> conditions.stream().filter( c -> c.appliesTo( name, true ) )
                        .map( c -> c.getRexNode( builder, field.getType(), idx ) ).toList();
                case INDEX -> conditions.stream().filter( c -> c.appliesTo( idx ) )
                        .map( c -> c.getRexNode( builder, field.getType(), idx ) ).toList();
            } );
        }
        if ( conditionNodes.isEmpty() ) {
            return null;
        }
        if ( conditionNodes.size() == 1 ) {
            return conditionNodes.get( 0 );
        }
        return builder.makeCall( OperatorRegistry.get( combineWithOr ? OperatorName.OR : OperatorName.AND ), conditionNodes );
    }


    public boolean canBeFused() {
        return conditions.stream().allMatch( c -> c.getOperator().canFuse );
    }


    public List<String> getMissingRequiredFields( List<String> names ) {
        return switch ( targetMode ) {
            case EXACT -> conditions.stream().map( Condition::getField )
                    .filter( f -> !names.contains( f ) ).toList();
            case REGEX -> List.of(); // regex cannot have missing fields
            case INDEX -> conditions.stream()
                    .filter( c -> c.getFieldIndex() >= names.size() )
                    .map( Condition::getField ).toList();
        };
    }


    @Value
    public static class Condition {

        String field;
        Operator operator;
        String value;
        boolean ignoreCase; // relevant for evaluating the condition, not for SelectMode

        @JsonIgnore
        @NonFinal
        Pattern fieldPattern;

        @JsonIgnore
        @NonFinal
        int fieldIndex;

        @JsonIgnore
        @NonFinal
        Predicate<PolyValue> predicate;

        @JsonIgnore
        @NonFinal
        Pattern valuePattern;

        @JsonIgnore
        @NonFinal
        List<String> values;

        @JsonIgnore
        Map<Class<? extends PolyValue>, PolyValue> convertedValues = new ConcurrentHashMap<>(); // a cache for reusing casted values


        public void initialize( SelectMode mode ) throws IllegalArgumentException {
            validate();
            try {
                switch ( mode ) {
                    case EXACT -> {
                    }
                    case REGEX -> {
                        fieldPattern = Pattern.compile( field );
                    }
                    case INDEX -> {
                        fieldIndex = Integer.parseInt( field );
                    }
                }
                predicate = constructPredicate();
                if ( operator == Operator.REGEX || operator == Operator.REGEX_NOT ) {
                    valuePattern = ignoreCase ? Pattern.compile( value, Pattern.CASE_INSENSITIVE ) : Pattern.compile( value );
                } else if ( operator == Operator.INCLUDED || operator == Operator.NOT_INCLUDED ) {
                    values = Arrays.stream( value.split( "," ) ).map( String::trim ).filter( s -> !s.isEmpty() ).toList();
                }
            } catch ( Exception e ) {
                throw new IllegalArgumentException( e.getMessage() );
            }
        }


        public boolean appliesTo( int idx ) {
            return fieldIndex == idx;
        }


        public boolean appliesTo( String name, boolean isRegex ) {
            if ( isRegex ) {
                return fieldPattern.matcher( name ).matches();
            }
            return field.equals( name );
        }


        private void validate() throws IllegalArgumentException {
            if ( field.isBlank() ) {
                throw new IllegalArgumentException( "Field must not be empty" );
            }
            if ( operator.hasValue && value == null ) {
                throw new IllegalArgumentException( "Value of '" + operator + "' condition must not be null" );
            }
            if ( !operator.allowBlankValue && value.isBlank() ) {
                throw new IllegalArgumentException( "Value of '" + operator + "' condition must not be empty" );
            }
        }


        private Predicate<PolyValue> constructPredicate() {
            return v -> switch ( operator ) {
                case EQUALS -> isEqual( v );
                case NOT_EQUALS -> !isEqual( v );
                case GREATER_THAN -> compareWith( v ) > 0;
                case LESS_THAN -> compareWith( v ) < 0;
                case GREATER_THAN_EQUALS -> compareWith( v ) > 0 || isEqual( v );
                case LESS_THAN_EQUALS -> compareWith( v ) < 0 || isEqual( v );
                case REGEX -> valuePattern.matcher( ActivityUtils.valueToString( v ).value ).matches();
                case REGEX_NOT -> !valuePattern.matcher( ActivityUtils.valueToString( v ).value ).matches();
                case NULL -> v == null || v.isNull();
                case NON_NULL -> v != null && !v.isNull();
                case INCLUDED -> includes( v );
                case NOT_INCLUDED -> !includes( v );
                case CONTAINS -> contains( v );
                case NOT_CONTAINS -> !contains( v );
                case HAS_KEY -> hasKey( v );
                case IS_ARRAY -> v != null && v.isList();
                case IS_OBJECT -> v != null && (v.isMap() || v.isDocument());
                default -> throw new NotImplementedException( "Operator " + operator + " is not implemented" );
            };
        }


        @JsonIgnore
        public RexNode getRexNode( RexBuilder builder, AlgDataType type, int idx ) {
            RexIndexRef indexRef = builder.makeInputRef( type, idx );
            return switch ( operator ) {
                case EQUALS -> isEqualAsRex( builder, indexRef, type );
                case NOT_EQUALS -> not( builder, isEqualAsRex( builder, indexRef, type ) );
                case GREATER_THAN -> builder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN ), indexRef, valueAsLiteral( type ) );
                case LESS_THAN -> builder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN ), indexRef, valueAsLiteral( type ) );
                case GREATER_THAN_EQUALS -> builder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), indexRef, valueAsLiteral( type ) );
                case LESS_THAN_EQUALS -> builder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), indexRef, valueAsLiteral( type ) );
                case NULL -> builder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), indexRef );
                case NON_NULL -> builder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), indexRef );
                case INCLUDED -> includesAsRex( builder, indexRef, type );
                case NOT_INCLUDED -> not( builder, includesAsRex( builder, indexRef, type ) );
                default -> throw new IllegalStateException( "Operator " + operator + " cannot be fused." );
            };
        }


        private boolean isEqual( PolyValue v ) {
            if ( v.isNumber() ) {
                return v.equals( valueAsNumber() );
            }
            String s = ActivityUtils.valueToString( v ).value;
            return ignoreCase ? s.equalsIgnoreCase( value ) : s.equals( value );
        }


        private boolean includes( PolyValue v ) {
            return values.stream().anyMatch( entry -> {
                if ( v.isNumber() ) {
                    return v.equals( toNumber( entry ) );
                }
                String s = ActivityUtils.valueToString( v ).value;
                return ignoreCase ? s.equalsIgnoreCase( entry ) : s.equals( entry );
            } );
        }


        private boolean contains( PolyValue v ) {
            if ( v.isList() ) {
                return v.asList().stream().anyMatch( this::isEqual );
            } else if ( v.isDocument() ) {
                return v.asDocument().values().stream().anyMatch( this::isEqual );
            } else {
                return isEqual( v );
            }
        }


        private boolean hasKey( PolyValue v ) {
            if ( v.isList() ) {
                int i = valueAsNumber().intValue();
                return i >= 0 && v.asList().size() > i;
            }
            if ( v.isMap() || v.isDocument() ) {
                PolyString s = valueAsString();
                if ( ignoreCase ) {
                    return v.asDocument().keySet().stream().anyMatch(
                            key -> key.value.equalsIgnoreCase( s.value ) );
                }
                return v.asMap().containsKey( s );
            }
            return false;
        }


        private RexNode valueAsLiteral( AlgDataType outType ) {
            return valueAsLiteral( value, outType );
        }


        private RexNode valueAsLiteral( String value, AlgDataType outType ) {
            PolyValue polyValue = ActivityUtils.stringToPolyValue( value, outType.getPolyType() );
            return ActivityUtils.getRexLiteral( polyValue, outType );
        }


        private RexNode isEqualAsRex( RexBuilder builder, RexIndexRef indexRef, AlgDataType type ) {
            if ( ignoreCase ) {
                RexNode left = builder.makeCall( OperatorRegistry.get( OperatorName.UPPER ), indexRef );
                RexNode right = builder.makeCall( OperatorRegistry.get( OperatorName.UPPER ), valueAsLiteral( type ) );
                return builder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), left, right );
            }
            return builder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), indexRef, valueAsLiteral( type ) );
        }


        private RexNode includesAsRex( RexBuilder builder, RexIndexRef indexRef, AlgDataType type ) {
            List<RexNode> nodes = values.stream().map( v -> {
                if ( ignoreCase ) {
                    RexNode left = builder.makeCall( OperatorRegistry.get( OperatorName.UPPER ), indexRef );
                    RexNode right = builder.makeCall( OperatorRegistry.get( OperatorName.UPPER ), valueAsLiteral( v, type ) );
                    return builder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), left, right );
                }
                return builder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), indexRef, valueAsLiteral( v, type ) );
            } ).toList();
            if ( nodes.size() == 1 ) {
                return nodes.get( 0 );
            }
            return builder.makeCall( OperatorRegistry.get( OperatorName.OR ), nodes );
        }


        private static RexNode not( RexBuilder builder, RexNode node ) {
            return builder.makeCall( OperatorRegistry.get( OperatorName.NOT ), node );
        }


        /**
         * @return 1 if v is greater than this.value
         */
        private int compareWith( PolyValue v ) {
            if ( v.isTemporal() ) {
                return switch ( v.type ) {
                    case DATE -> v.compareTo( valueAsDate() );
                    case TIME -> v.compareTo( valueAsTime() );
                    case TIMESTAMP -> v.compareTo( valueAsTimestamp() );
                    default -> throw new IllegalArgumentException( "Unsupported type: " + v.type );
                };
            }
            if ( v.isNumber() ) {
                return v.compareTo( valueAsNumber() );
            }
            return ActivityUtils.valueToString( v ).compareTo( valueAsString() );
        }


        private PolyNumber valueAsNumber() {
            return (PolyNumber) convertedValues.computeIfAbsent( PolyNumber.class, k -> toNumber( value ) );
        }


        private PolyString valueAsString() {
            return (PolyString) convertedValues.computeIfAbsent( PolyString.class, k -> PolyString.of( value ) );
        }


        private PolyDate valueAsDate() {
            return (PolyDate) convertedValues.computeIfAbsent( PolyDate.class,
                    k -> ActivityUtils.castPolyTemporal( valueAsString(), PolyType.DATE ) );
        }


        private PolyTime valueAsTime() {
            return (PolyTime) convertedValues.computeIfAbsent( PolyTime.class,
                    k -> ActivityUtils.castPolyTemporal( valueAsString(), PolyType.TIME ) );
        }


        private PolyTimestamp valueAsTimestamp() {
            return (PolyTimestamp) convertedValues.computeIfAbsent( PolyTimestamp.class,
                    k -> ActivityUtils.castPolyTemporal( valueAsString(), PolyType.TIMESTAMP ) );
        }


        private static PolyNumber toNumber( String value ) {
            try {
                Number number = NumberFormat.getInstance( Locale.US ).parse( value );
                if ( number instanceof Long ) {
                    return PolyLong.of( number );
                } else {
                    return PolyDouble.of( number );
                }
            } catch ( ParseException e ) {
                throw new NumberFormatException( "Value is not a valid number: " + value );
            }
        }

    }


    /**
     * The possible operators to be used in conditions.
     * Important limitation: a FIELD_EXISTS operator is not possible!
     * Why? We iterate over fields and test whether they meet conditions.
     * Non-existent fields cannot be tested.
     * A workaround is the HAS_KEY operator which tests for subfield existence.
     * Otherwise, existence has to be tested separately.
     */
    public enum Operator {
        EQUALS( true, true ),
        NOT_EQUALS( true, true ),
        GREATER_THAN,
        LESS_THAN,
        GREATER_THAN_EQUALS,
        LESS_THAN_EQUALS,
        REGEX( true, false, false ),
        REGEX_NOT( true, false, false ),
        NULL( false ),
        NON_NULL( false ),
        /**
         * Specify a list of values. Evaluates to true if at least one of its entries is equal to the value.
         */
        INCLUDED,
        NOT_INCLUDED,
        /**
         * Whether the value appears as an entry in an array or value in an object or is equal to an atomic value
         */
        CONTAINS( true, false, false ),
        NOT_CONTAINS( true, false, false ),
        HAS_KEY, // for array: corresponds to index, for Map: the actual key
        IS_ARRAY( false, true, false ),
        IS_OBJECT( false, true, false ),
        ;

        public final boolean hasValue; // whether value must not be null
        public final boolean allowBlankValue; // whether the value can be a blank string
        public final boolean canFuse;


        Operator() {
            this( true );
        }


        Operator( boolean hasValue ) {
            this( hasValue, !hasValue );
        }


        Operator( boolean hasValue, boolean allowBlank ) {
            this( hasValue, allowBlank, true );
        }


        Operator( boolean hasValue, boolean allowBlank, boolean canFuse ) {
            this.hasValue = hasValue;
            this.allowBlankValue = allowBlank;
            this.canFuse = canFuse;
        }
    }

}
