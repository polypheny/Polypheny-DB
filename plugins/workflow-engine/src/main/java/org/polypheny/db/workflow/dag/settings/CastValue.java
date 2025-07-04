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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class CastValue implements SettingValue {

    List<SingleCast> casts;


    public void validate( boolean allowDuplicateSource, boolean allowTarget, boolean allowJson, boolean singleCast ) throws IllegalArgumentException {
        if ( singleCast && casts.size() != 1 ) {
            throw new IllegalArgumentException( "Exactly 1 cast is expected but got " + casts.size() );
        }
        Set<String> sources = new HashSet<>();
        Set<String> targets = new HashSet<>();
        for ( SingleCast cast : casts ) {
            if ( !allowDuplicateSource ) {
                if ( cast.source.isBlank() ) {
                    throw new IllegalArgumentException( "Name must not be empty" );
                }
                if ( sources.contains( cast.source ) ) {
                    throw new IllegalArgumentException( "Duplicate source: " + cast.source );
                }
                sources.add( cast.source );
            }
            if ( allowTarget ) {
                String name = cast.getOutName();
                if ( targets.contains( name ) ) {
                    throw new IllegalArgumentException( "Duplicate target: " + name );
                }
                targets.add( name );
            } else if ( cast.hasTarget() ) {
                throw new IllegalArgumentException( "Specifying targets is not allowed: " + cast.target );
            }
            if ( !allowJson && cast.asJsonBoolean() ) {
                throw new IllegalArgumentException( "Marking a cast as JSON is not allowed: " + cast.getOutName() );
            }
            cast.buildType();
        }
    }


    public Map<String, SingleCast> asMap() {
        Map<String, SingleCast> map = new HashMap<>();
        for ( SingleCast cast : casts ) {
            map.put( cast.source, cast );
        }
        return map;
    }


    public AlgDataType asAlgDataType() {
        Builder builder = AlgDataTypeFactory.DEFAULT.builder();
        for ( SingleCast cast : casts ) {
            builder.add( cast.getOutName(), null, cast.getAlgDataType() );
        }
        return builder.build();
    }


    @Value
    public static class SingleCast {

        String source; // source (sub)field / column
        String target;

        PolyType type;
        boolean nullable;
        String collectionsType; // empty string or null for no collection, otherwise only ARRAY is valid
        Integer precision;
        Integer scale;
        //String defaultValue; // see Crud.createTable()
        Integer dimension;
        Integer cardinality;

        @Getter(AccessLevel.NONE)
        boolean asJson;


        @java.beans.ConstructorProperties({ "source", "target", "type", "nullable", "collectionsType", "precision", "scale", "dimension", "cardinality", "asJson" })
        public SingleCast( String source, String target, PolyType type, boolean nullable, String collectionsType, Integer precision, Integer scale, Integer dimension, Integer cardinality, boolean asJson ) {
            this.source = source;
            this.target = target;
            this.type = type;
            this.nullable = nullable;
            this.collectionsType = collectionsType;
            this.precision = precision;
            this.scale = scale;
            this.dimension = dimension;
            this.cardinality = cardinality;
            this.asJson = asJson;
        }


        @JsonProperty("asJson")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Boolean asJson() {
            return asJson ? true : null; // only serialize isJson if it's true -> hidden in most cases
        }


        public boolean asJsonBoolean() {
            return asJson; // separate method since asJson is only used for serialization
        }


        @JsonIgnore
        @NonFinal
        AlgDataType algDataType; // set with buildType


        @JsonIgnore
        @NonFinal
        Function<PolyValue, PolyValue> converter; // set with buildType

        @JsonIgnore
        @NonFinal
        PolyValue nullValue; // set with buildType


        @JsonIgnore
        public String getOutName() {
            return hasTarget() ? target : source;
        }


        @JsonIgnore
        public boolean isCollection() {
            return collectionsType != null && !collectionsType.isEmpty();
        }


        public boolean hasTarget() {
            return target != null && !target.isBlank();
        }


        /**
         * Should be called before any other methods are called
         */
        public void buildType() {
            try {
                if ( asJson ) {
                    algDataType = AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT );
                    return;
                }

                PolyType collectionsType = switch ( Objects.requireNonNullElse( this.collectionsType, "" ) ) {
                    case "ARRAY" -> PolyType.ARRAY;
                    case "" -> null;
                    default -> throw new IllegalArgumentException( "Unsupported collection type: " + this.collectionsType );
                };
                algDataType = LogicalColumn.getAlgDataType( AlgDataTypeFactory.DEFAULT,
                        precision,
                        scale,
                        type,
                        collectionsType,
                        cardinality,
                        dimension,
                        nullable );

                converter = PolyValue.getConverter( type );
                nullValue = PolyValue.getNull( PolyValue.classFrom( type ) );
            } catch ( Exception e ) {
                throw new IllegalArgumentException( e.getMessage() );
            }
        }


        public PolyValue castValue( PolyValue value ) {
            if ( value == null ) {
                return PolyNull.NULL;
            }
            if ( asJson ) {
                return value.toPolyJson();
            }

            if ( !isCollection() ) {
                if ( type.getFamily() == PolyTypeFamily.CHARACTER ) { // -> string casts succeed more often
                    return ActivityUtils.valueToPolyString( value );
                } else if ( value.isString() && value.asString().value.isEmpty() ) {
                    return PolyNull.NULL;
                } else if ( type == PolyType.BOOLEAN && value.isString() ) {
                    String str = value.asString().value.trim().toLowerCase( Locale.ROOT );
                    return PolyBoolean.of( str.equals( "true" ) || str.equals( "1" ) );
                } else if ( value.isString() && PolyType.DATETIME_TYPES.contains( type ) ) {
                    return ActivityUtils.castPolyTemporal( value.asString(), type );
                }
            } else if ( collectionsType.equals( "ARRAY" ) ) {
                if ( value.isList() ) {
                    return PolyList.of( value.asList().stream().map( converter ).toList() );
                }
                return PolyList.of( converter.apply( value ) );
            }
            return converter.apply( value );
        }


        public static SingleCast of( String source, PolyType type, boolean nullable ) {
            return SingleCast.of( source, type, null, nullable );
        }


        public static SingleCast of( String source, PolyType type, Integer precision, boolean nullable ) {
            return new SingleCast(
                    source,
                    null,
                    type,
                    nullable,
                    "",
                    precision,
                    null,
                    null,
                    null,
                    false
            );
        }


        public static SingleCast of( String source ) {
            return new SingleCast(
                    source,
                    null,
                    null,
                    true,
                    "",
                    null,
                    null,
                    null,
                    null,
                    true
            );
        }

    }


}
