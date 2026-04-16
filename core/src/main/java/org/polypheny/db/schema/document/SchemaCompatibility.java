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
package org.polypheny.db.schema.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.lang.Nullable;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.schema.document.DocumentSchema.AllOfNode;
import org.polypheny.db.schema.document.DocumentSchema.AnyOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ArrayNode;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.DocumentSchema.NotNode;
import org.polypheny.db.schema.document.DocumentSchema.ObjectNode;
import org.polypheny.db.schema.document.DocumentSchema.OneOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ScalarNode;
import org.polypheny.db.type.PolyType;

/**
 * Compatibility checks for schema changes.
 */
public final class SchemaCompatibility {

    private SchemaCompatibility() {
    }


    public static boolean isCompatible( @Nullable DocumentSchema current, @Nullable DocumentSchema proposed ) {
        if ( proposed == null ) {
            return true;
        }

        if ( current == null ) {
            return false;
        }

        DocumentSchema.AdditionalProperties currentRootAdditionalProperties = current.additionalProperties() != null ? current.additionalProperties() : DocumentSchema.AdditionalProperties.ALLOW;

        DocumentSchema.AdditionalProperties proposedRootAdditionalProperties = proposed.additionalProperties() != null ? proposed.additionalProperties() : currentRootAdditionalProperties;

        if ( currentRootAdditionalProperties == DocumentSchema.AdditionalProperties.ALLOW && proposedRootAdditionalProperties == DocumentSchema.AdditionalProperties.FORBID ) {
            return false;
        }

        return isObjectCompatible( current.root(), proposed.root(), currentRootAdditionalProperties, proposedRootAdditionalProperties );
    }


    private static boolean isObjectCompatible( ObjectNode current, ObjectNode proposed, DocumentSchema.AdditionalProperties inheritedCurrentAdditionalProperties, DocumentSchema.AdditionalProperties inheritedProposedAdditionalProperties ) {

        DocumentSchema.AdditionalProperties currentAdditionalProperties = effectiveAdditionalProperties( current.additionalProperties, inheritedCurrentAdditionalProperties );
        DocumentSchema.AdditionalProperties proposedAdditionalProperties = effectiveAdditionalProperties( proposed.additionalProperties, inheritedProposedAdditionalProperties );

        Set<String> currentRequired = current.effectiveRequired();
        Set<String> proposedRequired = proposed.effectiveRequired();

        for ( String key : proposedRequired ) {
            if ( !currentRequired.contains( key ) ) {
                return false;
            }
        }

        for ( Map.Entry<String, Node> entry : proposed.properties.entrySet() ) {
            String key = entry.getKey();
            Node currentChild = current.properties.get( key );

            if ( currentChild == null ) {
                if ( proposedRequired.contains( key ) ) {
                    return false;
                }
                continue;
            }

            if ( !isNodeCompatible( currentChild, entry.getValue(), currentAdditionalProperties, proposedAdditionalProperties ) ) {
                return false;
            }
        }

        if ( proposedAdditionalProperties == DocumentSchema.AdditionalProperties.FORBID ) {
            for ( String key : current.properties.keySet() ) {
                if ( !proposed.properties.containsKey( key ) ) {
                    return false;
                }
            }
        }

        if ( currentAdditionalProperties == DocumentSchema.AdditionalProperties.ALLOW && proposedAdditionalProperties == DocumentSchema.AdditionalProperties.FORBID ) {
            return false;
        }

        if ( tightensLowerBound( current.minProperties, proposed.minProperties ) ) {
            return false;
        }

        if ( tightensUpperBound( current.maxProperties, proposed.maxProperties ) ) {
            return false;
        }

        return true;
    }


    private static boolean isNodeCompatible( Node current, Node proposed, DocumentSchema.AdditionalProperties inheritedCurrentAdditionalProperties, DocumentSchema.AdditionalProperties inheritedProposedAdditionalProperties ) {

        if ( isCompositionNode( current ) || isCompositionNode( proposed ) ) {
            return false;
        }

        if ( current instanceof ScalarNode currentScalar && proposed instanceof ScalarNode proposedScalar ) {
            return isScalarCompatible( currentScalar, proposedScalar );
        }

        if ( current instanceof ObjectNode currentObject && proposed instanceof ObjectNode proposedObject ) {
            return isObjectCompatible( currentObject, proposedObject, inheritedCurrentAdditionalProperties, inheritedProposedAdditionalProperties );
        }

        if ( current instanceof ArrayNode currentArray && proposed instanceof ArrayNode proposedArray ) {
            return isArrayCompatible( currentArray, proposedArray, inheritedCurrentAdditionalProperties, inheritedProposedAdditionalProperties );
        }

        return false;
    }


    private static boolean isCompositionNode( Node node ) {
        return node instanceof AnyOfNode || node instanceof OneOfNode || node instanceof AllOfNode || node instanceof NotNode;
    }


    private static boolean isArrayCompatible( ArrayNode current, ArrayNode proposed, DocumentSchema.AdditionalProperties inheritedCurrentAdditionalProperties, DocumentSchema.AdditionalProperties inheritedProposedAdditionalProperties ) {

        if ( !isNodeCompatible( current.items, proposed.items, inheritedCurrentAdditionalProperties, inheritedProposedAdditionalProperties ) ) {
            return false;
        }

        if ( tightensLowerBound( current.minItems, proposed.minItems ) ) {
            return false;
        }

        if ( tightensUpperBound( current.maxItems, proposed.maxItems ) ) {
            return false;
        }

        boolean currentUniqueItems = Boolean.TRUE.equals( current.uniqueItems );
        boolean proposedUniqueItems = Boolean.TRUE.equals( proposed.uniqueItems );

        if ( !currentUniqueItems && proposedUniqueItems ) {
            return false;
        }

        return true;
    }


    private static boolean isScalarCompatible( ScalarNode current, ScalarNode proposed ) {
        if ( !isTypeSupersetOrWidening( current.types, proposed.types ) ) {
            return false;
        }

        if ( tightensLowerBound( current.minLength, proposed.minLength ) ) {
            return false;
        }

        if ( tightensUpperBound( current.maxLength, proposed.maxLength ) ) {
            return false;
        }

        if ( current.pattern == null && proposed.pattern != null ) {
            return false;
        }

        if ( current.pattern != null && proposed.pattern != null && !current.pattern.equals( proposed.pattern ) ) {
            return false;
        }

        if ( tightensLowerBound( current.minimum, proposed.minimum ) ) {
            return false;
        }

        if ( tightensUpperBound( current.maximum, proposed.maximum ) ) {
            return false;
        }

        if ( current.multipleOf == null && proposed.multipleOf != null ) {
            return false;
        }

        if ( current.multipleOf != null && proposed.multipleOf != null && current.multipleOf.compareTo( proposed.multipleOf ) != 0 ) {
            return false;
        }

        if ( current.constValue == null && proposed.constValue != null ) {
            return false;
        }

        if ( current.constValue != null && proposed.constValue != null && !current.constValue.equals( proposed.constValue ) ) {
            return false;
        }

        if ( current.enumValues == null && proposed.enumValues != null ) {
            return false;
        }

        if ( current.enumValues != null && proposed.enumValues != null && !enumIsSuperset( current.enumValues, proposed.enumValues ) ) {
            return false;
        }

        return true;
    }


    private static boolean enumIsSuperset( List<JsonNode> currentEnumValues, List<JsonNode> proposedEnumValues ) {
        Set<JsonNode> proposedValues = new HashSet<>( proposedEnumValues );
        return proposedValues.containsAll( currentEnumValues );
    }


    private static boolean isTypeSupersetOrWidening( List<PolyType> currentTypes, List<PolyType> proposedTypes ) {
        if ( currentTypes == null || currentTypes.isEmpty() ) {
            return true;
        }

        if ( proposedTypes == null || proposedTypes.isEmpty() ) {
            return false;
        }

        Set<PolyType> proposedTypeSet = new HashSet<>( proposedTypes );

        for ( PolyType currentType : currentTypes ) {
            if ( proposedTypeSet.contains( currentType ) ) {
                continue;
            }

            if ( isIntegerType( currentType ) ) {
                boolean hasNumericType = false;

                for ( PolyType proposedType : proposedTypes ) {
                    if ( isNumericType( proposedType ) ) {
                        hasNumericType = true;
                        break;
                    }
                }

                if ( hasNumericType ) {
                    continue;
                }
            }

            return false;
        }

        return true;
    }


    private static boolean isIntegerType( PolyType type ) {
        return type == PolyType.TINYINT || type == PolyType.SMALLINT || type == PolyType.INTEGER || type == PolyType.BIGINT;
    }


    private static boolean isNumericType( PolyType type ) {
        return isIntegerType( type ) || type == PolyType.DECIMAL || type == PolyType.FLOAT || type == PolyType.REAL || type == PolyType.DOUBLE;
    }


    private static DocumentSchema.AdditionalProperties effectiveAdditionalProperties( DocumentSchema.AdditionalProperties nodeAdditionalProperties, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        if ( nodeAdditionalProperties == null || nodeAdditionalProperties == DocumentSchema.AdditionalProperties.INHERIT ) {
            return inheritedAdditionalProperties;
        }

        return nodeAdditionalProperties;
    }


    private static boolean tightensLowerBound( Integer current, Integer proposed ) {
        int currentValue = current == null ? 0 : current;
        int proposedValue = proposed == null ? 0 : proposed;
        return proposedValue > currentValue;
    }


    private static boolean tightensUpperBound( Integer current, Integer proposed ) {
        if ( proposed == null ) {
            return false;
        }

        if ( current == null ) {
            return true;
        }

        return proposed < current;
    }


    private static boolean tightensLowerBound( BigDecimal current, BigDecimal proposed ) {
        if ( proposed == null ) {
            return false;
        }

        if ( current == null ) {
            return true;
        }

        return proposed.compareTo( current ) > 0;
    }


    private static boolean tightensUpperBound( BigDecimal current, BigDecimal proposed ) {
        if ( proposed == null ) {
            return false;
        }

        if ( current == null ) {
            return true;
        }

        return proposed.compareTo( current ) < 0;
    }

}