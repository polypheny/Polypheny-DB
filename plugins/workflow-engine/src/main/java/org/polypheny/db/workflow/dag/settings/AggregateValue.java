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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Value;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class AggregateValue implements SettingValue {

    List<AggregateEntry> aggregates;


    @JsonCreator
    public AggregateValue( List<AggregateEntry> aggregates ) {
        this.aggregates = aggregates;
    }


    @Override
    public JsonNode toJson() {
        return MAPPER.valueToTree( aggregates );
    }


    public void validate( Collection<String> allowedFunctions ) throws IllegalArgumentException {
        for ( AggregateEntry entry : aggregates ) {
            if ( !allowedFunctions.contains( entry.getFunction() ) ) {
                throw new IllegalArgumentException( "Invalid function: " + entry.getFunction() );
            }
            if ( entry.target.isEmpty() ) {
                throw new IllegalArgumentException( "Target field must not be empty." );
            }
        }
    }


    public void validate( AlgDataType type, @Nullable Collection<String> reservedCols ) throws IllegalArgumentException {
        Set<String> cols = new HashSet<>( type.getFieldNames() );
        for ( AggregateEntry entry : aggregates ) {
            if ( !cols.contains( entry.getTarget() ) ) {
                throw new IllegalArgumentException( "Target column does not exist: " + entry.getTarget() );
            }
            if ( reservedCols != null && reservedCols.contains( entry.getTarget() ) ) {
                throw new IllegalArgumentException( "Not a valid aggregate column: " + entry.getTarget() );
            }
        }
    }


    public List<AggregateCall> toAggregateCalls( AlgNode input, Collection<String> groupCols ) {
        Set<String> usedAliases = new HashSet<>( groupCols );
        List<AggregateCall> calls = new ArrayList<>();
        for ( AggregateEntry entry : aggregates ) {
            calls.add( entry.toAggregateCall( input, usedAliases ) );
        }
        return calls;
    }


    public List<LaxAggregateCall> toLaxAggregateCalls( int inputIndex, Collection<String> groupCols ) {
        Set<String> usedAliases = new HashSet<>( groupCols );
        List<LaxAggregateCall> calls = new ArrayList<>();
        for ( AggregateEntry entry : aggregates ) {
            calls.add( entry.toLaxAggregateCall( inputIndex, usedAliases ) );
        }
        return calls;
    }


    @JsonIgnore
    public List<String> getTargets() {
        List<String> targets = new ArrayList<>();
        for ( AggregateEntry entry : aggregates ) {
            targets.add( entry.getTarget() );
        }
        return targets;
    }


    public void addUniquifiedAliases( Set<String> usedAliases ) {
        for ( AggregateEntry entry : aggregates ) {
            entry.deriveAlias( usedAliases );
        }
    }


    public void addAggregateColumns( AlgDataTypeFactory.Builder builder, AlgDataType inType, AlgDataTypeFactory factory, Collection<String> groupCols ) {
        Set<String> usedAliases = new HashSet<>( groupCols );
        for ( AggregateEntry entry : aggregates ) {
            builder.add( entry.deriveAlias( usedAliases ), null, entry.getColumnType( inType, factory ) ).nullable( true );
        }
    }


    @Value
    public static class AggregateEntry {

        String target;
        String function;
        String alias;


        private AggregateCall toAggregateCall( AlgNode input, Set<String> usedAliases ) {
            List<String> names = input.getTupleType().getFieldNames();
            int i = names.indexOf( target );
            return AggregateCall.create(
                    getAggFunction(),
                    false,
                    false,
                    List.of( i ),
                    -1,
                    AlgCollations.EMPTY,
                    0,
                    input,
                    null,
                    deriveAlias( usedAliases )
            );
        }


        private LaxAggregateCall toLaxAggregateCall( int inputIndex, Set<String> usedAliases ) {
            String derivedAlias = deriveAlias( usedAliases );
            return LaxAggregateCall.create(
                    derivedAlias,
                    getAggFunction(),
                    ActivityUtils.getDocRexNameRef( target, inputIndex )
            );
        }


        private String deriveAlias( Set<String> usedAliases ) {
            String derivedAlias = (alias == null || alias.isBlank()) ? function.toLowerCase() + "_" + target : alias;
            derivedAlias = ValidatorUtil.uniquify( derivedAlias, usedAliases, ValidatorUtil.EXPR_SUGGESTER ); // automatically updates usedAliases
            return derivedAlias;
        }


        private AlgDataType getColumnType( AlgDataType inType, AlgDataTypeFactory factory ) {
            return function.equals( "COUNT" ) ?
                    factory.createPolyType( PolyType.BIGINT ) :
                    inType.getField( target, true, false ).getType();
        }


        @JsonIgnore
        private AggFunction getAggFunction() {
            return OperatorRegistry.getAgg( OperatorName.valueOf( function ) );
        }

    }

}
