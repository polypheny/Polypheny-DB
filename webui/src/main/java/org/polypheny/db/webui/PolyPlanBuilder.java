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

package org.polypheny.db.webui;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgParser;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Pair;

public class PolyPlanBuilder {

    private PolyPlanBuilder() {
        // This is a utility class
    }


    /**
     * Creates a AlgNode tree from the given PolyAlg representation
     *
     * @param polyAlg string representing the AlgNode tree serialized as PolyAlg
     * @param statement transaction statement
     * @return AlgRoot with {@code AlgRoot.alg} being the top node of tree
     * @throws NodeParseException if the parser is not able to construct the intermediary PolyAlgNode tree
     * @throws RuntimeException if polyAlg cannot be parsed into a valid AlgNode tree
     */
    public static AlgRoot buildFromPolyAlg( String polyAlg, PlanType planType, Statement statement ) throws NodeParseException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, snapshot );
        return buildFromPolyAlg( polyAlg, planType, snapshot, cluster );
    }


    /**
     * Creates a AlgNode tree from the given PolyAlg representation not related to a statement.
     *
     * @param polyAlg string representing the AlgNode tree serialized as PolyAlg
     * @return AlgRoot with {@code AlgRoot.alg} being the top node of tree
     * @throws NodeParseException if the parser is not able to construct the intermediary PolyAlgNode tree
     * @throws RuntimeException if polyAlg cannot be parsed into a valid AlgNode tree
     */
    public static AlgRoot buildFromPolyAlg( String polyAlg, PlanType planType ) throws NodeParseException {
        Snapshot snapshot = Catalog.snapshot();
        AlgCluster cluster = AlgCluster.create(
                new VolcanoPlanner(), new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, snapshot );
        return buildFromPolyAlg( polyAlg, planType, snapshot, cluster );
    }


    private static AlgRoot buildFromPolyAlg( String polyAlg, PlanType planType, Snapshot snapshot, AlgCluster cluster ) throws NodeParseException {
        PolyAlgToAlgConverter converter = new PolyAlgToAlgConverter( planType, snapshot, cluster );

        PolyAlgParser parser = PolyAlgParser.create( polyAlg );
        PolyAlgNode node = (PolyAlgNode) parser.parseQuery();
        return converter.convert( node );

    }


    public static Pair<List<PolyValue>, List<AlgDataType>> translateDynamicParams( List<String> vals, List<String> types ) {
        if ( vals.size() != types.size() ) {
            throw new GenericRuntimeException( "Number of values does not match number of types!" );
        }

        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        List<PolyValue> translatedVals = new ArrayList<>();
        List<AlgDataType> translatedTypes = new ArrayList<>();
        for ( int i = 0; i < vals.size(); i++ ) {
            String s = vals.get( i );
            AlgDataType t = convertType( types.get( i ), factory );

            PolyValue value = switch ( t.getPolyType() ) {
                case BOOLEAN -> PolyBoolean.of( Boolean.parseBoolean( s ) );
                case TINYINT, SMALLINT, INTEGER -> PolyInteger.of( Integer.parseInt( s ) );
                case BIGINT -> PolyLong.of( Long.parseLong( s ) );
                case DECIMAL -> PolyBigDecimal.of( s );
                case FLOAT, REAL -> PolyFloat.of( Float.parseFloat( s ) );
                case DOUBLE -> PolyDouble.of( Double.parseDouble( s ) );
                case DATE -> PolyDate.of( Long.parseLong( s ) );
                case TIME -> PolyTime.of( Long.parseLong( s ) );
                case TIMESTAMP -> PolyTimestamp.of( Long.parseLong( s ) );
                case CHAR, VARCHAR -> PolyString.of( s );
                default -> throw new NotImplementedException();
            };
            translatedVals.add( value );
            translatedTypes.add( t );


        }
        return Pair.of( translatedVals, translatedTypes );
    }


    private static AlgDataType convertType( String t, AlgDataTypeFactory factory ) {
        //e.g. t = "CHAR(5)"
        String[] parts = t.split( "\\(" );
        PolyType type = PolyType.valueOf( parts[0] );
        if ( parts.length == 1 ) {
            return factory.createPolyType( type );
        }
        String[] args = parts[1].substring( 0, parts[1].length() - 1 ).split( "," );

        return switch ( args.length ) {
            case 1 -> factory.createPolyType( type, Integer.parseInt( args[0].trim() ) );
            case 2 -> factory.createPolyType( type, Integer.parseInt( args[0].trim() ), Integer.parseInt( args[1].trim() ) );
            default -> throw new GenericRuntimeException( "Unexpected number of type arguments: " + args.length );
        };

    }

}
