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

package org.polypheny.db.sql.sql.util.freshness;


import java.sql.Timestamp;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.replication.freshness.FreshnessExtractor;
import org.polypheny.db.processing.replication.freshness.FreshnessManager.EvaluationType;
import org.polypheny.db.processing.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeRuntimeException;
import org.polypheny.db.processing.replication.freshness.exceptions.UnknownFreshnessTimeUnitRuntimeException;
import org.polypheny.db.processing.replication.freshness.exceptions.UnsupportedFreshnessSpecificationRuntimeException;
import org.polypheny.db.processing.replication.freshness.properties.FreshnessSpecification;
import org.polypheny.db.sql.sql.SqlTimestampLiteral;


public class SqlFreshnessExtractor extends FreshnessExtractor {


    private Node toleratedFreshness;
    private Identifier rawEvaluationType;
    private Identifier unit;


    public SqlFreshnessExtractor(
            Node toleratedFreshness,
            Identifier rawEvaluationType,
            Identifier unit ) {
        this.toleratedFreshness = toleratedFreshness;
        this.rawEvaluationType = rawEvaluationType;
        this.unit = unit;
    }


    public FreshnessSpecification extractFreshnessSpecification()
            throws UnknownFreshnessTimeUnitRuntimeException,
            UnknownFreshnessEvaluationTypeRuntimeException,
            UnsupportedFreshnessSpecificationRuntimeException {

        extractSqlFreshnessSpecification();
        return freshnessSpecification;
    }


    private void extractSqlFreshnessSpecification()
            throws UnknownFreshnessTimeUnitRuntimeException,
            UnknownFreshnessEvaluationTypeRuntimeException,
            UnsupportedFreshnessSpecificationRuntimeException {

        if ( toleratedFreshness == null || rawEvaluationType == null || unit == null ) {
            throw new RuntimeException( "Freshness Extraction failed. " );
        }
        extractFromNodeLists();
    }


    private void extractFromNodeLists() throws UnknownFreshnessEvaluationTypeRuntimeException, UnknownFreshnessTimeUnitRuntimeException, UnsupportedFreshnessSpecificationRuntimeException {

        EvaluationType tmpEvaluationType;
        double tmpFreshnessIndex = -1.0;

        // Serves as the lower bound of accepted freshness
        Timestamp requestedTimestamp = null;

        switch ( rawEvaluationType.toString().toUpperCase() ) {
            case "DELAY":

                tmpEvaluationType = EvaluationType.DELAY;
                long invocationTimestamp = System.currentTimeMillis();
                long timeDifference = 0;
                long specifiedTimeDelta = Long.valueOf( toleratedFreshness.toString() );

                // Initially set to 1000ms = 1s
                int timeMultiplier = 1000;

                switch ( unit.toString().toUpperCase() ) {
                    case "MINUTE":
                        // Convert to milliseconds
                        timeMultiplier *= 60;
                        break;

                    case "HOUR":
                        // Convert to milliseconds
                        timeMultiplier *= 60 * 60;
                        break;

                    default:
                        throw new UnknownFreshnessTimeUnitRuntimeException( unit.toString().toUpperCase() );
                }

                // Transform currentTimestamp to the tolerated level of freshness
                timeDifference = specifiedTimeDelta * timeMultiplier;
                invocationTimestamp = invocationTimestamp - timeDifference;
                requestedTimestamp = new Timestamp( invocationTimestamp );

                break;

            case "TIMESTAMP":
                tmpEvaluationType = EvaluationType.TIMESTAMP;
                requestedTimestamp = Timestamp.valueOf( ((SqlTimestampLiteral) toleratedFreshness).getValue().toString() );

                break;

            case "PERCENTAGE":

                double percentageValue = Double.valueOf( toleratedFreshness.toString() );
                if ( percentageValue > 0.0 && percentageValue <= 100.0 ) {
                    tmpEvaluationType = EvaluationType.PERCENTAGE;
                    tmpFreshnessIndex = percentageValue / 100;
                } else {
                    throw new UnsupportedFreshnessSpecificationRuntimeException( EvaluationType.PERCENTAGE, toleratedFreshness.toString() );
                }
                break;

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( rawEvaluationType.toString().toUpperCase() );
        }

        setFreshnessSpecification( requestedTimestamp, tmpEvaluationType, tmpFreshnessIndex );
    }


    /**
     * Needed to modify strings otherwise the SQL-input 'a' will be also added as the value "'a'" and not as "a" as intended
     * Essentially removes " ' " at the start and end of value
     *
     * @param node Node to be modified
     * @return String
     */
    private static String getValueOfSqlNode( Node node ) {

        if ( node instanceof Literal ) {
            return ((Literal) node).toValue();
        }
        return node.toString();
    }

}