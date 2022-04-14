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

package org.polypheny.db.sql.sql.util.replication.freshness;


import java.sql.Timestamp;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.replication.freshness.FreshnessExtractor;
import org.polypheny.db.replication.freshness.FreshnessManager.EvaluationType;
import org.polypheny.db.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeRuntimeException;
import org.polypheny.db.replication.freshness.exceptions.UnknownFreshnessTimeUnitRuntimeException;
import org.polypheny.db.replication.freshness.exceptions.UnsupportedFreshnessSpecificationRuntimeException;
import org.polypheny.db.replication.freshness.properties.FreshnessSpecification;
import org.polypheny.db.sql.sql.SqlTimestampLiteral;


public class SqlFreshnessExtractor extends FreshnessExtractor {


    public SqlFreshnessExtractor(
            Node toleratedFreshness,
            Identifier rawEvaluationType,
            Identifier unit ) {
        super( toleratedFreshness, rawEvaluationType, unit );
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
        long timeDelay = 0;

        // Serves as the lower bound of accepted freshness
        Timestamp requestedTimestamp = null;

        switch ( rawEvaluationType.toString().toUpperCase() ) {

            case "ABSOLUTE_DELAY":

                tmpEvaluationType = EvaluationType.ABSOLUTE_DELAY;
                long invocationTimestamp = System.currentTimeMillis();

                long timeDifference = 0;
                timeDifference = generateTimeDifference( tmpEvaluationType );

                invocationTimestamp = invocationTimestamp - timeDifference;
                requestedTimestamp = new Timestamp( invocationTimestamp );

                break;

            case "TIMESTAMP":

                tmpEvaluationType = EvaluationType.TIMESTAMP;
                requestedTimestamp = Timestamp.valueOf( ((SqlTimestampLiteral) toleratedFreshness).getValue().toString() );

                break;

            case "RELATIVE_DELAY":

                tmpEvaluationType = EvaluationType.RELATIVE_DELAY;
                timeDelay = generateTimeDifference( tmpEvaluationType );
                break;

            case "PERCENTAGE":

                double percentageValue = Double.valueOf( toleratedFreshness.toString() );
                if ( percentageValue >= 0.0 && percentageValue <= 100.0 ) {
                    tmpEvaluationType = EvaluationType.PERCENTAGE;
                    tmpFreshnessIndex = percentageValue / 100;
                } else {
                    throw new UnsupportedFreshnessSpecificationRuntimeException( EvaluationType.PERCENTAGE, toleratedFreshness.toString() );
                }
                break;

            case "INDEX":
                tmpFreshnessIndex = Double.valueOf( toleratedFreshness.toString() );
                if ( tmpFreshnessIndex < 0.0 && tmpFreshnessIndex > 100.0 ) {
                    throw new UnsupportedFreshnessSpecificationRuntimeException( EvaluationType.PERCENTAGE, toleratedFreshness.toString() );
                }
                tmpEvaluationType = EvaluationType.INDEX;
                break;

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( rawEvaluationType.toString().toUpperCase() );
        }

        setFreshnessSpecification( requestedTimestamp, tmpEvaluationType, tmpFreshnessIndex, timeDelay );
    }


    /**
     * Generates a time difference that has been specified via the Time Delay
     *
     * @param evalType executed evaluation Type only needed for logging
     * @return the calculated allowed time difference between objects
     */
    private long generateTimeDifference( EvaluationType evalType ) {

        long specifiedTimeDelta = Long.valueOf( toleratedFreshness.toString() );

        // Check validity
        if ( specifiedTimeDelta < 0 ) {
            throw new UnsupportedFreshnessSpecificationRuntimeException( evalType, toleratedFreshness.toString() );
        }

        // Initially set to 1000ms = 1s
        int timeMultiplier = 1000;

        switch ( unit.toString().toUpperCase() ) {
            case "SECOND":
                break;

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

        // Transform specifiedTimeDelta to the tolerated level of freshness
        return specifiedTimeDelta * timeMultiplier;
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