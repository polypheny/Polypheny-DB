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

package org.polypheny.db.processing.replication.freshness;


import java.sql.Timestamp;
import java.util.List;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeException;
import org.polypheny.db.processing.replication.freshness.exceptions.UnknownFreshnessTimeUnitException;
import org.polypheny.db.processing.replication.freshness.exceptions.UnsupportedFreshnessSpecificationException;


/**
 * This class is used to transform and extract relevant information out of a query which specified the optional acceptance to retrieve outdated data.
 */
public abstract class FreshnessManager {


    public abstract double transformToFreshnessIndex( CatalogTable table, String s, EvaluationType evaluationType );

    public abstract List<CatalogPartitionPlacement> getRelevantPartitionPlacements( CatalogTable table, double freshnessIndex );


    public static double transformTimestampToIndex( Timestamp timestamp ) {
        return 0;
    }


    public static class FreshnessInformation {


        public double freshnessIndex; // Value has to be between 0 and 1
        public EvaluationType evaluationType;
        public Timestamp toleratedTimestamp;


        public FreshnessInformation(
                double freshnessIndex,
                EvaluationType evaluationType,
                Timestamp toleratedTimestamp ) {
            this.freshnessIndex = freshnessIndex;
            this.evaluationType = evaluationType;
            this.toleratedTimestamp = toleratedTimestamp;
        }


        public static FreshnessInformation fromNodeLists(
                Node toleratedFreshness,
                Identifier rawEvaluationType,
                Identifier unit
        ) throws UnknownFreshnessEvaluationTypeException, UnknownFreshnessTimeUnitException, UnsupportedFreshnessSpecificationException {

            EvaluationType tmpEvaluationType;
            double tmpFreshnessIndex = -1.0;

            // Serves as the lower bound of accepted freshness
            Timestamp requestedTimestamp = null;
            boolean requireIndexTransformation = false;

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
                            throw new UnknownFreshnessTimeUnitException( unit.toString().toUpperCase() );
                    }

                    // Transform currentTimestamp to the tolerated level of freshness
                    timeDifference = specifiedTimeDelta * timeMultiplier;
                    invocationTimestamp = invocationTimestamp - timeDifference;
                    requestedTimestamp = new Timestamp( invocationTimestamp );

                    requireIndexTransformation = true;

                    break;

                case "TIMESTAMP":
                    tmpEvaluationType = EvaluationType.TIMESTAMP;
                    requestedTimestamp = Timestamp.valueOf( toleratedFreshness.toString() );
                    requireIndexTransformation = true;

                    break;

                case "PERCENTAGE":

                    double percentageValue = Double.valueOf( toleratedFreshness.toString() );
                    if ( percentageValue > 0.0 && percentageValue <= 100.0 ) {
                        tmpEvaluationType = EvaluationType.PERCENTAGE;
                        tmpFreshnessIndex = percentageValue / 100;
                    } else {
                        throw new UnsupportedFreshnessSpecificationException( EvaluationType.PERCENTAGE, toleratedFreshness.toString() );
                    }
                    break;

                default:
                    throw new UnknownFreshnessEvaluationTypeException( rawEvaluationType.toString().toUpperCase() );
            }

            // Required to transform the calculated freshness of DELAY and TIMESTAMP to an index
            if ( requireIndexTransformation && requestedTimestamp != null ) {
                tmpFreshnessIndex = FreshnessManager.transformTimestampToIndex( requestedTimestamp );
            }

            if ( tmpFreshnessIndex == -1 ) {
                throw new RuntimeException( "Failed to calculate a freshnessIndex" );
            }

            return new FreshnessInformation( tmpFreshnessIndex, tmpEvaluationType, requestedTimestamp );
        }


        /**
         * Needed to modify strings otherwise the SQL-input 'a' will be also added as the value "'a'" and not as "a" as intended
         * Essentially removes " ' " at the start and end of value
         *
         * @param node Node to be modified
         * @return String
         */
        public static String getValueOfSqlNode( Node node ) {

            if ( node instanceof Literal ) {
                return ((Literal) node).toValue();
            }
            return node.toString();
        }

    }


    public enum EvaluationType {
        TIMESTAMP,
        DELAY,
        PERCENTAGE
    }

}
