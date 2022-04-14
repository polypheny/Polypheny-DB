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

package org.polypheny.db.replication.freshness;


import java.sql.Timestamp;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.replication.freshness.FreshnessManager.EvaluationType;
import org.polypheny.db.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeRuntimeException;
import org.polypheny.db.replication.freshness.exceptions.UnknownFreshnessTimeUnitRuntimeException;
import org.polypheny.db.replication.freshness.exceptions.UnsupportedFreshnessSpecificationRuntimeException;
import org.polypheny.db.replication.freshness.properties.FreshnessSpecification;


public abstract class FreshnessExtractor {

    protected FreshnessSpecification freshnessSpecification;

    protected Node toleratedFreshness;
    protected Identifier rawEvaluationType;
    protected Identifier unit;

    //TODO @HENNLO Add extractor for other querylanguages


    protected FreshnessExtractor(
            Node toleratedFreshness,
            Identifier rawEvaluationType,
            Identifier unit ) {
        this.toleratedFreshness = toleratedFreshness;
        this.rawEvaluationType = rawEvaluationType;
        this.unit = unit;
    }


    /**
     * Extracts FreshnessSpecifications from any query, if it was specified.
     * Is used to enrich a transaction to have access to these specifications later on.
     *
     * @return General FreshnessSpecification unrelated to a specific query language.
     */
    public abstract FreshnessSpecification extractFreshnessSpecification()
            throws UnknownFreshnessTimeUnitRuntimeException,
            UnknownFreshnessEvaluationTypeRuntimeException,
            UnsupportedFreshnessSpecificationRuntimeException;


    public FreshnessSpecification getFreshnessSpecification() {

        if ( freshnessSpecification == null ) {
            throw new RuntimeException( "FreshnessSpecification has not been extracted yet." );
        }

        return freshnessSpecification;
    }


    protected void setFreshnessSpecification( Timestamp toleratedTimestamp, EvaluationType evaluationType, double freshnessIndex, long timeDelay ) {
        this.freshnessSpecification = new FreshnessSpecification( toleratedTimestamp, evaluationType, freshnessIndex, timeDelay );
    }
}
