/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.exploreByExample.models;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.webui.models.results.RelationalResult;


@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
@Value
public class RelationalExploreResult extends RelationalResult {

    /**
     * Explore-by-Example, information about classification, because classification is only possible if a table holds at least 10 entries
     */
    public String classificationInfo;

    /**
     * Explore-by-Example Explorer Id for
     */
    public int explorerId;

    /**
     * Pagination for Explore-by-Example, Information if it includes classified data
     */
    public boolean includesClassificationInfo;

    /**
     * Pagination for Explore-by-Example, to display the classified Data with the addition of true/false
     */
    public String[][] classifiedData;

    /**
     * Explore-by-Example, Information if the weka classifier is translated to sql or not
     */
    public boolean isConvertedToSql;


    public static RelationalExploreResultBuilder<?, ?> from( RelationalResult rel ) {
        RelationalExploreResultBuilder<?, ?> builder = builder();
        builder.header( rel.header );
        builder.currentPage( rel.currentPage );
        builder.highestPage( rel.highestPage );
        builder.hasMoreRows( rel.hasMore );
        builder.table( rel.table );
        builder.tables( rel.tables );
        builder.request( rel.request );
        builder.affectedRows( rel.affectedRows );
        builder.exception( rel.exception );
        builder.generatedQuery( rel.query );
        builder.language( rel.language );
        builder.type( rel.type );

        return builder;
    }

}
