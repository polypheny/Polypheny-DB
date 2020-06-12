/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.exploreByExample;


import java.util.Collections;


/**
 * This is a simple mock sql processor, which enables limited testing for the Explore-by-Example functionality
 */
public class MockProcessor extends ExploreQueryProcessor {

    public MockProcessor() {
        super( null, null );

    }


    @Override
    public ExploreQueryResult executeSQL( String query ) {
        ExploreQueryResult res = new ExploreQueryResult();
        if ( query.endsWith( "LIMIT 60" ) ) {
            res.count = 30;
            return res;
        }
        res = new ExploreQueryResult( new String[][]{}, 0, Collections.singletonList( "INTEGER" ), Collections.singletonList( "public.depts.deptno" ) );
        return res;
    }
}
