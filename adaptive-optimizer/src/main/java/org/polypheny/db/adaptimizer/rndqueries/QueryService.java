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

package org.polypheny.db.adaptimizer.rndqueries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.transaction.Statement;

@Slf4j
@NoArgsConstructor
public class QueryService {

    @SuppressWarnings("UnusedReturnValue")
    public List<List<Object>> executeTree( Statement statement, AlgNode algNode ) {

        AlgRoot logicalRoot = AlgRoot.of( algNode, Kind.SELECT );

        PolyResult polyResult = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

        Iterator<Object> iterator = PolyResult.enumerable( polyResult.getBindable() , statement.getDataContext() ).iterator();

        StopWatch stopWatch;
        List<List<Object>> result;

        stopWatch = new StopWatch();
        stopWatch.start();
        result = MetaImpl.collect( polyResult.getCursorFactory(), iterator, new ArrayList<>() );
        stopWatch.stop();
        polyResult.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );

        return result;
    }

}
