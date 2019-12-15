/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.Array;
import java.util.Arrays;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import ch.unibas.dmi.dbis.polyphenydb.QueryInterface;


@Slf4j
public class SqlQueryInterface extends QueryInterface {

    @Getter
    LowCostQueries lowCostQueries;


    /**
     * Interface to enable "services" to push transactions to the transactionManager
     * TODO: decide if way to go without server
     */
    public SqlQueryInterface( TransactionManager transactionManager, Authenticator authenticator ) {
        super( transactionManager, authenticator );
    }


    @Override
    public void run() {
        this.lowCostQueries = new LowCostQueries( this.transactionManager, "pa", "APP" );

        System.out.println( Arrays.toString( this.lowCostQueries.selectOneStat( "SELECT MIN(public.depts.deptno) FROM public.depts GROUP BY public.depts.deptno ORDER BY MIN(public.depts.deptno) " )
                .getData() ) );

        log.info( "SQL query interface started." );
    }

}
