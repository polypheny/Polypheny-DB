/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.test;

import java.lang.reflect.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.polypheny.db.TestHelper;
import org.polypheny.db.schemas.HrSnapshot;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;

public class PigTestTemplate {

    protected static TestHelper helper;
    protected static Transaction transaction;
    protected static PigAlgBuilder builder;


    @BeforeAll
    public static void init() throws NoSuchFieldException {
        helper = TestHelper.getInstance();
        transaction = helper.getTransaction();
        builder = PigAlgBuilder.create( transaction.createStatement() );

        Field f = AlgBuilder.class.getDeclaredField( "snapshot" );
        f.setAccessible( true );
        try {
            f.set( builder, new HrSnapshot() );
        } catch ( IllegalAccessException e ) {
            throw new RuntimeException( e );
        }

    }


    @AfterAll
    public static void cleanUp() {
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}
