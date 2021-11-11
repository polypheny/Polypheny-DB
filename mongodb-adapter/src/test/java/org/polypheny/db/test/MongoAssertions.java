/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.Ordering;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.hamcrest.CoreMatchers;
import org.polypheny.db.util.Util;


/**
 * Util class which needs to be in the same package as {@link PolyphenyDbAssert} due to package-private visibility.
 */
public class MongoAssertions {

    private MongoAssertions() {
    }


    /**
     * Similar to {@code PolyphenyDbAssert#checkResultUnordered}, but filters strings before comparing them.
     *
     * @param lines Expected expressions
     * @return validation function
     */
    public static Consumer<ResultSet> checkResultUnordered( final String... lines ) {
        return resultSet -> {
            try {
                final List<String> expectedList = Ordering.natural().immutableSortedCopy( Arrays.asList( lines ) );

                final List<String> actualList = new ArrayList<>();
                PolyphenyDbAssert.toStringList( resultSet, actualList );
                for ( int i = 0; i < actualList.size(); i++ ) {
                    String s = actualList.get( i );
                    actualList.set( i, s.replaceAll( "\\.0;", ";" ).replaceAll( "\\.0$", "" ) );
                }
                Collections.sort( actualList );

                assertThat( Ordering.natural().immutableSortedCopy( actualList ), CoreMatchers.equalTo( expectedList ) );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        };
    }


    /**
     * Whether to run Mongo integration tests. Enabled by default, however test is only included if "it" profile is activated ({@code -Pit}). To disable, specify {@code -Dpolyphenydb.test.mongodb=false} on the Java command line.
     *
     * @return Whether current tests should use an external mongo instance
     */
    public static boolean useMongo() {
        return Util.getBooleanProperty( "polyphenydb.integrationTest" ) && Util.getBooleanProperty( "polyphenydb.test.mongodb", true );
    }


    /**
     * Checks wherever tests should use Fongo instead of Mongo. Opposite of {@link #useMongo()}.
     *
     * @return Whether current tests should use embedded <a href="https://github.com/fakemongo/fongo">Fongo</a> instance
     */
    public static boolean useFongo() {
        return !useMongo();
    }


    /**
     * Used to skip tests if current instance is not mongo. Some functionalities are not available in fongo.
     *
     * @see <a href="https://github.com/fakemongo/fongo/issues/152">Aggregation with $cond (172)</a>
     */
    public static void assumeRealMongoInstance() {
        assumeTrue( "Expect mongo instance", useMongo() );
    }

}

