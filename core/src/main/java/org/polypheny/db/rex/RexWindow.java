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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;


/**
 * Specification of the window of rows over which a {@link RexOver} windowed aggregate is evaluated.
 *
 * Treat it as immutable!
 */
public class RexWindow {

    public final ImmutableList<RexNode> partitionKeys;
    public final ImmutableList<RexFieldCollation> orderKeys;
    private final RexWindowBound lowerBound;
    private final RexWindowBound upperBound;
    private final boolean isRows;
    private final String digest;


    /**
     * Creates a window.
     *
     * If you need to create a window from outside this package, use {@link RexBuilder#makeOver}.
     */
    RexWindow( List<RexNode> partitionKeys, List<RexFieldCollation> orderKeys, RexWindowBound lowerBound, RexWindowBound upperBound, boolean isRows ) {
        assert partitionKeys != null;
        assert orderKeys != null;
        this.partitionKeys = ImmutableList.copyOf( partitionKeys );
        this.orderKeys = ImmutableList.copyOf( orderKeys );
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.isRows = isRows;
        this.digest = computeDigest();
    }


    public String toString() {
        return digest;
    }


    public String toString( RexVisitor<String> visitor ) {
        if ( visitor == null ) {
            return toString();
        }
        return computeDigest( visitor );
    }


    public int hashCode() {
        return digest.hashCode();
    }


    public boolean equals( Object that ) {
        if ( that instanceof RexWindow ) {
            RexWindow window = (RexWindow) that;
            return digest.equals( window.digest );
        }
        return false;
    }


    private String computeDigest() {
        return computeDigest( null );
    }


    private String computeDigest( RexVisitor<String> visitor ) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        int clauseCount = 0;
        if ( partitionKeys.size() > 0 ) {
            if ( clauseCount++ > 0 ) {
                pw.print( ' ' );
            }
            pw.print( "PARTITION BY " );
            for ( int i = 0; i < partitionKeys.size(); i++ ) {
                if ( i > 0 ) {
                    pw.print( ", " );
                }
                RexNode partitionKey = partitionKeys.get( i );
                pw.print( visitor == null ? partitionKey.toString() : partitionKey.accept( visitor ) );
            }
        }
        if ( orderKeys.size() > 0 ) {
            if ( clauseCount++ > 0 ) {
                pw.print( ' ' );
            }
            pw.print( "ORDER BY " );
            for ( int i = 0; i < orderKeys.size(); i++ ) {
                if ( i > 0 ) {
                    pw.print( ", " );
                }
                RexFieldCollation orderKey = orderKeys.get( i );
                pw.print( orderKey.toString( visitor ) );
            }
        }
        if ( lowerBound == null ) {
            // No ROWS or RANGE clause
        } else if ( upperBound == null ) {
            if ( clauseCount++ > 0 ) {
                pw.print( ' ' );
            }
            if ( isRows ) {
                pw.print( "ROWS " );
            } else {
                pw.print( "RANGE " );
            }
            pw.print( lowerBound.toString( visitor ) );
        } else {
            if ( clauseCount++ > 0 ) {
                pw.print( ' ' );
            }
            if ( isRows ) {
                pw.print( "ROWS BETWEEN " );
            } else {
                pw.print( "RANGE BETWEEN " );
            }
            pw.print( lowerBound.toString( visitor ) );
            pw.print( " AND " );
            pw.print( upperBound.toString( visitor ) );
        }
        return sw.toString();
    }


    public RexWindowBound getLowerBound() {
        return lowerBound;
    }


    public RexWindowBound getUpperBound() {
        return upperBound;
    }


    public boolean isRows() {
        return isRows;
    }

}

