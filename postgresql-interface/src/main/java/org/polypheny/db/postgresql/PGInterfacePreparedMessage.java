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

package org.polypheny.db.postgresql;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class PGInterfacePreparedMessage {
    private String name;
    private List<String> datatype;
    private ChannelHandlerContext ctx;
    private String query;
    private String prepareString;
    private String executeString;
    private List<Object> values;
    private static final String  executeDelimiter = ", ";


    public PGInterfacePreparedMessage(String query, String executeString, ChannelHandlerContext ctx) {
        this.query = query;
        this.executeString = executeString;
        this.ctx = ctx;
    }

    public PGInterfacePreparedMessage(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setExecuteString(String executeString) {
        this.executeString = executeString;
    }

    public static String getExecuteDelimiter() {return executeDelimiter;}

    public List<String> extractValues() {
        //us execute string - seperator: ', '
        //bool ersetze...
        //cut string at ( and ) (remove chlammere) --> denn mach eif. split, ond problem solved...
        String onlyExecuteValues = executeString.split("\\(|\\)")[1];
        return List.of(onlyExecuteValues.split(getExecuteDelimiter()));
    }
}
