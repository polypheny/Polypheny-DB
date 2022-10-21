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

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

public class PGInterfacePreparedMessage {
    private String name;
    private ChannelHandlerContext ctx;
    private String query;
    private String wholePrepareString;
    private String executeString;
    private List<String> dataTypes;
    private static final String  executeDelimiter = ", ";


    public PGInterfacePreparedMessage(String name, String wholePrepareString, ChannelHandlerContext ctx) {
        this.name = name;
        this.wholePrepareString = wholePrepareString;
        this.ctx = ctx;
    }

    public PGInterfacePreparedMessage(String name, ChannelHandlerContext ctx) {
        this.name = name;
        this.ctx = ctx;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setExecuteString(String executeString) {
        this.executeString = executeString;
    }

    public void setWholePrepareString(String wholePrepareString) {
        this.wholePrepareString = wholePrepareString;
    }

    private void setDataTypes(List<String> dataTypes) { this.dataTypes = dataTypes; }

    public static String getExecuteDelimiter() {return executeDelimiter;}

    public String getQuery() { return query;}

    public List<String> getDataTypes() { return dataTypes;}

    public void extractAndSetValues() {
        //us execute string - seperator: ', '
        //bool ersetze...
        //cut string at ( and ) (remove chlammere) --> denn mach eif. split, ond problem solved...
        String onlyExecuteValues = executeString.split("\\(|\\)")[1];
        List<String> valueList = Arrays.asList(onlyExecuteValues.split(getExecuteDelimiter()));
        setDataTypes(valueList);
    }

    public void extractAndSetTypes() {
        String types = wholePrepareString.split("\\(|\\)")[1];
        List<String> typeList = Arrays.asList(types.split(getExecuteDelimiter()));

        //replace all bool with boolean to match polypheny dt
        if (typeList.contains("bool") || typeList.contains("BOOL")) {
            ListIterator<String> iterator = typeList.listIterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                if (next.equals("bool")) {
                    typeList.set(iterator.nextIndex()-1, "BOOLEAN");
                }
            }
        }
        setDataTypes(typeList);
    }

    public void changeParameterSymbol() {

        String[] parts = wholePrepareString.split("\\$");
        String newPrepareString = new String();

        for (int i = 1; i<parts.length; i++) {
            newPrepareString = newPrepareString + "?" + parts[i].substring(1);
        }

        setWholePrepareString(parts[0] + newPrepareString);

    }

    public void extractAndSetQuery() {
        String query = wholePrepareString.split("AS ")[1];
        setQuery(query);
    }

    public void prepareQuery() {
        extractAndSetTypes();
        changeParameterSymbol();
        extractAndSetTypes();
        extractAndSetQuery();
    }


}
