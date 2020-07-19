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

package org.polypheny.db.webui.models.requests;


import java.util.HashMap;
import org.polypheny.db.webui.models.HubMeta.TableMapping;


public class HubRequest {

    public int userId;
    public String user;
    public String password;
    public String action;
    public String secret;

    //change password
    public String oldPw;
    public String newPw1;
    public String newPw2;

    //editDataset
    public String name;
    public String description;
    public int pub;//public

    //uploadDataset
    public String dataset;
    public int datasetId;
    public boolean createPks;
    public boolean defaultValues;
    public String tableName;

    //downloadDataset
    public String schema;
    public String store;
    public String url;

    //export dataset
    //schema
    public HashMap<String, TableMapping> tables;
    public String hubLink;

    //delete user
    public int deleteUser;

    //create user
    public boolean admin;
    public String email;

}
