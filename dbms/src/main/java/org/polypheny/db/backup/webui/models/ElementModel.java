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

package org.polypheny.db.backup.webui.models;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.polypheny.db.backup.BackupEntityWrapper;
import org.polypheny.db.backup.BackupInformationObject;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.Pair;

@Value
public class ElementModel {

    @JsonProperty
    public String initialName;

    @JsonProperty
    public String adjustedName;

    @JsonProperty
    public TypeName type;

    @JsonProperty
    public DataModel model;

    @JsonProperty
    public List<ElementModel> children;

    @JsonProperty
    @JsonDeserialize(using = JsonForceNullDeserializer.class)
    public List<Pair<String, String>> dependencies; // other element name / reason for dependency

    @JsonProperty
    public BackupType backupType;

    @JsonProperty
    public String additionalInformation;


    @JsonCreator
    public ElementModel(
            @JsonProperty("initialName") @Nonnull String initialName,
            @JsonProperty("adjustedName") @Nullable String adjustedName,
            @JsonProperty("type") @Nonnull TypeName type,
            @JsonProperty("model") @Nullable DataModel model,
            @JsonProperty("children") @Nonnull List<ElementModel> children,
            @JsonProperty("dependencies") @Nonnull List<Pair<String, String>> dependencies,
            @JsonProperty("backupType") @Nullable BackupType backupType,
            @JsonProperty("additionalInformation") @Nullable String additionalInformation ) {
        this.initialName = initialName;
        this.adjustedName = adjustedName;
        this.type = type;
        this.model = model;
        this.children = children;
        this.dependencies = dependencies;
        this.backupType = backupType;
        this.additionalInformation = additionalInformation;
    }


    public enum TypeName {
        NAMESPACE,
        TABLE,
        COLLECTION,
        GRAPH,
        COLUMN,
        VIEW,
        MATERIALIZED_VIEW,
        FIELD
    }


    // todo ff: write converter for all webui object types
    public static List<ElementModel> fromBackupObject( BackupInformationObject backupObject ) {
        List<ElementModel> elements = new ArrayList<>();
        backupObject.getWrappedNamespaces().values().stream().map( namespaces ->
                        new ElementModel(
                                namespaces.getEntityObject().name,
                                null,
                                TypeName.NAMESPACE,
                                DataModel.RELATIONAL,
                                new ArrayList<>(
                                        backupObject.getWrappedTables()
                                                .values()
                                                .stream()
                                                .flatMap(
                                                        tables -> tables.stream()
                                                                .filter( t -> t.getEntityObject().namespaceId == namespaces.getEntityObject().id )
                                                                .map( table -> fromBackupTable( table, backupObject ) ) )
                                                .collect( Collectors.toList() )
                                ),
                                new ArrayList<>(),
                                BackupType.NONE,
                                null ) )
                .forEach( elements::add );
        // todo ff: write converter for all webui object types
        return elements;
    }


    // todo ff add dependencies logic
    private static ElementModel fromBackupTable( BackupEntityWrapper<LogicalEntity> table, BackupInformationObject backupObject ) {
        return new ElementModel(
                table.getEntityObject().name,
                null,
                TypeName.TABLE,
                DataModel.RELATIONAL,
                backupObject.getColumns().values().stream().flatMap( columns -> columns.stream().filter( column -> column.tableId == table.getEntityObject().id ) )
                        .map( column ->
                                new ElementModel(
                                        column.name,
                                        null,
                                        TypeName.COLUMN,
                                        DataModel.RELATIONAL,
                                        new ArrayList<>(),
                                        new ArrayList<>(), // todo ff add dependencies logic
                                        BackupType.NONE,
                                        null ) ).collect( Collectors.toList() ),
                new ArrayList<>(),
                BackupType.NONE,
                null );
    }


    // example method, delete later
    public static List<ElementModel> getDummyRels() {
        return List.of(
                new ElementModel(
                        "testNamespace",
                        null,
                        TypeName.NAMESPACE,
                        DataModel.RELATIONAL,
                        List.of( new ElementModel(
                                "testTable2",
                                null,
                                TypeName.TABLE,
                                DataModel.RELATIONAL,
                                List.of(
                                        new ElementModel(
                                                "testColumn",
                                                null,
                                                TypeName.COLUMN,
                                                DataModel.RELATIONAL,
                                                List.of(),
                                                List.of(),
                                                BackupType.NONE,
                                                null
                                        ),
                                        new ElementModel(
                                                "testColumn2",
                                                null,
                                                TypeName.COLUMN,
                                                DataModel.RELATIONAL,
                                                List.of(),
                                                List.of(),
                                                BackupType.NONE,
                                                null
                                        )
                                ),
                                List.of(),
                                BackupType.NONE,
                                "This is some additional information."
                        ) ),
                        List.of(),
                        BackupType.NONE,
                        null ),
                new ElementModel(
                        "testNamespace2",
                        null,
                        TypeName.NAMESPACE,
                        DataModel.RELATIONAL,
                        List.of( new ElementModel(
                                "testTable2",
                                null,
                                TypeName.TABLE,
                                DataModel.RELATIONAL,
                                List.of(
                                        new ElementModel(
                                                "testColumn",
                                                null,
                                                TypeName.COLUMN,
                                                DataModel.RELATIONAL,
                                                List.of(),
                                                List.of(),
                                                BackupType.NONE,
                                                null
                                        ),
                                        new ElementModel(
                                                "testColumn2",
                                                null,
                                                TypeName.COLUMN,
                                                DataModel.RELATIONAL,
                                                List.of(),
                                                List.of( Pair.of( "testTable2", "testColumn" ) ),
                                                BackupType.NONE,
                                                null
                                        )
                                ),
                                List.of(),
                                BackupType.NONE,
                                "This is some additional information."
                        ) ),
                        List.of(),
                        BackupType.NONE,
                        null
                )

        );
    }


    // example method, delete later
    public static ElementModel getDummyDoc() {
        return new ElementModel(
                "testNamespaceDoc",
                null,
                TypeName.NAMESPACE,
                DataModel.DOCUMENT,
                List.of( new ElementModel(
                        "testCollection",
                        null,
                        TypeName.COLLECTION,
                        DataModel.DOCUMENT,
                        List.of(),
                        List.of(),
                        BackupType.NONE,
                        "This is some additional information."
                ) ),
                List.of(),
                BackupType.NONE,
                null
        );
    }


    // example method, delete later
    public static ElementModel getDummyGraph() {
        return new ElementModel(
                "testNamespaceGraph",
                null,
                TypeName.NAMESPACE,
                DataModel.GRAPH,
                List.of(),
                List.of(),
                BackupType.NONE,
                null
        );
    }


    private static class JsonForceNullDeserializer extends JsonDeserializer<Object> {

        @Override
        public Object deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException, JacksonException {
            return null;
        }

    }

}
