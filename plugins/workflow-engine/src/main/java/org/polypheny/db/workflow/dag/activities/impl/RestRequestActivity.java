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

package org.polypheny.db.workflow.dag.activities.impl;

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "restRequest", displayName = "REST Request", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, isOptional = true, description = "An optional collection where the first document is used as request body if enabled.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The response to the request, or an empty collection if no response was received.") },
        shortDescription = "Performs an HTTP request with optional headers and body."
)
@StringSetting(key = "target", displayName = "Target URL", nonBlank = true, maxLength = 10 * 1024, pos = 0)
@EnumSetting(key = "method", displayName = "Request Method", pos = 1,
        options = { "GET", "POST", "PUT", "PATCH", "DELETE" }, defaultValue = "GET",
        style = EnumStyle.RADIO_BUTTON)

@BoolSetting(key = "bodyFromInput", displayName = "Set Body from Input", pos = 10,
        defaultValue = false,
        shortDescription = "Set the request body to the first document in the input collection. If no document exists, the body remains empty.")
@StringSetting(key = "body", displayName = "Request Body", textEditor = true, language = "json", pos = 11,
        maxLength = 10 * 1024, subPointer = "bodyFromInput", subValues = { "false" }, defaultValue = "{}",
        shortDescription = "The request body to send. If empty, the body also remains empty.")

@StringSetting(key = "pointer", displayName = "Response Field", pos = 20,
        maxLength = 1024, group = ADVANCED_GROUP,
        shortDescription = "The target (sub)field of the response, given the response is valid json. If the target is an array of objects, its entries are mapped to individual documents.")
@StringSetting(key = "headers", displayName = "Request Headers", textEditor = true, language = "json", pos = 21,
        maxLength = 10 * 1024, defaultValue = "{}", group = ADVANCED_GROUP,
        shortDescription = "Optional request headers specified as a JSON-object with string values.")
@StringSetting(key = "queryParams", displayName = "Query Parameters", textEditor = true, language = "json", pos = 22,
        maxLength = 10 * 1024, defaultValue = "{}", group = ADVANCED_GROUP,
        shortDescription = "Optional query parameters specified as a JSON-object with string values. Alternatively, the query parameters can also directly be specified in the target URL.")
@IntSetting(key = "timeout", displayName = "Timeout Duration (Seconds)", pos = 23,
        group = ADVANCED_GROUP,
        min = 1, max = 300, defaultValue = 30)
@BoolSetting(key = "fail", displayName = "Fail on Unsuccessful Response Code", pos = 24,
        defaultValue = true, group = ADVANCED_GROUP,
        shortDescription = "If true, the activity fails if the response code is not between 200 and 300.")

@SuppressWarnings("unused")
public class RestRequestActivity implements Activity {

    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "target" ) && !isValidURL( settings.getString( "target" ) ) ) {
            throw new InvalidSettingException( "Invalid Target URL: " + settings.getString( "target" ), "target" );
        }
        if ( settings.keysPresent( "queryParams" ) ) {
            String queryParams = settings.getString( "queryParams" );
            if ( !queryParams.isBlank() ) {
                try {
                    jsonToMap( queryParams );
                } catch ( Exception e ) {
                    throw new InvalidSettingException( "Invalid Query Parameters: " + e.getMessage(), "queryParams" );
                }
            }
        }
        if ( settings.keysPresent( "headers" ) ) {
            String headers = settings.getString( "headers" );
            try {
                jsonToMap( headers );
            } catch ( Exception e ) {
                throw new InvalidSettingException( "Invalid Headers: " + e.getMessage(), "queryParams" );
            }
        }
        if ( settings.keysPresent( "bodyFromInput" ) ) {
            if ( settings.getBool( "bodyFromInput" ) ) {
                if ( inTypes.get( 0 ).isMissing() ) {
                    throw new InvalidInputException( "Input is required if the request body is set from the input", 0 );
                }
            } else if ( settings.keysPresent( "bodyType", "body" ) && settings.getString( "bodyType" ).equals( "application/json" ) ) {
                String body = settings.getString( "body" );
                try {
                    mapper.readTree( body );
                } catch ( Exception e ) {
                    throw new InvalidSettingException( "Invalid JSON for body: " + e.getMessage(), "body" );
                }
            }

        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        String target = settings.getString( "target" ); // might already have queryParams
        String method = settings.getString( "method" ); // GET, POST ...
        Map<String, String> queryParams = jsonToMap( settings.getString( "queryParams" ) );
        Map<String, String> headers = jsonToMap( settings.getString( "headers" ) );
        int timeout = settings.getInt( "timeout" );
        String pointer = settings.getString( "pointer" );

        String body = settings.getString( "body" );
        if ( settings.getBool( "bodyFromInput" ) ) {
            DocReader reader = (DocReader) inputs.get( 0 );
            body = "";
            for ( PolyDocument doc : reader.getDocIterable() ) {
                doc.remove( docId );
                body = Objects.requireNonNullElse( doc.toJson(), "" );
                break;
            }
        }
        ctx.logInfo( "Body: " + body );

        // Append query parameters
        if ( !queryParams.isEmpty() ) {
            String queryString = queryParams.entrySet()
                    .stream()
                    .map( entry -> URLEncoder.encode( entry.getKey(), StandardCharsets.UTF_8 ) + "=" +
                            URLEncoder.encode( entry.getValue(), StandardCharsets.UTF_8 ) )
                    .collect( Collectors.joining( "&" ) );
            target += (target.contains( "?" ) ? "&" : "?") + queryString;
        }
        ctx.logInfo( "Target URL with queryParams: " + target );

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout( Duration.ofSeconds( timeout ) )
                .build();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri( URI.create( target ) )
                .timeout( Duration.ofSeconds( timeout ) );
        headers.forEach( requestBuilder::header );
        // we do NOT log headers to avoid leaking secrets from env variables

        switch ( method ) {
            case "GET" -> requestBuilder.GET();
            case "POST" -> requestBuilder.POST( HttpRequest.BodyPublishers.ofString( body ) );
            case "PUT" -> requestBuilder.PUT( HttpRequest.BodyPublishers.ofString( body ) );
            case "PATCH" -> requestBuilder.method( "PATCH", HttpRequest.BodyPublishers.ofString( body ) );
            case "DELETE" -> requestBuilder.DELETE();
            default -> throw new IllegalArgumentException( "Unsupported HTTP method: " + method );
        }

        HttpResponse<String> response = client.send( requestBuilder.build(), HttpResponse.BodyHandlers.ofString() );
        String responseBody = response.body();
        boolean isSuccess = response.statusCode() >= 200 && response.statusCode() < 300;
        if ( settings.getBool( "fail" ) && !isSuccess ) {
            ctx.throwException( "HTTP request failed with status code: " + response.statusCode() + " and body: " + responseBody );
        }

        DocWriter output = ctx.createDocWriter( 0 );
        if ( responseBody == null ) {
            ctx.logInfo( "Received Null Response with status code: " + response.statusCode() );
            return;
        }
        ctx.logInfo( "Response with status code: " + response.statusCode() );

        try {
            jsonToDocuments( responseBody, pointer ).forEach( output::write );
        } catch ( JsonProcessingException e ) { // response is not valid json
            output.write( new PolyDocument( Map.of( PolyString.of( "response_text" ), PolyString.of( responseBody ) ) ) );
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return 1;
    }


    private static boolean isValidURL( String url ) {
        try {
            URI uri = new URI( url );
            return uri.getScheme() != null && (uri.getScheme().equals( "http" ) || uri.getScheme().equals( "https" ));
        } catch ( Exception e ) {
            return false;
        }
    }


    private static Map<String, String> jsonToMap( String json ) throws Exception {
        Map<String, String> map = new HashMap<>();
        if ( json.isBlank() ) {
            return map;
        }
        ObjectNode node = mapper.readValue( json, ObjectNode.class );
        for ( Entry<String, JsonNode> entry : node.properties() ) {
            if ( entry.getValue().isTextual() ) {
                map.put( entry.getKey(), entry.getValue().textValue() );
            } else {
                map.put( entry.getKey(), entry.getValue().toString() );
            }
        }
        return map;
    }


    private static List<PolyDocument> jsonToDocuments( String json, String pointer ) throws JsonProcessingException, IllegalArgumentException {
        JsonPointer jsonPointer = ActivityUtils.dotToJsonPointer( pointer );
        JsonNode root = mapper.readTree( json ).requiredAt( jsonPointer ); // throws illegal argument exception if invalid
        System.out.println( "Root: " + root );
        List<PolyDocument> documents = new ArrayList<>();
        if ( root.isArray() ) {
            for ( JsonNode node : root ) {
                if ( node.isObject() ) {
                    System.out.println( "Node is Object: " + node );
                    System.out.println( "Polyvalue from it: " + PolyValue.fromJson( node.toString() ) + ", " + PolyValue.fromJson( node.toString() ).type );
                    documents.add( PolyValue.fromJson( node.toString() ).asDocument() );
                } else {
                    // array does not only contain objects -> map array to single document
                    documents = List.of( new PolyDocument( Map.of( PolyString.of( "response" ), PolyValue.fromJson( root.toString() ) ) ) );
                    break;
                }
            }
        } else if ( root.isObject() ) {
            documents.add( PolyValue.fromJson( root.toString() ).asDocument() );
        } else {
            System.out.println( "Root is not an object: " + root + ", " + root.getNodeType() );
            documents.add( new PolyDocument( Map.of( PolyString.of( "response" ), PolyValue.fromJson( root.toString() ) ) ) );
        }

        documents.forEach( ActivityUtils::addDocId );
        return documents;
    }

}
