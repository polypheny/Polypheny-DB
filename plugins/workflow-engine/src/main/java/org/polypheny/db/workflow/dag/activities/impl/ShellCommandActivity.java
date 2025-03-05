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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Set;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "shellCommand", displayName = "Execute Shell Command", categories = { ActivityCategory.DOCUMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "Possibly outputs stdout and stderr of the executed command.") },
        shortDescription = "Executes a command in the specified shell and possibly redirects the outputs into documents.")
@StringSetting(key = "command", displayName = "Command", pos = 0,
        textEditor = true, language = "sh",
        maxLength = 10 * 1024, nonBlank = true, defaultValue = "echo hello",
        shortDescription = "The command to execute")
@EnumSetting(key = "shell", displayName = "Shell", options = { "bash", "sh", "cmd", "PowerShell" }, defaultValue = "bash", pos = 1,
        shortDescription = "The shell to use for executing the command. The shell must be compatible with the operating system."
                + " WARNING: This activity is considered to be dangerous, as arbitrary commands can be executed! Use it at your own risk.")
@StringSetting(key = "directory", displayName = "Working Directory", pos = 2,
        maxLength = 1024, defaultValue = "",
        shortDescription = "The working directory. If empty, the default working directory is used.")

@BoolSetting(key = "stdout", displayName = "Output stdout as Document", pos = 3,
        group = ADVANCED_GROUP, defaultValue = true)
@BoolSetting(key = "stderr", displayName = "Output stderr as Document", pos = 4,
        group = ADVANCED_GROUP, defaultValue = true)
@BoolSetting(key = "split", displayName = "One Line Per Document", pos = 5,
        group = ADVANCED_GROUP, defaultValue = false,
        shortDescription = "If true, each individual line of stdout or stderr is output into a new document.")
@BoolSetting(key = "fail", displayName = "Fail on Non-Zero Exit Code", pos = 6,
        group = ADVANCED_GROUP, defaultValue = true,
        shortDescription = "If true, the activity fails if the command results in a non-zero exit code.")

@SuppressWarnings("unused")
public class ShellCommandActivity implements Activity {

    private static final int POLL_INTERVAL_MS = 100; // how often to check if command execution finished or should be interrupted
    private final PolyString stdoutField = PolyString.of( "stdout" );
    private final PolyString stderrField = PolyString.of( "stderr" );
    private final PolyString lineField = PolyString.of( "line" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        checkAllowed();
        if ( settings.keysPresent( "shell" ) ) {
            Shell shell = Shell.valueOf( settings.getString( "shell" ).toUpperCase() );
            checkOsCompatibility( shell );
        }
        return DocType.of( Set.of( stdoutField.value, stderrField.value ) ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        checkAllowed();
        String command = settings.getString( "command" );
        Shell shell = Shell.valueOf( settings.getString( "shell" ).toUpperCase() );
        checkOsCompatibility( shell );
        boolean split = settings.getBool( "split" );
        boolean stdout = settings.getBool( "stdout" );
        boolean stderr = settings.getBool( "stderr" );
        String dirStr = settings.getString( "directory" );

        ProcessBuilder builder = new ProcessBuilder();
        switch ( shell ) {
            case BASH -> builder.command( "bash", "-c", command );
            case SH -> builder.command( "sh", "-c", command );
            case CMD -> builder.command( "cmd.exe", "/c", command );
            case POWERSHELL -> builder.command( "powershell.exe", "-Command", command );
        }
        if ( !dirStr.isBlank() ) {
            builder.directory( new File( dirStr ) );
        }
        if ( !stdout ) {
            builder.redirectOutput( Redirect.DISCARD );
        }
        if ( !stderr ) {
            builder.redirectError( Redirect.DISCARD );
        }

        // A more robust method would be to start separate threads for reading stdout / stderr
        Process process = builder.start();
        ctx.logInfo( "Started process" );
        DocWriter writer = ctx.createDocWriter( 0 );
        try {
            if ( stdout ) {
                ctx.logInfo( "Reading stdout..." );
                writeOutput( process, true, split, writer, ctx );
            }
            if ( stderr ) {
                ctx.logInfo( "Reading stderr..." );
                writeOutput( process, false, split, writer, ctx );
            }

            while ( process.isAlive() ) {
                Thread.sleep( POLL_INTERVAL_MS );
                ctx.checkInterrupted();
            }
        } catch ( ExecutorException e ) { // activity execution was interrupted
            ctx.logWarning( "Activity was interrupted. Destroying process..." );
            process.destroy();
            throw e;
        }
        int exitCode = process.exitValue();
        if ( exitCode != 0 && settings.getBool( "fail" ) ) {
            throw new GenericRuntimeException( "Shell command failed with exit code " + exitCode );
        }

    }


    private void checkAllowed() {
        if ( !RuntimeConfig.WORKFLOWS_ENABLE_UNSAFE.getBoolean() ) {
            throw new GenericRuntimeException( "Execution of unsafe activities is disabled in the RuntimeConfig." );
        }
    }


    private void checkOsCompatibility( Shell shell ) throws InvalidSettingException {
        if ( System.getProperty( "os.name" ).toLowerCase().contains( "win" ) != shell.forWindows ) {
            throw new InvalidSettingException( "Selected shell is not compatible with your operating system", "shell" );
        }
    }


    private void writeOutput( Process process, boolean isStdout, boolean split, DocWriter writer, ExecutionContext ctx ) throws Exception {
        PolyString fieldStr = isStdout ? stdoutField : stderrField;
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) ) {
            if ( !waitForReady( reader, process, ctx ) ) {
                return;
            }
            if ( split ) {
                int count = 0;
                String line;
                while ( (line = reader.readLine()) != null ) {
                    ctx.logInfo( "Reading " + fieldStr.value + ": " + line );
                    count++;
                    ctx.checkInterrupted();
                    PolyDocument doc = new PolyDocument();
                    doc.put( fieldStr, PolyString.of( line ) );
                    doc.put( lineField, PolyInteger.of( count ) );
                    writer.write( doc );
                    if ( !waitForReady( reader, process, ctx ) ) {
                        return;
                    }
                }
            } else {
                StringBuilder sb = new StringBuilder();
                String line;

                while ( (line = reader.readLine()) != null ) {
                    if ( !sb.isEmpty() ) {
                        sb.append( "\n" );
                    }
                    ctx.logInfo( "Reading " + fieldStr.value + ": " + line );
                    sb.append( line );
                    ctx.checkInterrupted();
                    if ( !waitForReady( reader, process, ctx ) ) {
                        break;
                    }
                }
                PolyDocument doc = new PolyDocument();
                doc.put( fieldStr, PolyString.of( sb.toString() ) );
                writer.write( doc );
            }
        } catch ( IOException e ) {
            ctx.logWarning( "Exception while reading " + fieldStr.value + ": " + e.getMessage() );
        }
    }


    private boolean waitForReady( BufferedReader reader, Process process, ExecutionContext ctx ) throws IOException, ExecutorException, InterruptedException {
        while ( !reader.ready() ) {
            if ( !process.isAlive() ) {
                ctx.logWarning( "Stopping line reading as process is dead" );
                return false;
            }
            Thread.sleep( POLL_INTERVAL_MS );
            ctx.checkInterrupted();
        }
        return true;
    }


    private enum Shell {
        BASH( false ),
        SH( false ),
        CMD( true ),
        POWERSHELL( true );

        public final boolean forWindows;


        Shell( boolean forWindows ) {
            this.forWindows = forWindows;
        }
    }

}
