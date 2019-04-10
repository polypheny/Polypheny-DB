package ch.unibas.dmi.dbis.polyphenydb.config;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

@WebSocket
public class ConfigWebsocket {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();

    @OnWebSocketConnect
    public void connected(Session session) {
        System.out.println("Ui connected to Websocket");
        sessions.add(session);
    }

    @OnWebSocketClose
    public void closed(Session session, int statusCode, String reason) {
        System.out.println("Ui left Websocket");
        sessions.remove(session);
    }

    @OnWebSocketMessage
    public void configWebSocket(Session session, String message) throws IOException {
        System.out.println("Got: " + message);   // Print message
        session.getRemote().sendString(message); // and send it back
    }

    public static void broadcast( String msg ) throws IOException{
        System.out.println("broadcasting..");
        for( Session s: sessions ){
            s.getRemote().sendString( msg );
        }
    }

}
