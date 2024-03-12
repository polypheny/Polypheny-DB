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

package org.polypheny.db.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.util.Objects;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;
import javax.swing.WindowConstants;
import javax.swing.border.EtchedBorder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatusNotificationService;
import org.polypheny.db.StatusNotificationService.ErrorConfig;
import org.polypheny.db.StatusNotificationService.StatusType;


/**
 * SplashHelper is responsible for generating the SplashScreen and allows
 * interacting with it.
 */
@Slf4j
public class SplashHelper {

    private final SplashScreen screen;

    @Setter
    private int statusId;
    @Setter
    private int errorId;


    public SplashHelper() {
        screen = new SplashScreen();
        Thread splashT = new Thread( screen );
        splashT.start();
        statusId = StatusNotificationService.addSubscriber( ( m, n ) -> screen.setStatus( m ), StatusType.INFO );
        errorId = StatusNotificationService.addSubscriber( ( m, n ) -> screen.setError( m, (ErrorConfig) n ), StatusType.ERROR );
    }


    public void setComplete() {
        GuiUtils.openUiInBrowser();
        this.screen.setComplete();
        StatusNotificationService.removeSubscriber( statusId );
        StatusNotificationService.removeSubscriber( errorId );
    }


    public static class SplashScreen extends JWindow implements Runnable {

        private final JLabel picLabel;
        private final JFrame frame;
        private final JLabel status;
        private final JButton openButton;
        private boolean inErrorState = false;


        public SplashScreen() {
            this.frame = new JFrame( "Polypheny" );
            frame.setAlwaysOnTop( true );
            frame.setIconImage( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-600.png" ) ) ).getImage() );
            frame.setSize( 400, 250 );
            frame.setUndecorated( true );
            frame.setResizable( false );
            frame.setDefaultCloseOperation( WindowConstants.HIDE_ON_CLOSE );
            frame.getContentPane().setLayout( new BoxLayout( frame.getContentPane(), BoxLayout.Y_AXIS ) );

            JPanel panel = new JPanel( new BorderLayout() );
            panel.setBorder( new EtchedBorder() );
            frame.add( panel, BorderLayout.CENTER );

            // top
            JPanel top = new JPanel( new BorderLayout() );
            top.setBorder( BorderFactory.createEmptyBorder( 12, 12, 0, 0 ) );

            ImageIcon writing = new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-landscape-70.png" ) ) );
            JLabel label = new JLabel( writing );
            top.add( label, BorderLayout.WEST );

            // middle

            JPanel middle = new JPanel( new BorderLayout() );
            this.status = new JLabel( "loading..." );
            status.setFont( new Font( "Verdana", Font.PLAIN, 12 ) );

            // bottom
            JPanel bottom = new JPanel( new BorderLayout() );
            bottom.setBorder( BorderFactory.createEmptyBorder( 0, 12, 0, 12 ) );

            JLabel version = new JLabel( GuiUtils.getPolyphenyVersion() );
            version.setFont( new Font( "Verdana", Font.PLAIN, 10 ) );

            JLabel copy = new JLabel( "©2019-present The Polypheny Project" );
            copy.setFont( new Font( "Verdana", Font.PLAIN, 10 ) );

            bottom.add( copy, BorderLayout.WEST );
            bottom.add( version, BorderLayout.EAST );

            this.picLabel = new JLabel( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "loading-32.gif" ) ) ) );
            picLabel.setBorder( BorderFactory.createEmptyBorder( 0, 12, 0, 12 ) );
            this.openButton = new JButton( "Open Polypheny" );
            this.openButton.setOpaque( false );
            this.openButton.setVisible( false );
            JPanel buttonPanel = new JPanel();
            buttonPanel.setBorder( BorderFactory.createEmptyBorder( 6, 24, 24, 24 ) );
            buttonPanel.add( openButton );

            middle.add( picLabel, BorderLayout.WEST );
            middle.add( status );
            middle.add( buttonPanel, BorderLayout.SOUTH );

            panel.add( top, BorderLayout.NORTH );
            panel.add( middle, BorderLayout.CENTER );
            panel.add( bottom, BorderLayout.SOUTH );

            Dimension dimension = Toolkit.getDefaultToolkit().getScreenSize();
            int x = (int) ((dimension.getWidth() - frame.getWidth()) / 2);
            int y = (int) ((dimension.getHeight() - frame.getHeight()) / 2);
            frame.setLocation( x, y );

            panel.setAlignmentX( Component.CENTER_ALIGNMENT );
            frame.setVisible( true );
        }


        public void setComplete() {
            frame.setVisible( false );
            dispose();
        }


        public void setStatus( String status ) {
            if ( !inErrorState ) {
                this.status.setText( status );
            }
        }


        public void setError( String errorMsg, ErrorConfig config ) {
            // we are already blocking
            if ( inErrorState ) {
                return;
            }

            // block future changes
            if ( config.doBlock() ) {
                inErrorState = true;
            }

            if ( config.showButton() ) {
                this.openButton.setVisible( true );

                // call defined function and exit if needed
                this.openButton.addActionListener( e -> {
                    config.func().accept( null );

                    if ( config.doExit() ) {
                        System.exit( -1 );
                    }
                } );

                this.openButton.setText( config.buttonMessage() );
            }

            this.picLabel.setIcon( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "warning.png" ) ) ) );
            this.status.setText( "<html>" + errorMsg + "</html>" );
        }


        @Override
        public void run() {

        }

    }

}
