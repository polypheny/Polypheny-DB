package ch.unibas.dmi.dbis.polyphenydb.config;

public abstract class ConfigNumber extends ConfigScalar {

    public ConfigNumber ( String key ) {
        super( key );
    }

    public ConfigNumber ( String key, String description ) {
        super( key, description );
    }

}
