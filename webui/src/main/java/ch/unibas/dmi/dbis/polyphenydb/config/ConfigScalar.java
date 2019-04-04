package ch.unibas.dmi.dbis.polyphenydb.config;


public abstract class ConfigScalar extends Config {

    public ConfigScalar ( String key ) {
        super( key );
    }

    public ConfigScalar ( String key, String description ) {
        super( key, description );
    }

}
