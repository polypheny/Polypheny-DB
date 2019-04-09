package ch.unibas.dmi.dbis.polyphenydb.config;


import java.math.BigDecimal;


public class ConfigDouble extends ConfigNumber {

    double value;

    public ConfigDouble ( String key, double value ) {
        this.key = key;
        this.value = value;
    }

    public ConfigDouble ( String key, String description, double value ) {
        this.key = key;
        this.description = description;
        this.value = value;
    }

    @Override
    public void setObject( Object value ) {
        this.value = ( double ) value;
        notifyConfigListeners( this );
    }

    @Override
    public double getDouble() {
        return this.value;
    }

    @Override
    public BigDecimal getDecimal() {
        return new BigDecimal( this.value );
    }

    @Override
    public void setDouble( double value ) {
        this.value = value;
        notifyConfigListeners( this );
    }
}
