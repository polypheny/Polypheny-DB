package ch.unibas.dmi.dbis.polyphenydb.config;


import java.math.BigDecimal;


public class ConfigDecimal extends ConfigNumber {

    private BigDecimal value;

    public ConfigDecimal ( String key, BigDecimal value ) {
        this.key = key;
        this.value = value;
    }

    public ConfigDecimal ( String key, String description, BigDecimal value ) {
        this.key = key;
        this.description = description;
        this.value = value;
    }

    @Override
    public void setObject( Object value ) {
        this.value = ( BigDecimal ) value;
        notifyConfigListeners( this );
    }

    @Override
    public BigDecimal getDecimal() {
        return this.value;
    }

    @Override
    public void setDecimal( BigDecimal value ) {
        this.value = value;
        notifyConfigListeners( this );
    }

}
