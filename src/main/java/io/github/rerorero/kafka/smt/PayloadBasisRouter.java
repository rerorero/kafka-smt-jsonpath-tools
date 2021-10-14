package io.github.rerorero.kafka.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class PayloadBasisRouter<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String REPLACEMENT = "replacement";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(REPLACEMENT, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "Replacement string. "
              + "You can use the JsonPath format to represent a reference to a field in a message. "
              + "For example, if you want to use a field named id with a suffix, "
              + "you can use the following: \"{$.id}_topic\"");

  private JsonPathFormatter.ForStruct structFormatter;
  private JsonPathFormatter.ForMap mapFormatter;

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    final String format = config.getString(REPLACEMENT);
    structFormatter = new JsonPathFormatter.ForStruct(format);
    mapFormatter = new JsonPathFormatter.ForMap(format);
  }

  @Override
  public R apply(R record) {
    final Object opValue = operatingValue(record);
    if (opValue == null) {
      throw new DataException("Can't extract field value: message is null");
    }

    String replacedTopic;
    if (operatingSchema(record) == null) {
      final Map<String, Object> value = requireMap(opValue, "smt-payload-basis-router");
      replacedTopic = mapFormatter.replace(value);
    } else {
      final Struct value = requireStruct(opValue, "smt-payload-basis-router");
      replacedTopic = structFormatter.replace(value);
    }

    return record
        .newRecord(replacedTopic, record.kafkaPartition(), record.keySchema(), record.key(),
            record.valueSchema(), record.value(), record.timestamp());
  }

  @Override
  public void close() {
  }

  abstract Schema operatingSchema(R record);

  abstract Object operatingValue(R record);

  public static class Key<R extends ConnectRecord<R>> extends PayloadBasisRouter<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends PayloadBasisRouter<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }
  }
}
