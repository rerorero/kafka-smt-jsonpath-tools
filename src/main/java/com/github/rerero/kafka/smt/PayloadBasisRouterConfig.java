package com.github.rerero.kafka.smt;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class PayloadBasisRouterConfig {

  private final SimpleConfig conf;

  public static final String REPLACEMENT = "replacement";

  public static final ConfigDef DEF = new ConfigDef()
      .define(REPLACEMENT, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "Replacement string.");

  public PayloadBasisRouterConfig(Map<String, ?> props) {
    this.conf = new SimpleConfig(DEF, props);
  }

  public String getReplacementString() {
    return conf.getString(REPLACEMENT);
  }
}
