package com.github.rerero.kafka.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class PayloadBasisRouter<R extends ConnectRecord<R>> implements Transformation<R> {

}
