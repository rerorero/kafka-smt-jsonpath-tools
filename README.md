# kafka-smt-jsonpath-tools

# Installation

[Download the jar file from the release page](https://github.com/rerorero/kafka-jsonpath-tools/releases) and copy it into a directory that is under one of the plugin.path. [This doccument](https://docs.confluent.io/platform/current/connect/transforms/custom.html) would help you.

## PayloadBasisRouter

SMT for topic routing based on the field in a message.

- You can use the field values in a Kafka message to specify the name of the topic to route to.
- Using JsonPath-like expression.

Example:

```
{
  ...

  "transforms": "route",
  "transforms.route.type": "io.github.rerorero.kafka.smt.PayloadBasisRouter$Value",
  "transforms.route.replacement": "topic-{$.user.name}"
}
```

| Config        | Description                      |
| ------------- | -------------------------------- |
| `replacement` | Topic name to route the message. |

| Config        | Description                                                                                                                                                                                   |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`        | Select `io.github.rerorero.kafka.smt.PayloadBasisRouter$Value` or </br>`io.github.rerorero.kafka.smt.PayloadBasisRouter$Key` based on </br> which field do you want to use for `replacement`. |
| `replacement` | Topic name to route the message. It can include one or more fields in Kafka message value or key. Fields are specified as JaonPath string enclosed in `{}` like `{$.user.name}`.              |

## Json Path expression

Not all grammers are supported. Please see here for the current limitations:
[rerorero/kafka-connect-jsonpath-accessor#json-path-expressions](https://github.com/rerorero/kafka-connect-jsonpath-accessor#json-path-expressions)

# Build and Deployment

Build and test:

```
gradlew build test
```

Run integration test:

```
gradlew build shadowJar
cd e2e
./test.sh
echo $? # should exit with 0
```
