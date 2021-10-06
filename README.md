# kafka-smt-payload-basis-router

Kafka connect single message transform (SMT) for topic routing based on the field in a message.

- You can use the field values in a Kafka message to specify the name of the topic to route to.
- Using JsonPath-like expression.

# Installation

[Download the jar file from the release page](https://github.com/rerorero/kafka-smt-payload-basis-router/releases) and copy it into a directory that is under one of the plugin.path. [This doccument](https://docs.confluent.io/platform/current/connect/transforms/custom.html) would help you.

# Configurations

Example:

```
{
  ...

  "transforms": "route",
  "transforms.route.type": "com.github.rerorero.kafka.smt.PayloadBasisRouter$Value",
  "transforms.route.replacement": "topic-{$.user.name}"
}
```

| Config        | Description                      |
| ------------- | -------------------------------- |
| `replacement` | Topic name to route the message. |

| Config        | Description                                                                                                                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`        | Select `com.github.rerorero.kafka.smt.PayloadBasisRouter$Value` or </br>`com.github.rerorero.kafka.smt.PayloadBasisRouter$Key` based on </br> which field do you want to use for `replacement`. |
| `replacement` | Topic name to route the message.                                                                                                                                                                |

### `replacement` format

`replacement` can include one or more fields in the Kafka message value or key. Fields are specified as JsonPath-like string enclosed in `{}` e.g. `{$.user.name}`.

Only the following syntaxes are supported for now:

| Operator     | Description                                                                 |
| ------------ | --------------------------------------------------------------------------- |
| `$`          | The root element. All JsonPath string has to be started with this operator. |
| `.<name>`    | Dot-notated child.                                                          |
| `['name']`   | Bracket-notated child                                                       |
| `[<number>]` | Array index.                                                                |

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
