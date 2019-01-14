# pubsubgrpctest

## How to run

1. Update `PubsubGrpcTest` constants at the top of the file with your project name and subscription names.

```java
  private final static String PROJECT_NAME = "[your-project]";
  private final static String SUBSCRIPTION_1 = "[you-subscription-for-streaming-pull]";
  private final static String SUBSCRIPTION_2 = "[your-subscription-for-sync-pull]";
```

2. Run `./gradlew clean test` or `mvn clean test`.

3. Witin 10 seconds, publish a message to the topic you created.
