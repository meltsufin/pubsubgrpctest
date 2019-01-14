/*
 *  Copyright 2018 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * @author Mike Eltsufin
 */
public class PubsubGrcpTest {

  private final static String PROJECT_NAME = "eltsufin-sandbox";
  private final static String SUBSCRIPTION_1 = "test-subscription-1";
  private final static String SUBSCRIPTION_2 = "test-subscription-2";

  @Test
  public void test() {
    ProjectSubscriptionName subscription = ProjectSubscriptionName
        .of(PROJECT_NAME, SUBSCRIPTION_1);

    MessageReceiver receiver =
        (message, consumer) -> {
          System.out.println("got message: " + message.getData().toStringUtf8());
          consumer.ack();
        };

    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscription, receiver).build();
      subscriber.addListener(
          new Subscriber.Listener() {
            @Override
            public void failed(Subscriber.State from, Throwable failure) {
              // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
              System.err.println(failure);
            }
          },
          MoreExecutors.directExecutor());
      subscriber.startAsync().awaitRunning();

      // wait 10 seconds for messages to arrive
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (subscriber != null) {
        subscriber.stopAsync();
      }
    }
  }

  @Test
  public void testPull() throws IOException {
    SubscriberStubSettings subscriberStubSettings =
        SubscriberStubSettings.newBuilder()
            .setTransportChannelProvider(
                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                    .setMaxInboundMessageSize(20 << 20) // 20MB
                    .build())
            .build();

    try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
      int numOfMessages = 10;   // max number of messages to be pulled
      String subscriptionName = ProjectSubscriptionName.format(PROJECT_NAME, SUBSCRIPTION_2);
      PullRequest pullRequest =
          PullRequest.newBuilder()
              .setMaxMessages(numOfMessages)
              .setReturnImmediately(false) // return immediately if messages are not available
              .setSubscription(subscriptionName)
              .build();

      // use pullCallable().futureCall to asynchronously perform this operation
      PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
      List<String> ackIds = new ArrayList<>();
      for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
        // handle received message
        // ...
        ackIds.add(message.getAckId());
      }
      // acknowledge received messages
      AcknowledgeRequest acknowledgeRequest =
          AcknowledgeRequest.newBuilder()
              .setSubscription(subscriptionName)
              .addAllAckIds(ackIds)
              .build();
      // use acknowledgeCallable().futureCall to asynchronously perform this operation
      subscriber.acknowledgeCallable().call(acknowledgeRequest);
      pullResponse.getReceivedMessagesList().forEach(System.out::println);
    }
  }
}
