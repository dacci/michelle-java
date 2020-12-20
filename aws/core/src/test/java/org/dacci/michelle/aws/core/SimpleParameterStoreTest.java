package org.dacci.michelle.aws.core;

import static java.util.concurrent.CompletableFuture.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.ssm.SsmAsyncClient;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParametersRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;

@ExtendWith(MockitoExtension.class)
class SimpleParameterStoreTest {
  @Test
  void testLoad_Sync(@Mock SsmClient ssm) {
    doReturn(
            GetParametersResponse.builder()
                .parameters(Parameter.builder().name("key").value("value").build())
                .invalidParameters("hoge")
                .build())
        .when(ssm)
        .getParameters(ArgumentMatchers.any(GetParametersRequest.class));

    var store = ParameterStore.create(b -> {});
    var actual = store.load(ssm);
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, not(hasKey("hoge")));
  }

  @Test
  void testLoad_Async(@Mock SsmAsyncClient ssm) {
    doReturn(
            completedFuture(
                GetParametersResponse.builder()
                    .parameters(Parameter.builder().name("key").value("value").build())
                    .invalidParameters("hoge")
                    .build()))
        .when(ssm)
        .getParameters(ArgumentMatchers.any(GetParametersRequest.class));

    var store = ParameterStore.create(b -> {});
    var actual = store.load(ssm).join();
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, not(hasKey("hoge")));
  }
}
