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
import software.amazon.awssdk.services.ssm.model.GetParametersByPathRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.paginators.GetParametersByPathIterable;
import software.amazon.awssdk.services.ssm.paginators.GetParametersByPathPublisher;

@ExtendWith(MockitoExtension.class)
class PathParameterStoreTest {
  @Test
  void testLoad_Sync(@Mock SsmClient ssm) {
    doReturn(
            GetParametersByPathResponse.builder()
                .parameters(
                    Parameter.builder().name("/test/key").value("value").build(),
                    Parameter.builder().name("/test/sub/key").value("value").build())
                .build())
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    doReturn(new GetParametersByPathIterable(ssm, GetParametersByPathRequest.builder().build()))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(b -> b.path("/test"));
    var actual = store.load(ssm);
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("sub/key", "value"));
  }

  @Test
  void testLoad_Async(@Mock SsmAsyncClient ssm) {
    doReturn(
            completedFuture(
                GetParametersByPathResponse.builder()
                    .parameters(
                        Parameter.builder().name("/test/key").value("value").build(),
                        Parameter.builder().name("/test/sub/key").value("value").build())
                    .build()))
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    var request = GetParametersByPathRequest.builder().path("/test").build();
    doReturn(new GetParametersByPathPublisher(ssm, request))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(request);
    var actual = store.load(ssm).join();
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("sub/key", "value"));
  }

  @Test
  void testLoad_SyncSlash(@Mock SsmClient ssm) {
    doReturn(
            GetParametersByPathResponse.builder()
                .parameters(
                    Parameter.builder().name("key").value("value").build(),
                    Parameter.builder().name("/test/key").value("value").build())
                .build())
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    doReturn(new GetParametersByPathIterable(ssm, GetParametersByPathRequest.builder().build()))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(b -> b.path("/"));
    var actual = store.load(ssm);
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("/test/key", "value"));
  }

  @Test
  void testLoad_AsyncSlash(@Mock SsmAsyncClient ssm) {
    doReturn(
            completedFuture(
                GetParametersByPathResponse.builder()
                    .parameters(
                        Parameter.builder().name("key").value("value").build(),
                        Parameter.builder().name("/test/key").value("value").build())
                    .build()))
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    var request = GetParametersByPathRequest.builder().path("/").build();
    doReturn(new GetParametersByPathPublisher(ssm, request))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(request);
    var actual = store.load(ssm).join();
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("/test/key", "value"));
  }

  @Test
  void testLoad_SyncPathEndsWithSlash(@Mock SsmClient ssm) {
    doReturn(
            GetParametersByPathResponse.builder()
                .parameters(
                    Parameter.builder().name("/test/key").value("value").build(),
                    Parameter.builder().name("/test/sub/key").value("value").build())
                .build())
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    doReturn(new GetParametersByPathIterable(ssm, GetParametersByPathRequest.builder().build()))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(b -> b.path("/test/"));
    var actual = store.load(ssm);
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("sub/key", "value"));
  }

  @Test
  void testLoad_AsyncPathEndsWithSlash(@Mock SsmAsyncClient ssm) {
    doReturn(
            completedFuture(
                GetParametersByPathResponse.builder()
                    .parameters(
                        Parameter.builder().name("/test/key").value("value").build(),
                        Parameter.builder().name("/test/sub/key").value("value").build())
                    .build()))
        .when(ssm)
        .getParametersByPath(ArgumentMatchers.any(GetParametersByPathRequest.class));
    var request = GetParametersByPathRequest.builder().path("/test/").build();
    doReturn(new GetParametersByPathPublisher(ssm, GetParametersByPathRequest.builder().build()))
        .when(ssm)
        .getParametersByPathPaginator(ArgumentMatchers.any(GetParametersByPathRequest.class));

    var store = ParameterStore.createByPath(request);
    var actual = store.load(ssm).join();
    assertThat(actual, hasEntry("key", "value"));
    assertThat(actual, hasEntry("sub/key", "value"));
  }
}
