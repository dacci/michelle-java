package org.dacci.michelle.aws.core;

import static java.util.concurrent.CompletableFuture.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.ssm.SsmAsyncClient;
import software.amazon.awssdk.services.ssm.SsmClient;

@ExtendWith(MockitoExtension.class)
class ParameterStoreTest {
  @Spy private ParameterStore store;

  @BeforeEach
  void setUp() {
    store.clear();
  }

  @Test
  void testLoad_Sync() {
    doReturn(Map.of("answer", "42")).when(store).load((SsmClient) null);

    store.put("hoge", "piyo");
    store.load((SsmClient) null, false);

    assertThat(store, allOf(hasEntry("answer", "42"), hasEntry("hoge", "piyo")));
  }

  @Test
  void testLoad_SyncClear() {
    doReturn(Map.of("answer", "42")).when(store).load((SsmClient) null);

    store.put("hoge", "piyo");
    store.load((SsmClient) null, true);

    assertThat(store, allOf(hasEntry("answer", "42"), not(hasKey("hoge"))));
  }

  @Test
  void testLoad_Async() {
    doReturn(completedFuture(Map.of("answer", "42"))).when(store).load((SsmAsyncClient) null);

    store.put("hoge", "piyo");
    store.load((SsmAsyncClient) null, false).join();

    assertThat(store, allOf(hasEntry("answer", "42"), hasEntry("hoge", "piyo")));
  }

  @Test
  void testLoad_AsyncClear() {
    doReturn(completedFuture(Map.of("answer", "42"))).when(store).load((SsmAsyncClient) null);

    store.put("hoge", "piyo");
    store.load((SsmAsyncClient) null, true).join();

    assertThat(store, allOf(hasEntry("answer", "42"), not(hasKey("hoge"))));
  }
}
