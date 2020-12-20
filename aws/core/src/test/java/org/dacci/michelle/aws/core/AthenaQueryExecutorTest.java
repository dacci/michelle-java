package org.dacci.michelle.aws.core;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.BatchGetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.UnprocessedQueryExecutionId;

@ExtendWith(MockitoExtension.class)
class AthenaQueryExecutorTest {
  private static ExecutorService threadPool;
  private AthenaAsyncClient athena = mock(AthenaAsyncClient.class);
  private AthenaQueryExecutor executor =
      new AthenaQueryExecutor(athena, threadPool, 1L, TimeUnit.SECONDS);

  @BeforeAll
  static void setUpClass() {
    threadPool = Executors.newCachedThreadPool();
  }

  @AfterAll
  static void tearDownClass() throws InterruptedException {
    threadPool.shutdown();

    while (!threadPool.isTerminated()) {
      threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testStartQueryExecution() {
    doReturn(
            CompletableFuture.completedFuture(
                StartQueryExecutionResponse.builder().queryExecutionId("hoge").build()))
        .when(athena)
        .startQueryExecution(ArgumentMatchers.any(StartQueryExecutionRequest.class));
    doReturn(
            CompletableFuture.completedFuture(
                BatchGetQueryExecutionResponse.builder()
                    .queryExecutions(
                        QueryExecution.builder()
                            .queryExecutionId("hoge")
                            .status(b -> b.state(QueryExecutionState.SUCCEEDED))
                            .build())
                    .build()))
        .when(athena)
        .batchGetQueryExecution(ArgumentMatchers.any(Consumer.class));

    var actual = executor.startQueryExecution(b -> {}).join();
    assertThat(actual.queryExecutionId(), is("hoge"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testStartQueryExecution_ExceptionOnQuery() {
    doReturn(
            CompletableFuture.completedFuture(
                StartQueryExecutionResponse.builder().queryExecutionId("hoge").build()))
        .when(athena)
        .startQueryExecution(ArgumentMatchers.any(StartQueryExecutionRequest.class));
    doReturn(
            CompletableFuture.completedFuture(
                BatchGetQueryExecutionResponse.builder()
                    .unprocessedQueryExecutionIds(
                        UnprocessedQueryExecutionId.builder()
                            .queryExecutionId("hoge")
                            .errorCode("errorCode")
                            .errorMessage("errorMessage")
                            .build())
                    .build()))
        .when(athena)
        .batchGetQueryExecution(ArgumentMatchers.any(Consumer.class));

    var actual =
        assertThrows(CompletionException.class, () -> executor.startQueryExecution(b -> {}).join());

    assertThat(actual.getCause(), instanceOf(AthenaException.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testStartQueryExecution_ExceptionOnStartQuery() {
    doThrow(AthenaException.class)
        .when(athena)
        .startQueryExecution(ArgumentMatchers.any(StartQueryExecutionRequest.class));

    assertThrows(AthenaException.class, () -> executor.startQueryExecution(b -> {}).join());

    verify(athena, times(0)).batchGetQueryExecution(ArgumentMatchers.any(Consumer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testStartQueryExecution_ExceptionOnPolling() {
    doReturn(
            CompletableFuture.completedFuture(
                StartQueryExecutionResponse.builder().queryExecutionId("hoge").build()))
        .when(athena)
        .startQueryExecution(ArgumentMatchers.any(StartQueryExecutionRequest.class));
    doReturn(CompletableFuture.failedFuture(AthenaException.builder().build()))
        .when(athena)
        .batchGetQueryExecution(ArgumentMatchers.any(Consumer.class));

    assertFalse(executor.startQueryExecution(b -> {}).isDone());
  }
}
