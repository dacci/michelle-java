package org.dacci.michelle.aws.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.BatchGetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.UnprocessedQueryExecutionId;

public final class AthenaQueryExecutor {
  private static final Logger log = LoggerFactory.getLogger(AthenaQueryExecutor.class);

  private static final int BATCH_SIZE = 50;

  private final Map<String, CompletableFuture<QueryExecution>> pending = new LinkedHashMap<>();
  private final Map<String, CompletableFuture<QueryExecution>> active = new LinkedHashMap<>();
  private boolean polling = false;

  private final AthenaAsyncClient athena;
  private final Executor executor;
  private final Executor delayedExecutor;

  AthenaQueryExecutor(
      AthenaAsyncClient athena, Executor executor, long interval, TimeUnit intervalUnit) {
    this.athena = athena;
    this.executor = executor;
    delayedExecutor = CompletableFuture.delayedExecutor(interval, intervalUnit, executor);
  }

  public CompletableFuture<QueryExecution> startQueryExecution(
      Consumer<StartQueryExecutionRequest.Builder> request) {
    return startQueryExecution(StartQueryExecutionRequest.builder().applyMutation(request).build());
  }

  public CompletableFuture<QueryExecution> startQueryExecution(StartQueryExecutionRequest request) {
    return athena.startQueryExecution(request).thenComposeAsync(this::onQueryStarted, executor);
  }

  private CompletableFuture<QueryExecution> onQueryStarted(StartQueryExecutionResponse response) {
    var future = new CompletableFuture<QueryExecution>();

    synchronized (pending) {
      pending.put(response.queryExecutionId(), future);
    }

    synchronized (active) {
      if (!polling) {
        polling = true;
        executor.execute(this::pollQueryExecution);
      }
    }

    return future;
  }

  private void pollQueryExecution() {
    synchronized (pending) {
      active.putAll(pending);
      pending.clear();
    }

    var futures = new ArrayList<CompletableFuture<?>>(active.size() / BATCH_SIZE + 1);

    var queries = active.keySet().toArray(String[]::new);
    for (int offset = 0, limit = queries.length; offset < limit; offset += BATCH_SIZE) {
      var length = Math.min(limit - offset, BATCH_SIZE);
      var batch = new String[length];
      System.arraycopy(queries, offset, batch, 0, length);

      var future =
          athena
              .batchGetQueryExecution(b -> b.queryExecutionIds(batch))
              .handleAsync(this::onExecutionQueried, executor);
      futures.add(future);
    }

    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

    synchronized (active) {
      if (active.isEmpty()) {
        polling = false;
      } else {
        delayedExecutor.execute(this::pollQueryExecution);
      }
    }
  }

  private Void onExecutionQueried(BatchGetQueryExecutionResponse response, Throwable exception) {
    if (exception != null) {
      log.error("BatchGetQueryExecution failed", exception);
      return null;
    }

    for (QueryExecution queryExecution : response.queryExecutions()) {
      switch (queryExecution.status().state()) {
        case SUCCEEDED:
        case FAILED:
        case CANCELLED:
          CompletableFuture<QueryExecution> future;
          synchronized (active) {
            future = active.remove(queryExecution.queryExecutionId());
          }
          future.complete(queryExecution);
          break;

        default:
          break;
      }
    }

    for (UnprocessedQueryExecutionId execution : response.unprocessedQueryExecutionIds()) {
      CompletableFuture<QueryExecution> future;
      synchronized (active) {
        future = active.remove(execution.queryExecutionId());
      }
      future.completeExceptionally(
          AthenaException.builder()
              .awsErrorDetails(
                  AwsErrorDetails.builder()
                      .errorCode(execution.errorCode())
                      .errorMessage(execution.errorMessage())
                      .build())
              .build());
    }

    return null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private AthenaAsyncClient athenaAsyncClient = null;
    private Executor executor = null;
    private long interval = 1;
    private TimeUnit unit = TimeUnit.SECONDS;

    public Builder athenaAsyncClient(AthenaAsyncClient athenaAsyncClient) {
      this.athenaAsyncClient = athenaAsyncClient;
      return this;
    }

    public Builder executor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public Builder pollInterval(long interval, TimeUnit unit) {
      this.interval = interval;
      this.unit = unit;
      return this;
    }

    public AthenaQueryExecutor build() {
      Objects.requireNonNull(athenaAsyncClient, "athenaAsyncClient must not be null");
      Objects.requireNonNull(executor, "executor must not be null");
      Objects.requireNonNull(unit, "unit of pollInterval must not be null");

      return new AthenaQueryExecutor(athenaAsyncClient, executor, interval, unit);
    }
  }
}
