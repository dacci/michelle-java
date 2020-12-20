package org.dacci.michelle.aws.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.ssm.SsmAsyncClient;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathResponse;
import software.amazon.awssdk.services.ssm.model.GetParametersRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;

@SuppressWarnings("serial")
public abstract class ParameterStore extends LinkedHashMap<String, String> {
  static class Simple extends ParameterStore {
    private static final Collector<Parameter, ?, Map<String, String>> TO_MAP_COLLECTOR =
        Collectors.toMap(Parameter::name, Parameter::value);

    static Map<String, String> toMap(GetParametersResponse response) {
      return response.parameters().stream().collect(TO_MAP_COLLECTOR);
    }

    private final GetParametersRequest request;

    Simple(GetParametersRequest request) {
      this.request = request;
    }

    @Override
    Map<String, String> load(SsmClient ssm) {
      return toMap(ssm.getParameters(request));
    }

    @Override
    CompletableFuture<Map<String, String>> load(SsmAsyncClient ssm) {
      return ssm.getParameters(request).thenApply(Simple::toMap);
    }
  }

  public static ParameterStore create(GetParametersRequest request) {
    return new Simple(request);
  }

  public static ParameterStore create(Consumer<GetParametersRequest.Builder> request) {
    return create(GetParametersRequest.builder().applyMutation(request).build());
  }

  static class Path extends ParameterStore {
    private final Collector<Parameter, ?, Map<String, String>> toMapCollector =
        Collectors.toMap(this::toMapKey, Parameter::value);
    private final GetParametersByPathRequest request;
    private final int prefixLength;

    Path(GetParametersByPathRequest request) {
      this.request = request;
      prefixLength =
          request.path().equals("/")
              ? 0
              : request.path().length() + (request.path().endsWith("/") ? 0 : 1);
    }

    private String toMapKey(Parameter parameter) {
      return parameter.name().substring(prefixLength);
    }

    @Override
    Map<String, String> load(SsmClient ssm) {
      return ssm.getParametersByPathPaginator(request).stream()
          .map(GetParametersByPathResponse::parameters)
          .flatMap(List::stream)
          .collect(toMapCollector);
    }

    @Override
    CompletableFuture<Map<String, String>> load(SsmAsyncClient ssm) {
      Map<String, String> loaded = new LinkedHashMap<>();
      return ssm.getParametersByPathPaginator(request)
          .flatMapIterable(GetParametersByPathResponse::parameters)
          .subscribe(p -> loaded.put(toMapKey(p), p.value()))
          .thenApply(v -> loaded);
    }
  }

  public static ParameterStore createByPath(GetParametersByPathRequest request) {
    return new Path(request);
  }

  public static ParameterStore createByPath(Consumer<GetParametersByPathRequest.Builder> request) {
    return createByPath(GetParametersByPathRequest.builder().applyMutation(request).build());
  }

  private final Object lock = new Object[0];

  ParameterStore() {}

  abstract Map<String, String> load(SsmClient ssm);

  public void load(SsmClient ssm, boolean clear) {
    putAll(load(ssm), clear);
  }

  abstract CompletableFuture<Map<String, String>> load(SsmAsyncClient ssm);

  public CompletableFuture<Void> load(SsmAsyncClient ssm, boolean clear) {
    return load(ssm).thenAccept(loaded -> putAll(loaded, clear));
  }

  private void putAll(Map<String, String> loaded, boolean clear) {
    synchronized (lock) {
      if (clear) clear();
      putAll(loaded);
    }
  }
}
