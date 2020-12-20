package org.dacci.michelle.aws.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectReader;

import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ObjectCache<T> {
  private static final int HTTP_NOT_MODIFIED = 304;

  class Entry implements AsyncResponseTransformer<GetObjectResponse, T>, Subscriber<ByteBuffer> {
    T value;
    Instant lastModified;

    volatile CompletableFuture<T> future;
    volatile GetObjectResponse response;
    volatile JsonParser parser;
    volatile ByteArrayFeeder feeder;
    volatile Subscription subscription;

    @Override
    public CompletableFuture<T> prepare() {
      future = new CompletableFuture<>();
      return future;
    }

    @Override
    public void onResponse(GetObjectResponse response) {
      this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
      publisher.subscribe(this);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      try {
        parser = objectReader.createNonBlockingByteArrayParser();
        feeder = (ByteArrayFeeder) parser;
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
      } catch (IOException e) {
        future.completeExceptionally(e);
        subscription.cancel();
        reset();
      }
    }

    @Override
    public void onNext(ByteBuffer buffer) {
      var bytes = new byte[buffer.remaining()];
      buffer.get(bytes);

      try {
        feeder.feedInput(bytes, 0, bytes.length);
        subscription.request(Long.MAX_VALUE);
      } catch (IOException e) {
        subscription.cancel();
        future.completeExceptionally(e);
        reset();
      }
    }

    @Override
    public void onComplete() {
      try {
        value = objectReader.readValue(parser, valueType);
        lastModified = response.lastModified();
        future.complete(value);
      } catch (IOException e) {
        future.completeExceptionally(e);
      } finally {
        reset();
      }
    }

    @Override
    public void exceptionOccurred(Throwable error) {
      var cause = error.getCause();
      if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() == HTTP_NOT_MODIFIED) {
        future.complete(value);
      } else {
        future.completeExceptionally(error);
      }

      reset();
    }

    @Override
    public void onError(Throwable error) {
      future.completeExceptionally(error);
      reset();
    }

    private void reset() {
      future = null;
      response = null;
      parser = null;
      feeder = null;
      subscription = null;
    }
  }

  private final ObjectReader objectReader;
  private final String bucket;
  private final String prefix;
  private final JavaType valueType;

  private final Map<String, Entry> cache = new ConcurrentHashMap<>();

  private S3ObjectCache(
      ObjectReader objectReader, String bucket, String prefix, JavaType valueType) {
    this.objectReader = objectReader;
    this.bucket = bucket;
    this.prefix = prefix;
    this.valueType = valueType;
  }

  /**
   * Gets value of corresponding key.
   *
   * @param s3 S3 client to make a request.
   * @param key Key of the object to get.
   * @return the value
   * @throws NoSuchKeyException The specified key does not exist.
   * @throws SdkException Base class for all exceptions that can be thrown by the SDK (both service
   *     and client). Can be used for catch all scenarios.
   * @throws SdkClientException If any client side error occurs such as an IO related failure,
   *     failure to get credentials, etc.
   * @throws S3Exception Base class for all service exceptions. Unknown exceptions will be thrown as
   *     an instance of this type.
   * @throws IOException if a low-level I/O problem (unexpected end-of-input, network error) occurs
   *     (passed through as-is without additional wrapping -- note that this is one case where
   *     {@link com.fasterxml.jackson.databind.DeserializationFeature#WRAP_EXCEPTIONS} does NOT
   *     result in wrapping of exception even if enabled)
   * @throws JsonParseException if underlying input contains invalid content of type {@link
   *     com.fasterxml.jackson.core.JsonParser} supports (JSON for default case)
   * @throws JsonMappingException if the input JSON structure does not match structure expected for
   *     result type (or has other mismatch issues)
   */
  public T get(S3Client s3, String key)
      throws IOException, JsonParseException, JsonMappingException {
    if (s3 == null) throw new IllegalArgumentException("s3 must not be null");
    if (key == null) throw new IllegalArgumentException("key must not be null");

    var entry = cache.computeIfAbsent(key, k -> new Entry());

    synchronized (entry) {
      if (entry.future != null) {
        return entry.future.join();
      }

      entry.future = new CompletableFuture<>();

      try (var in =
          s3.getObject(
              b -> b.bucket(bucket).key(prefix + key).ifModifiedSince(entry.lastModified))) {
        var parser = objectReader.getFactory().createParser(in);
        entry.value = objectReader.readValue(parser, valueType);
        entry.lastModified = in.response().lastModified();
        entry.future.complete(entry.value);
      } catch (S3Exception e) {
        if (e.statusCode() != HTTP_NOT_MODIFIED) {
          entry.future.completeExceptionally(e);
          throw e;
        }
      } finally {
        entry.future = null;
      }
    }

    return entry.value;
  }

  public CompletableFuture<T> get(S3AsyncClient s3, String key) {
    if (s3 == null) throw new IllegalArgumentException("s3 must not be null");
    if (key == null) throw new IllegalArgumentException("key must not be null");

    var entry = cache.computeIfAbsent(key, k -> new Entry());

    synchronized (entry) {
      if (entry.future != null) {
        return entry.future;
      }

      return s3.getObject(
          b -> b.bucket(bucket).key(prefix + key).ifModifiedSince(entry.lastModified), entry);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ObjectReader objectReader;
    private String bucket;
    private String prefix = "";

    public Builder objectReader(ObjectReader objectReader) {
      if (objectReader == null) throw new IllegalArgumentException("objectReader must not be null");

      this.objectReader = objectReader;
      return this;
    }

    public Builder bucket(String bucket) {
      if (bucket == null || bucket.isEmpty())
        throw new IllegalArgumentException("bucket must not be null nor empty");

      this.bucket = bucket;
      return this;
    }

    public Builder prefix(String prefix) {
      this.prefix = Objects.requireNonNullElse(prefix, "");
      return this;
    }

    public <T> S3ObjectCache<T> build(Class<? extends T> clazz) {
      if (clazz == null) throw new IllegalArgumentException("clazz must not be null");
      if (objectReader == null) throw new IllegalStateException("objectReader must be specified");

      return build(objectReader.getTypeFactory().constructType(clazz));
    }

    public <T> S3ObjectCache<T> build(TypeReference<T> typeReference) {
      if (typeReference == null)
        throw new IllegalArgumentException("typeReference must not be null");
      if (objectReader == null) throw new IllegalStateException("objectReader must be specified");

      return build(objectReader.getTypeFactory().constructType(typeReference));
    }

    public <T> S3ObjectCache<T> build(JavaType type) {
      if (type == null) throw new IllegalArgumentException("type must not be null");
      if (objectReader == null) throw new IllegalStateException("objectReader must be specified");
      if (bucket == null) throw new IllegalStateException("bucket must be specified");

      return new S3ObjectCache<>(objectReader, bucket, prefix, type);
    }
  }
}
