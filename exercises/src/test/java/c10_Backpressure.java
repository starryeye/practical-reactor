import org.junit.jupiter.api.*;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

/**
 * Backpressure is a mechanism that allows a consumer to signal to a producer that it is ready receive data.
 * This is important because the producer may be sending data faster than the consumer can process it, and can overwhelm consumer.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactive.backpressure
 * https://projectreactor.io/docs/core/release/reference/#_on_backpressure_and_ways_to_reshape_requests
 * https://projectreactor.io/docs/core/release/reference/#_operators_that_change_the_demand_from_downstream
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_asynchronous_but_single_threaded_push
 * https://projectreactor.io/docs/core/release/reference/#_a_hybrid_pushpull_model
 * https://projectreactor.io/docs/core/release/reference/#_an_alternative_to_lambdas_basesubscriber
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c10_Backpressure extends BackpressureBase {

    /**
     * In this exercise subscriber (test) will request several messages from the message stream.
     * Hook to the requests and record them to the `requests` list.
     */
    @Test
    public void request_and_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();
        Flux<String> messageStream = messageStream1()
                .doOnRequest(requests::add) // request 이벤트를 hooking 하여, 요청된 값을 requests 에 넣는다.
                ;

        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(1)
                    .then(() -> pub1.next("msg#1"))
                    .thenRequest(3)
                    .then(() -> pub1.next("msg#2", "msg#3"))
                    .then(pub1::complete)
                    .expectNext("msg#1", "msg#2", "msg#3")
                    .verifyComplete();

        Assertions.assertEquals(List.of(1L, 3L), requests);
    }

    /**
     * Adjust previous solution in such a way that you limit rate of requests. Number of requested messages stays the
     * same, but each request should be limited to 1 message.
     */
    @Test
    public void limited_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();
        Flux<String> messageStream = messageStream2()
                .doOnRequest(requests::add)
                .limitRate(1)
                ;
        /**
         * limitRate..
         * upstream 방향으로 적용되는 연산자이다. (doOnRequest 와 limitRate 순서를 변경하면 다른 결과가 나오는 것을 볼 수 있다.)
         * 2 개를 요청했는데 1 개로 제한(limitRate(1)) 해두면 1 개 아이템이 방출되면 다시 1 개 요청하는 방식이다.
         * 마블다이어그램 참고하면 쉬움
         */

        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(1)
                    .then(() -> pub2.next("msg#1"))
                    .thenRequest(3)
                    .then(() -> pub2.next("msg#2", "msg#3"))
                    .then(pub2::complete)
                    .expectNext("msg#1", "msg#2", "msg#3")
                    .verifyComplete();

        Assertions.assertEquals(List.of(1L, 1L, 1L, 1L), requests);
    }

    /**
     * Finish the implementation of the `uuidGenerator` so it exactly requested amount of UUIDs.
     * Or better said, it should respect the backpressure of the consumer.
     */
    @Test
    public void uuid_generator() {
        Flux<UUID> uuidGenerator = Flux.create(sink -> {
            sink.onRequest(r -> {
                LongStream.range(0, r)
                        .forEach(i -> sink.next(UUID.randomUUID()));
            });
        });
        // request 한 값(r) 만큼 UUID 를 생산해서 방출 해준다.

        StepVerifier.create(uuidGenerator
                                    .doOnNext(System.out::println)
                                    .timeout(Duration.ofSeconds(1))
                                    .onErrorResume(TimeoutException.class, e -> Flux.empty()),
                            StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(10)
                    .expectNextCount(10)
                    .thenCancel()
                    .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure.
     * In case that publisher produces more messages than subscriber is able to consume, raise an error.
     */
    @Test
    public void pressure_is_too_much() {
        Flux<String> messageStream = messageStream3()
                .onBackpressureError()
                ;

        /**
         * onBackpressureError..
         * 마블다이어그램을 보면 쉬운데..
         * 검문소 같은 역할을 한다.
         * upstream 방향으로 전달된 request 이벤트의 값이 2 개인데..
         * 모종의 이유로 item 이 2 개를 초과하여 전달 되는게 onBackpressureError 에서 확인하면
         * onBackpressureError 에서는 3 번째 item 을 전달하지 않고 downstream 으로는 OverflowException (failWithOverflow)을 전달하고
         * publisher 는 cancel 시킨다.
         */

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                                                              .initialRequest(0))
                    .expectSubscription()
                    .thenRequest(3)
                    .then(() -> pub3.next("A", "B", "C", "D"))
                    .expectNext("A", "B", "C")
                    .expectErrorMatches(Exceptions::isOverflow)
                    .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure. In case that publisher
     * produces more messages than subscriber is able to consume, buffer them for later consumption without raising an
     * error.
     */
    @Test
    public void u_wont_brake_me() {
        Flux<String> messageStream = messageStream4()
                .onBackpressureBuffer()
                ;
        /**
         * onBackpressureBuffer..
         * 마블 다이어그램을 보면 쉽다.
         * onBackpressureError 처럼 검문소 역할이다.
         * upstream 방향으로 전달된 request 이벤트의 값이 2 개인데..
         * 모종의 이유로 item 이 2 개를 초과하여 전달 되는게 onBackpressureBuffer 에서 확인하면
         * onBackpressureBuffer 에서 버퍼에 담아 두고 다음 request 이벤트가 오면 그만큼 방출해준다.
         */

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                                                              .initialRequest(0))
                    .expectSubscription()
                    .thenRequest(3)
                    .then(() -> pub4.next("A", "B", "C", "D"))
                    .expectNext("A", "B", "C")
                    .then(() -> pub4.complete())
                    .thenAwait()
                    .thenRequest(1)
                    .expectNext("D")
                    .verifyComplete();
    }

    /**
     * We saw how to react to request demand from producer side. In this part we are going to control demand from
     * consumer side by implementing BaseSubscriber directly.
     * Finish implementation of base subscriber (consumer of messages) with following objectives:
     * - once there is subscription, you should request exactly 10 messages from publisher
     * - once you received 10 messages, you should cancel any further requests from publisher.
     * Producer respects backpressure.
     */
    @Test
    public void subscriber() throws InterruptedException {
        AtomicReference<CountDownLatch> lockRef = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Subscription> sub = new AtomicReference<>();

        remoteMessageProducer()
                .doOnCancel(() -> lockRef.get().countDown())
                .subscribeWith(new BaseSubscriber<String>() {
                    //todo: do your changes only within BaseSubscriber class implementation
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        sub.set(subscription);
                        subscription.request(10);
                    }

                    @Override
                    protected void hookOnNext(String s) {
                        System.out.println(s);
                        if(count.incrementAndGet() == 10) {
                            sub.get().cancel();
                        }
                    }
                    //-----------------------------------------------------
                });

        lockRef.get().await();
        Assertions.assertEquals(10, count.get());
    }
}
