import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

/**
 * In Reactor a Sink allows safe manual triggering of signals. We will learn more about multicasting and backpressure in
 * the next chapters.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#sinks
 * https://projectreactor.io/docs/core/release/reference/#processor-overview
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c8_Sinks extends SinksBase {

    /**
     * You need to execute operation that is submitted to legacy system which does not support Reactive API.
     * You want to avoid blocking and let subscribers subscribe to `operationCompleted` Mono,
     * that will emit `true` once submitted operation is executed by legacy system.
     */
    @Test
    public void single_shooter() {

        Sinks.One<Boolean> sink = Sinks.one();

        Mono<Boolean> operationCompleted = sink.asMono();
        submitOperation(() -> {
            doSomeWork(); //don't change this line, 이 메서드가 레거시 코드 동작 부분이다.(동기 blocking)
            sink.tryEmitValue(true); // 레거시 코드가 끝나면 완료 이벤트를 발행한다.
        });

        /**
         * Sinks 정의..
         * "Sinks" are constructs through which Reactive Streams signals can be programmatically pushed,
         * with Flux or Mono semantics.
         */

        //don't change code below
        StepVerifier.create(operationCompleted.timeout(Duration.ofMillis(5500)))
                    .expectNext(true)
                    .verifyComplete();
    }

    /**
     * Similar to previous exercise, you need to execute operation that is submitted to legacy system which does not
     * support Reactive API. This time you need to obtain result of `get_measures_reading()` and emit it to subscriber.
     * If measurements arrive before subscribers subscribe to `get_measures_readings()`, buffer them and emit them to
     * subscribers once they are subscribed.
     */
    @Test
    public void single_subscriber() {

        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Integer> measurements = sink.asFlux();
        submitOperation(() -> {

            List<Integer> measures_readings = get_measures_readings(); //don't change this line, 이 메서드가 레거시 코드 동작 부분이다.(동기 blocking)

            measures_readings.forEach(sink::tryEmitNext); // 리스트의 아이템을 순차적으로 stream 아이템으로 방출
            sink.tryEmitComplete(); // stream 완료 이벤트 방출
        });

        /**
         * Sinks.Many<Integer> sink:
         *  - 여러 값을 방출할 수 있는 Sink를 정의, 이 경우 Integer 타입의 값을 방출
         * Sinks.many():
         * 	- Sinks 클래스에서 다수의 값을 방출할 수 있는 Many 타입의 Sink를 생성하는 팩토리 메서드이다.
         * multicast():
         * 	- multicast() 는 여러 구독자가 Sink에 구독할 수 있도록 함. 즉, 여러 구독자가 이 Sink 에서 방출하는 값을 받을 수 있다. (해당 테스트에서 구독자는 StepVerifier 하나이다.)
         * 	- multicast() 는 모든 구독자에게 동일한 값을 방출. 이는 멀티캐스트 방식으로, 하나의 데이터 소스를 여러 구독자에게 공유하는 것을 의미한다.
         * onBackpressureBuffer():
         * 	- 백프레셔 상황에서 값을 버퍼에 저장하도록 설정. 백프레셔는 데이터 생산 속도가 소비 속도보다 빠를 때 발생하는 문제
         * 	- onBackpressureBuffer() 는 이러한 상황에서 방출되지 않은 값을 버퍼에 저장하여 구독자가 데이터를 처리할 수 있을 때까지 유지.
         * 	- 이는 데이터 유실을 방지하고, 소비자가 데이터를 처리할 수 있을 때까지 값을 버퍼링하는 방식으로 시스템을 안정적으로 유지될 수 있도록 한다.
         */

        //don't change code below
        StepVerifier.create(measurements
                                    .delaySubscription(Duration.ofSeconds(6)))
                    .expectNext(0x0800, 0x0B64, 0x0504)
                    .verifyComplete();
    }

    /**
     * Same as previous exercise, but with twist that you need to emit measurements to multiple subscribers.
     * Subscribers should receive only the signals pushed through the sink after they have subscribed.
     */
    @Test
    public void it_gets_crowded() {
        //todo: feel free to change code as you need
        Flux<Integer> measurements = null;
        submitOperation(() -> {

            List<Integer> measures_readings = get_measures_readings(); //don't change this line
        });

        //don't change code below
        StepVerifier.create(Flux.merge(measurements
                                               .delaySubscription(Duration.ofSeconds(1)),
                                       measurements.ignoreElements()))
                    .expectNext(0x0800, 0x0B64, 0x0504)
                    .verifyComplete();
    }

    /**
     * By default, if all subscribers have cancelled (which basically means they have all un-subscribed), sink clears
     * its internal buffer and stops accepting new subscribers. For this exercise, you need to make sure that if all
     * subscribers have cancelled, the sink will still accept new subscribers. Change this behavior by setting the
     * `autoCancel` parameter.
     */
    @Test
    public void open_24_7() {
        //todo: set autoCancel parameter to prevent sink from closing
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<Integer> flux = sink.asFlux();

        //don't change code below
        submitOperation(() -> {
            get_measures_readings().forEach(sink::tryEmitNext);
            submitOperation(sink::tryEmitComplete);
        });

        //subscriber1 subscribes, takes one element and cancels
        StepVerifier sub1 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber2 subscribes, takes one element and cancels
        StepVerifier sub2 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber3 subscribes after all previous subscribers have cancelled
        StepVerifier sub3 = StepVerifier.create(flux.take(3)
                                                    .delaySubscription(Duration.ofSeconds(6)))
                                        .expectNext(0x0B64) //first measurement `0x0800` was already consumed by previous subscribers
                                        .expectNext(0x0504)
                                        .expectComplete()
                                        .verifyLater();

        sub1.verify();
        sub2.verify();
        sub3.verify();
    }

    /**
     * If you look closely, in previous exercises third subscriber was able to receive only two out of three
     * measurements. That's because used sink didn't remember history to re-emit all elements to new subscriber.
     * Modify solution from `open_24_7` so third subscriber will receive all measurements.
     */
    @Test
    public void blue_jeans() {
        //todo: enable autoCancel parameter to prevent sink from closing
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<Integer> flux = sink.asFlux();

        //don't change code below
        submitOperation(() -> {
            get_measures_readings().forEach(sink::tryEmitNext);
            submitOperation(sink::tryEmitComplete);
        });

        //subscriber1 subscribes, takes one element and cancels
        StepVerifier sub1 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber2 subscribes, takes one element and cancels
        StepVerifier sub2 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber3 subscribes after all previous subscribers have cancelled
        StepVerifier sub3 = StepVerifier.create(flux.take(3)
                                                    .delaySubscription(Duration.ofSeconds(6)))
                                        .expectNext(0x0800)
                                        .expectNext(0x0B64)
                                        .expectNext(0x0504)
                                        .expectComplete()
                                        .verifyLater();

        sub1.verify();
        sub2.verify();
        sub3.verify();
    }


    /**
     * There is a bug in the code below. May multiple producer threads concurrently generate data on the sink?
     * If yes, how? Find out and fix it.
     */
    @Test
    public void emit_failure() {
        //todo: feel free to change code as you need
        Sinks.Many<Integer> sink = Sinks.many().replay().all();

        for (int i = 1; i <= 50; i++) {
            int finalI = i;
            new Thread(() -> sink.tryEmitNext(finalI)).start();
        }

        //don't change code below
        StepVerifier.create(sink.asFlux()
                                .doOnNext(System.out::println)
                                .take(50))
                    .expectNextCount(50)
                    .verifyComplete();
    }
}
