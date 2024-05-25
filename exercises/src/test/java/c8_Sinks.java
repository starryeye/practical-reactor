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
         *
         * Sinks.one() 을 사용하면 단일 값을 방출하는 Mono 타입의 Sink가 생성
         *
         * Sinks의 목적:
         * 	- 수동으로 신호를 생성하고 이를 구독자에게 전달하는 기능을 제공
         * 	- Sinks.One 은 단일 값을 방출하며, Sinks.Many 는 다수의 값을 방출
         * Thread Safety:
         * 	- Reactor 의 Sinks 는 다중 스레드 환경에서 안전하게 사용할 수 있도록 설계됨
         * 	- tryEmitXXX API는 병렬 호출 시 빠르게 실패하고, emitXXX API는 제공된 EmissionFailureHandler를 통해 경쟁 상태를 처리할 수 있음
         * Sink 유형:
         * 	- many().multicast(): 여러 구독자에게 새롭게 푸시된 데이터를 전송. 구독자들은 구독 후에 푸시된 데이터만 수신
         * 	- many().unicast(): 첫 번째 구독자가 구독하기 전에 푸시된 데이터를 버퍼링하고, 이후에 한 명의 구독자에게만 데이터를 전송
         * 	- many().replay(): 지정된 히스토리 크기만큼의 데이터를 캐시하여 새 구독자에게 재생하고, 이후 새 데이터를 계속 푸시
         * 	- one(): 단일 요소를 방출하며, 이를 구독자에게 전달
         * 	- empty(): 단일 완료 신호 또는 오류 신호를 방출
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
         * 	- 여러 값을 방출하고, 이를 여러 구독자에게 "동일하게" 전달한다. -> Hot stream 이 되도록한다. (해당 테스트에서 구독자는 StepVerifier 하나이다.)
         * 	- 참고로 Mono, Flux 은 기본적으로 Cold stream 이다.
         * onBackpressureBuffer():
         * 	- 백프레셔 상황에서 값을 버퍼에 저장하도록 설정. 백프레셔는 데이터 생산 속도가 소비 속도보다 빠를 때 발생하는 문제
         * 	- onBackpressureBuffer() 는 이러한 상황에서 방출되지 않은 값을 버퍼에 저장하여 구독자가 데이터를 처리할 수 있을 때까지 유지.
         * 	- 이는 데이터 유실을 방지하고, 소비자가 데이터를 처리할 수 있을 때까지 값을 버퍼링하는 방식으로 시스템을 안정적으로 유지될 수 있도록 한다.
         * 	- 마블 다이어그램을 보면 이해가 빠를 것이다.
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

        /**
         * 문제의 내용에 대해 테스트코드가 이상해서 테스트 코드 자체를 바꿔봤다.
         */

        // multicast 로 여러 구독자에게 동일한 item 을 동시에 발행, directBestEffort 로 과거 발행된 item 캐싱 없이 hot publisher 로 동작되도록함
        Sinks.Many<Integer> sink = Sinks.many().multicast().directBestEffort();

        Flux<Integer> measurements = sink.asFlux();
        submitOperation(() -> { // 5초 후에 동작됨

            List<Integer> measures_readings = get_measures_readings(); //don't change this line, 이 메서드가 레거시 코드 동작 부분이다.(동기 blocking)

            measures_readings.forEach(sink::tryEmitNext);
            sink.tryEmitComplete();
        });

        // 구독자 1: 모든 값을 수신
        StepVerifier.create(measurements)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(4))
                .expectNext(0x0800, 0x0B64, 0x0504) // publisher 에서 발행되기 전에 구독하였으므로 모든 item 을 받음
                .verifyComplete();

        // 구독자 2: 나중에 구독하여 값을 수신 받지 못함
        StepVerifier.create(measurements.delaySubscription(Duration.ofSeconds(6))) // publisher 에서 모든 item 과 이벤트가 발행된 이후 구독
                .expectSubscription()
                .verifyComplete(); // hot stream 에서 이미 완료 이벤트가 발행된 이후 구독하면 바로 완료 이벤트가 도착한다.
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
