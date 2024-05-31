import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

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
         * https://projectreactor.io/docs/core/release/reference/#sinks
         *
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

        /**
         * 사실, onBackpressureBuffer 를 사용해도 test 성공한다.
         *
         * multicast 때문에  Hot publisher 가 되어서..
         * item 발행 당시의 모든 구독자가 해당 item 을 모두 소비하면 그 item 은 사라진다. (onBackpressureBuffer 냐 directBestEffort 이냐 상관 없음)
         *
         * onBackpressureBuffer vs directBestEffort
         * 마블 다이어그램을 보면 바로 알 수 있는데..
         * onBackpressureBuffer 는 어떤 subscriber 가 구독 시점 이후에 발행된 item 을 모두 받는 개념이고
         * directBestEffort 는 어떤 subscriber 가 구독 시점이 아닌 item 요청(request) 시점 이후에 발행된 item 을 모두 받는 개념인 듯 하다.
         */

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

        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
        Flux<Integer> flux = sink.asFlux();

        /**
         * autoCancel 이 true 이면..
         * - 모든 구독자가 취소되면, 싱크가 내부 버퍼를 비우고 새로운 구독자를 받지 않는다. (기본 값)
         * autoCancel 이 false 이면..
         * - 모든 구독자가 취소되더라도, 싱크가 내부 버퍼를 유지하고 새로운 구독자를 받을 수 있다.
         * 버퍼를 비우지 않고 새로운 구독자를 받아들일 수 있는 것이 주요 차이점이다.
         *
         * 참고.
         * multicast 를 사용했기 때문에 Hot publisher 가 되어서..
         * item 발행 당시의 모든 구독자가 해당 item 을 모두 소비하면 그 item 은 사라진다.
         *
         * 즉, subscriber3 이 첫번째 item 을 받지 못한 이유는..
         * subscriber1, 2 이 publisher 를 구독하고 첫번째 item 을 소비한 이후에
         * subscriber3 이 구독을 하여 소비된 첫번째 item 은 못받게 된 것이다.
         * 여기서, publisher 의 autoCancel 옵션이 false 였기 때문에,
         * subscriber1, 2 가 모두 구독 취소 한 시점(6 초 이후)에, subscriber3 가 publisher 를 구독해도
         * 바로 onComplete 되지 않고 소비되지 않은 item 2 개를 받을 수 있는 것이다.
         *
         */

        //don't change code below
        submitOperation(() -> {
            get_measures_readings().forEach(sink::tryEmitNext);
            submitOperation(sink::tryEmitComplete);
        });

        //subscriber1 subscribes, takes one element and cancels
        StepVerifier sub1 = StepVerifier.create(Flux.merge(flux.take(1).log()))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber2 subscribes, takes one element and cancels
        StepVerifier sub2 = StepVerifier.create(Flux.merge(flux.take(1).log()))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber3 subscribes after all previous subscribers have cancelled
        StepVerifier sub3 = StepVerifier.create(flux.take(3).log()
                                                    .delaySubscription(Duration.ofSeconds(6))) // 6 초 후, 구독
                                        .expectNext(0x0B64) //first measurement `0x0800` was already consumed by previous subscribers
                                        .expectNext(0x0504)
                                        .expectComplete()
                                        .verifyLater();

        // StepVerifier 실행 설정을 모두 마친후 한번에 실행 시킨다.
        System.out.println("sub verify start");
        sub1.verify();
        System.out.println("sub1 verify end");
        sub2.verify();
        System.out.println("sub2 verify end");
        sub3.verify();
        System.out.println("sub3 verify end");

        /**
         * todo, 의문점..
         *  verify 는 각자가 동기 blocking 으로 publisher 를 구독후 수행되는 것으로 알고 있었다..
         *  그렇게 되면.. 모순이 발생한다. sub1 이 모두 종료되고 sub2 가 수행되므로 sub2 에서 첫번째 item 을 받지 못해야 정상이다..
         *  그런데.. 로그를 확인해보니.. "sub verity start" 보다 sub1, sub2 의 onSubscribe 로그가 먼저 찍힌다...
         *
         * verifyLater()
         * Trigger the subscription and prepare for verifications but doesn't block.
         * https://projectreactor.io/docs/test/release/api/reactor/test/StepVerifier.html#verifyLater--
         *
         * 아래는 AI 답변..
         * 구독의 시작
         * - verifyLater()를 사용했을 때, 실제 StepVerifier의 실행은 verify()가 호출될 때까지 지연됩니다.
         * 하지만 구독 자체는 verifyLater() 호출 시 바로 시작되므로, 각각의 StepVerifier 인스턴스가 생성될 때 구독이 시작됩니다.
         *
         * 검증의 순서와 동시성
         * - 모든 StepVerifier 인스턴스는 verify()가 호출될 때까지 대기합니다. verify() 호출은 동기적이고 순차적으로 실행되므로,
         * 각 검증은 이전 검증이 완료된 후에 실행됩니다. 하지만 이는 구독 시점에 영향을 미치지 않습니다.
         *
         * 로그의 출력 시점
         * - verifyLater()로 인해 구독이 시작되는 순간부터 관련 로그가 출력될 수 있습니다.
         * 이는 구독 로직이 verify() 호출과는 독립적으로 먼저 발생하기 때문입니다. 그래서 "sub verify start" 로그가 출력되기 전에
         * 구독 관련 로그(onSubscribe)가 출력될 수 있습니다.
         */
    }

    /**
     * If you look closely, in previous exercises third subscriber was able to receive only two out of three
     * measurements. That's because used sink didn't remember history to re-emit all elements to new subscriber.
     * Modify solution from `open_24_7` so third subscriber will receive all measurements.
     */
    @Test
    public void blue_jeans() {

        Sinks.Many<Integer> sink = Sinks.many().replay().all();
        Flux<Integer> flux = sink.asFlux();

        /**
         * .many().replay().all()
         * 구독자의 구독시점, item 요청시점과 전혀 상관 없이 그냥 cold publisher 로 동작하여
         * 모든 구독자는 동일한 item 을 제공 받는다.
         */

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
