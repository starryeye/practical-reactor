import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In this chapter we will learn difference between hot and cold publishers,
 * how to split a publisher into multiple and how to keep history so late subscriber don't miss any updates.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactor.hotCold
 * https://projectreactor.io/docs/core/release/reference/#which.multicasting
 * https://projectreactor.io/docs/core/release/reference/#advanced-broadcast-multiple-subscribers-connectableflux
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c12_Broadcasting extends BroadcastingBase {

    /**
     * Split incoming message stream into two streams, one contain user that sent message and second that contains
     * message payload.
     */
    @Test
    public void sharing_is_caring() throws InterruptedException {
        Flux<Message> messages = messageStream()
                .publish()
                .refCount(2)
                ;
        /**
         * publish()..
         * Flux 를 ConnectableFlux 로 바꾸어주고, 여러 구독자에게 동일한 item 을 제공해주는 hot publisher 로 사용되게끔 해준다.
         *
         * ConnectableFlux 의 연산자..
         * - autoConnect() : 지정된 수의 subscriber 가 connect 될 때 sequence 를 시작하며,
         *      모든 subscriber 가 해제되면 sequence 를 중지하고, 다시 지정된 수의 subscriber 가 connect 되면 기존 sequence 를 이어서 시작한다. (이어서 시작)
         * - refCount() : 지정된 수의 subscriber 가 connect 될 때 sequence 를 시작하며,
         *      모든 subscriber 가 해제되면 sequence 를 중지하고, 다시 지정된 수의 subscriber 가 connect 되면 새로운 sequence 를 시작한다. (다시 처음부터 시작)
         *
         * 참고
         * share() : publish().refCount(1)의 단축형
         *      최소 한 명의 subscriber 가 connect 되면 sequence 를 시작하고, 모든 subscriber 가 해제되면 sequence 를 중지한다.
         *
         *
         * 참고.. (내 느낌)
         * map() 에서 sink 를 사용해서 좀더 이벤트나 item 을 좀더 세밀하게 방출할 수 있도록 한게 handle() 연산자 라면..
         * publish(), refCount() 의 그러한 버전이 Sinks.many().multicast() 인듯 하다..
         */

        //don't change code below
        Flux<String> userStream = messages.map(m -> m.user);
        Flux<String> payloadStream = messages.map(m -> m.payload);

        CopyOnWriteArrayList<String> metaData = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> payload = new CopyOnWriteArrayList<>();

        userStream.doOnNext(n -> System.out.println("User: " + n)).subscribe(metaData::add);
        payloadStream.doOnNext(n -> System.out.println("Payload: " + n)).subscribe(payload::add);

        Thread.sleep(3000);

        Assertions.assertEquals(Arrays.asList("user#0", "user#1", "user#2", "user#3", "user#4"), metaData);
        Assertions.assertEquals(Arrays.asList("payload#0", "payload#1", "payload#2", "payload#3", "payload#4"),
                                payload);
    }

    /**
     * Since two subscribers are interested in the updates, which are coming from same source, convert `updates` stream
     * to from cold to hot source.
     * Answer: What is the difference between hot and cold publisher? Why does won't .share() work in this case?
     */
    @Test
    public void hot_vs_cold() {

        Flux<String> updates = systemUpdates()
                .publish()
                .autoConnect(1)
                ;

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext(n -> System.out.println("subscriber 1 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                    .verifyComplete();

        //subscriber 2
        StepVerifier.create(updates.take(4).doOnNext(n -> System.out.println("subscriber 2 got: " + n)))
                    .expectNext("DISK_SPACE_LOW", "OOM_DETECTED", "CRASHED", "UNKNOWN")
                    .verifyComplete();
    }

    /**
     * In previous exercise second subscriber subscribed to update later, and it missed some updates. Adapt previous
     * solution so second subscriber will get all updates, even the one's that were broadcaster before its
     * subscription.
     */
    @Test
    public void history_lesson() {

        Flux<String> updates = systemUpdates()
                .cache()
                ;
        /**
         * 참고
         * systemUpdates()
         *  그대로 놔두어도 정답 처리 된다. 기본적으로 Flux 는 cold publisher 이기 때문이다.
         * systemUpdates().publish().refCount(1) (= systemUpdates().share())
         *  cold 에서 hot 으로 변경하였지만, refCount(1) 으로 인해 cold 처럼 동작된다.
         * systemUpdates().cache()
         *  cache 가 중간에서 지금까지 발행(첫번째 subscriber 에게 발행)된 item 들을 캐싱하고 있다가
         *  새로운 subscriber 에게 빠르게 전달 후 다시 원본 publisher 에 접근하여 이후 item 들을 발행 받는다.
         *  -> 두번째 subscriber 에게는 5 개의 item 중.. 3개는 delay 없이 한번에 발행되는 모습을 확인 가능
         */

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext(n -> System.out.println("subscriber 1 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                    .verifyComplete();

        //subscriber 2
        StepVerifier.create(updates.take(5).doOnNext(n -> System.out.println("subscriber 2 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW", "OOM_DETECTED")
                    .verifyComplete();
    }

}
