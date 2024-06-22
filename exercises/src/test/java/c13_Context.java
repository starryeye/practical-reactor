import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Often we might require state when working with complex streams. Reactor offers powerful context mechanism to share
 * state between operators, as we can't rely on thread-local variables, because threads are not guaranteed to be the
 * same. In this chapter we will explore usage of Context API.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#context
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c13_Context extends ContextBase {

    /**
     * You are writing a message handler that is executed by a framework (client). Framework attaches a http correlation
     * id to the Reactor context. Your task is to extract the correlation id and attach it to the message object.
     */
    public Mono<Message> messageHandler(String payload) {

        return Mono.deferContextual(contextView -> { // defer 에서 contextView 를 파라미터로 넣어주는 연산자이다.
            String correlationId = contextView.get(HTTP_CORRELATION_ID);
            return Mono.just(new Message(correlationId, payload));
        });
    }

    @Test
    public void message_tracker() {
        //don't change this code
        Mono<Message> mono = messageHandler("Hello World!")
                .contextWrite(Context.of(HTTP_CORRELATION_ID, "2-j3r9afaf92j-afkaf"));

        StepVerifier.create(mono)
                    .expectNextMatches(m -> m.correlationId.equals("2-j3r9afaf92j-afkaf") && m.payload.equals(
                            "Hello World!"))
                    .verifyComplete();
    }

    /**
     * Following code counts how many times connection has been established. But there is a bug in the code. Fix it.
     */
    @Test
    public void execution_counter() {
        
        Mono<Void> repeat = Mono.deferContextual(ctx -> {
                    int i = ctx.get(AtomicInteger.class).incrementAndGet();
                    System.out.println("context = " + i);
                    return openConnection();
        })
//                .contextWrite(context -> context.put(AtomicInteger.class, new AtomicInteger(0))) // 덮어쓰기가 되어서 테스트 통과 안됨
                .contextWrite(Context.of(AtomicInteger.class, new AtomicInteger(0))) // 정답
                ;
        /**
         *
         * .contextWrite(context -> context.put(AtomicInteger.class, new AtomicInteger(0)))
         * .contextWrite(Context.of(AtomicInteger.class, new AtomicInteger(0)))
         * 왜 둘의 차이가 생기는지 모르겠음..
         * -> 공식문서 피셜..
         * https://projectreactor.io/docs/core/release/reference/#context
         * https://projectreactor.io/docs/core/release/api/reactor/util/context/Context.html#putAll-reactor.util.context.ContextView-
         *  Use put(Object key, Object value) to store a key-value pair, returning a new Context instance.
         *  You can also merge two contexts into a new one by using putAll(ContextView).
         *  -> put 은 확실히 덮어쓰기가 되는 듯하고, putAll 은 merge 를 수행한다는데 위 현상이 말이 되려면..
         *      기존의 context 에 동일한 Key 가 존재하면 병합 대상 context 를 버리는 방식이어야 한다..
         *      더이상 검색은 안되네..
         */

        StepVerifier.create(repeat.repeat(4))
                    .thenAwait(Duration.ofSeconds(10))
                    .expectAccessibleContext()
                    .assertThat(ctx -> {
                        assert ctx.get(AtomicInteger.class).get() == 5;
                    }).then()
                    .expectComplete().verify();
    }

    /**
     * You need to retrieve 10 result pages from the database.
     * Using the context and repeat operator, keep track of which page you are on.
     * If the error occurs during a page retrieval, log the error message containing page number that has an
     * error and skip the page. Fetch first 10 pages.
     */
    @Test
    public void pagination() {

        AtomicInteger pageWithError = new AtomicInteger(); // set this field when error occurs

        // 문제 원본
        Flux<Integer> results = getPage(0)
                .flatMapMany(Page::getResult)
                .repeat(10)
                .doOnNext(i -> System.out.println("Received: " + i));

        // 이렇게 하면 되지 않을까 해서 시도해봄..
//        Flux<Integer> results = Mono.deferContextual(contextView -> {
//            int pageNumber = contextView.get(AtomicInteger.class).incrementAndGet();
//            return getPage(pageNumber);
//        }).flatMapMany(Page::getResult)
//                        .doOnError(throwable -> {}) // 에러가 났을 경우 로깅 처리와 pageWithError 에 pageNumber(context) 를 저장 해줘야하는데 doOnError 연산자로는 할 수 없다..

        // 이렇게 하면 되지 않을까 해서 시도해봄.. 2 todo, 뭐가 잘못 된거지..
//        Flux<Integer> results = Mono.create(monoSink -> {
//
//            int pageNumber = monoSink.currentContext().get(AtomicInteger.class).incrementAndGet();
//
//            Flux<Integer> integerFlux = getPage(pageNumber)
//                    .flatMapMany(Page::getResult)
//                    .doOnError(throwable -> {
//                        pageWithError.set(pageNumber);
//                        System.out.println("Error: " + throwable.getMessage() + ", page: " + pageNumber);
//                    })
//                    .onErrorResume(throwable -> Flux.empty());
//
//            monoSink.success(integerFlux);
//        })
//                .flatMapMany(Function::identity) // Mono<Flux<Integer>> 에서 Flux<Integer> 를 수행하면서 나오는 아이템과 이벤트를 방출한다.
//                .repeat(10)
//                .contextWrite(Context.of(AtomicInteger.class, new AtomicInteger(0)));

        //don't change this code
        StepVerifier.create(results)
                    .expectNextCount(90)
                    .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get());
    }
}
