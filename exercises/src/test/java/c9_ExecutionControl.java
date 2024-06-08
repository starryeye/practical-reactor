import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.NonBlocking;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * With multi-core architectures being a commodity nowadays, being able to easily parallelize work is important.
 * Reactor helps with that by providing many mechanisms to execute work in parallel.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#schedulers
 * https://projectreactor.io/docs/core/release/reference/#advanced-parallelizing-parralelflux
 * https://projectreactor.io/docs/core/release/reference/#_the_publishon_method
 * https://projectreactor.io/docs/core/release/reference/#_the_subscribeon_method
 * https://projectreactor.io/docs/core/release/reference/#which.time
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c9_ExecutionControl extends ExecutionControlBase {

    /**
     * You are working on smartphone app and this part of code should show user his notifications. Since there could be
     * multiple notifications, for better UX you want to slow down appearance between notifications by 1 second.
     * Pay attention to threading, compare what code prints out before and after solution. Explain why?
     */
    @Test
    public void slow_down_there_buckaroo() {
        long threadId = Thread.currentThread().getId();
        Flux<String> notifications = readNotifications()
                .delayElements(Duration.ofSeconds(1)) // 기본적으로 ParallelScheduler 로 수행된다. (publishOn 처럼 스레드 변경지점)
                .doOnNext(System.out::println)
                ;

        StepVerifier.create(notifications
                                    .doOnNext(s -> assertThread(threadId)))
                    .expectNextCount(5)
                    .verifyComplete();
    }

    private void assertThread(long invokerThreadId) {
        long currentThread = Thread.currentThread().getId();
        if (currentThread != invokerThreadId) {
            System.out.println("-> Not on the same thread");
        } else {
            System.out.println("-> On the same thread");
        }
        Assertions.assertTrue(currentThread != invokerThreadId, "Expected to be on a different thread");
    }

    /**
     * You are using free access to remote hosting machine. You want to execute 3 tasks on this machine, but machine
     * will allow you to execute one task at a time on a given schedule which is orchestrated by the semaphore. If you
     * disrespect schedule, your access will be blocked.
     * Delay execution of tasks until semaphore signals you that you can execute the task.
     */
    @Test
    public void ready_set_go() {

        Flux<String> tasks = tasks()
//                .concatMap(Function.identity()) // 이렇게 하면 반드시 순서 보장이 되긴함 c6, task_executor_again 참고 (대신, .expectNoEvent(Duration.ofMillis(2000)) 을 만족하지 못함)
                .concatMap(task -> task.delaySubscription(semaphore()));

        /**
         * semaphore() 가 정한 딜레이에 맞춰서 Mono<String> 를 subscribe 한다.
         *
         * 즉, Mono<String> 이 3개가 순서대로 concatMap 으로 전달되고
         * concatMap 으로 인해 1 개씩 전달된 item 이자 publisher 가 구독되기 시작하는데 구독할때 delaySubscription 으로 인해
         * semaphore 가 정한 딜레이에 맞춰서 subscribe 가 이루어진다.
         *
         * concatMap 에 대한 정확한 동작은 c6, task_executor_again 를 보면 이해가 빠를 것이다.
         */

        //don't change code below
        StepVerifier.create(tasks)
                    .expectNext("1")
                    .expectNoEvent(Duration.ofMillis(2000))
                    .expectNext("2")
                    .expectNoEvent(Duration.ofMillis(2000))
                    .expectNext("3")
                    .verifyComplete();
    }

    /**
     * Make task run on thread suited for short, non-blocking, parallelized work.
     * Answer:
     * - Which types of schedulers Reactor provides?
     * - What is their purpose?
     * - What is their difference?
     */
    @Test
    public void non_blocking() {
        Mono<Void> task = Mono.fromRunnable(() -> {
                                  Thread currentThread = Thread.currentThread();
                                  assert NonBlocking.class.isAssignableFrom(Thread.currentThread().getClass());
                                  System.out.println("Task executing on: " + currentThread.getName());
                              })
                .subscribeOn(Schedulers.parallel())
                              .then();
        /**
         * assert NonBlocking.class.isAssignableFrom(Thread.currentThread().getClass());
         * -> 현재 스레드가 NonBlocking 인터페이스를 구현한 스레드임을 확인하여, 작업이 NonBlocking 스레드에서 실행되고 있는지를 보장한다.
         */

        /**
         * https://projectreactor.io/docs/core/release/reference/#schedulers
         * 아래 scheduler 설명 외에도 virtualThread 에 관한 내용도 존재..
         *
         * Which types of schedulers Reactor provides?
         * What is their purpose?
         *	Schedulers.immediate(): 현재 스레드에서 즉시 작업을 실행한다.
         * 	Schedulers.single(): 하나의 재사용 가능한 스레드에서 작업을 실행한다. 단일 스레드 작업에 적합하다. 재사용하고 싶지 않다면, Schedulers.newSingle() 이용
         * 	Schedulers.elastic(): 각 작업마다 새로운 스레드를 생성하고(제한되지 않는 크기) 유휴 스레드를 재사용한다. I/O 바운드 작업에 적합하다. -> 사용하지 말고 boundedElastic 사용할 것을 추천
         * 	Schedulers.parallel(): 고정 크기의 워커 스레드 풀(일반적으로 CPU 코어 수만큼)을 사용한다. CPU 바운드 작업에 적합하다.
         * 	Schedulers.boundedElastic(): 블로킹 I/O 바운드 작업을 위해 제한된 크기의 탄력적인 스레드 풀을 생성한다.
         * 	Schedulers.fromExecutorService(...): 커스텀 ExecutorService를 사용하여 작업을 스케줄링할 수 있다.
         *
         * What is their difference?
         * 스레드 관리:
         * 	Schedulers.immediate()는 현재 스레드에서 스위칭 없이 실행
         * 	Schedulers.single()는 전용 스레드 하나에서 실행
         * 	Schedulers.elastic()는 필요에 따라 동적으로 스레드를 생성하고 재사용하며 무한히 확장됨
         * 	Schedulers.parallel()는 병렬 계산을 위한 고정된 수의 스레드를 사용
         * 	Schedulers.boundedElastic()는 블로킹 작업을 위한 제한된 크기의 스레드 풀을 생성
         * 	Schedulers.fromExecutorService(...)는 사용자 제공 ExecutorService를 사용하여 커스텀 스레드 관리를 수행
         * 사용 사례:
         * 	Schedulers.immediate()는 컨텍스트 전환이 필요 없는 매우 짧은 작업에 적합
         * 	Schedulers.single()는 동일한 스레드에서 순차적으로 실행되어야 하는 작업에 유용
         * 	Schedulers.elastic() 대신 boundedElastic 을 사용
         * 	Schedulers.parallel()는 병렬 실행의 이점을 누릴 수 있는 CPU 바운드 작업에 적합
         * 	Schedulers.boundedElastic()는 더 많은 스레드가 필요하지만 제한된 성장을 요구하는 블로킹 I/O 바운드 작업에 적합
         * 	Schedulers.fromExecutorService(...)는 커스텀 실행 전략을 위한 유연성을 제공
         */

        StepVerifier.create(task)
                    .verifyComplete();
    }

    /**
     * Make task run on thread suited for long, blocking, parallelized work.
     * Answer:
     * - What BlockHound for?
     */
    @Test
    public void blocking() {

        // BlockHound 를 위해 -XX:+AllowRedefinitionToAddDeleteMethods 를 VM options 에 추가 해줘야함
        BlockHound.install(); //don't change this line

        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall)
                              .subscribeOn(Schedulers.boundedElastic())
                              .then();

        /**
         * 긴 블로킹 작업에 적합한 스레드
         */

        StepVerifier.create(task)
                    .verifyComplete();
    }

    /**
     * Adapt code so tasks are executed in parallel, with max concurrency of 3.
     */
    @Test
    public void free_runners() {

        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall)
                .subscribeOn(Schedulers.boundedElastic())
                .then()
                ;

        Flux<Void> taskQueue = Flux.just(task, task, task)
                .flatMap(Function.identity(), 3) // todo, concurrency 의미
                ;

         // todo, 정답과 비교해보기
//        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall);
//        Flux<Void> taskQueue = Flux.just(task, task, task)
//                .flatMap(Function.identity())
//                .subscribeOn(Schedulers.boundedElastic())
//                ;


        //don't change code below
        Duration duration = StepVerifier.create(taskQueue)
                                        .expectComplete()
                                        .verify();

        Assertions.assertTrue(duration.getSeconds() <= 2, "Expected to complete in less than 2 seconds");
    }

    /**
     * Adapt the code so tasks are executed in parallel, but task results should preserve order in which they are invoked.
     */
    @Test
    public void sequential_free_runners() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks()
                .flatMap(Function.identity());
        ;

        //don't change code below
        Duration duration = StepVerifier.create(tasks)
                                        .expectNext("1")
                                        .expectNext("2")
                                        .expectNext("3")
                                        .verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 1, "Expected to complete in less than 1 seconds");
    }

    /**
     * Make use of ParallelFlux to branch out processing of events in such way that:
     * - filtering events that have metadata, printing out metadata, and mapping to json can be done in parallel.
     * Then branch in before appending events to store. `appendToStore` must be invoked sequentially!
     */
    @Test
    public void event_processor() {
        //todo: feel free to change code as you need
        Flux<String> eventStream = eventProcessor()
                .filter(event -> event.metaData.length() > 0)
                .doOnNext(event -> System.out.println("Mapping event: " + event.metaData))
                .map(this::toJson)
                .concatMap(n -> appendToStore(n).thenReturn(n));

        //don't change code below
        StepVerifier.create(eventStream)
                    .expectNextCount(250)
                    .verifyComplete();

        List<String> steps = Scannable.from(eventStream)
                                      .parents()
                                      .map(Object::toString)
                                      .collect(Collectors.toList());

        String last = Scannable.from(eventStream)
                               .steps()
                               .collect(Collectors.toCollection(LinkedList::new))
                               .getLast();

        Assertions.assertEquals("concatMap", last);
        Assertions.assertTrue(steps.contains("ParallelMap"), "Map operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelPeek"), "doOnNext operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelFilter"), "filter operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelRunOn"), "runOn operator not used");
    }

    private String toJson(Event n) {
        try {
            return new ObjectMapper().writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }
}
