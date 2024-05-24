import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * It's time introduce some resiliency by recovering from unexpected events!
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.errors
 * https://projectreactor.io/docs/core/release/reference/#error.handling
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c7_ErrorHandling extends ErrorHandlingBase {

    /**
     * You are monitoring hearth beat signal from space probe. Heart beat is sent every 1 second.
     * Raise error if probe does not any emit heart beat signal longer then 3 seconds.
     * If error happens, save it in `errorRef`.
     */
    @Test
    public void houston_we_have_a_problem() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        Flux<String> heartBeat = probeHeartBeatSignal()
                .timeout(Duration.ofSeconds(3)) // 3 초 안에 item 이 도착하지 않으면 에러이다.
                .doOnError(errorRef::set) // 에러를 errorRef 에 저장한다.
                ;

        StepVerifier.create(heartBeat)
                    .expectNextCount(3)
                    .expectError(TimeoutException.class) // timeout 에 걸리면 발생하는 예외는 TimeoutException.class 이다.
                    .verify();

        Assertions.assertTrue(errorRef.get() instanceof TimeoutException);
    }

    /**
     * Retrieve currently logged user.
     * If any error occurs, exception should be further propagated as `SecurityException`.
     * Keep original cause.
     */
    @Test
    public void potato_potato() {
        Mono<String> currentUser = getCurrentUser()
                .onErrorMap(SecurityException::new) // 예외 발생시, 다른 예외로 전환시킨다.
                //use SecurityException
                ;

        StepVerifier.create(currentUser)
                    .expectErrorMatches(e -> e instanceof SecurityException &&
                            e.getCause().getMessage().equals("No active session, user not found!"))
                    .verify();
    }

    /**
     * Consume all the messages `messageNode()`.
     * Ignore any failures, and if error happens finish consuming silently without propagating any error.
     */
    @Test
    public void under_the_rug() {
        Flux<String> messages = messageNode()
                .onErrorResume(e -> Mono.empty()); // 예외가 도착하면 Mono.empty(바로 완료처리) 로 변경하여 방출한다.

        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .verifyComplete();
    }

    /**
     * Retrieve all the messages `messageNode()`,and if node suddenly fails
     * use `backupMessageNode()` to consume the rest of the messages.
     */
    @Test
    public void have_a_backup() {

        Flux<String> messages = messageNode()
                .onErrorResume(e -> backupMessageNode()); // 예외 발생 시, backupMessageNode() 로 대체한다.

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2", "0x3", "0x4")
                    .verifyComplete();
    }

    /**
     * Consume all the messages `messageNode()`, if node suddenly fails report error to `errorReportService` then
     * propagate error downstream.
     */
    @Test
    public void error_reporter() {

        Flux<String> messages = messageNode()
                .onErrorResume(
                        e -> errorReportService(e) // 예외 발생 시, errorReportService() 를 수행
                                .then(Mono.error(e)) // 그 이후 발생된 예외를 그대로 downstream 으로 방출
                );

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .expectError(RuntimeException.class)
                    .verify();
        Assertions.assertTrue(errorReported.get());
    }

    /**
     * Execute all tasks from `taskQueue()`. If task executes
     * without any error, commit task changes, otherwise rollback task changes.
     * Do don't propagate any error downstream.
     */
    @Test
    public void unit_of_work() {
        Flux<Task> taskFlux = taskQueue()
                .flatMap( // flatMap 으로 item(task) 에 접근
                        task -> task.execute() // task 를 수행한다.
                                .then(task.commit()) // onComplete 시 task.commit 수행
                                .onErrorResume(task::rollback) // onError 시 rollback 수행
                                .thenReturn(task) // onComplete 시 task 를 리턴하여 StepVerifier 에서 무언가 검증한다.
                )
                ;

        StepVerifier.create(taskFlux)
                    .expectNextMatches(task -> task.executedExceptionally.get() && !task.executedSuccessfully.get())
                    .expectNextMatches(task -> task.executedSuccessfully.get() && task.executedSuccessfully.get())
                    .verifyComplete();
    }

    /**
     * `getFilesContent()` should return files content from three different files. But one of the files may be
     * corrupted and will throw an exception if opened.
     * Using `onErrorContinue()` skip corrupted file and get the content of the other files.
     */
    @Test
    public void billion_dollar_mistake() {

        Flux<String> content = getFilesContent()
                .flatMap(Function.identity())
                .onErrorContinue(
                        (throwable, o) -> System.out.println("Error occur.. from = " + o + ", error message = " + throwable.getMessage())
                )
//                .onErrorContinue(
//                        new BiConsumer<Throwable, Object>() {
//                            @Override
//                            public void accept(Throwable throwable, Object o) {
//                                System.out.println("Error occur.. from = " + o + ", error message = " + throwable.getMessage());
//                            }
//                        }
//                )
                ;

        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    /**
     * Quote from one of creators of Reactor: onErrorContinue is my billion-dollar mistake. `onErrorContinue` is
     * considered as a bad practice, its unsafe and should be avoided.
     *
     * <p>See:</p>
     * <ul>
     *   <li><a href="https://nurkiewicz.com/2021/08/onerrorcontinue-reactor.html">onErrorContinue</a></li>
     *   <li><a href="https://devdojo.com/ketonemaniac/reactor-onerrorcontinue-vs-onerrorresume">onErrorContinue vs onErrorResume</a></li>
     *   <li><a href="https://bsideup.github.io/posts/daily_reactive/where_is_my_exception/">Where is my exception?</a></li>
     * </ul>
     *
     * Your task is to implement `onErrorContinue()` behaviour using `onErrorResume()` operator,
     * by using knowledge gained from previous lessons.
     */
    @Test
    public void resilience() {

//        Flux<String> content = getFilesContent()
//                .flatMap(Function.identity())
//                .doOnError(e -> System.out.println("Error occur.. from = " + e))
//                .onErrorResume(e -> Mono.empty())
//                ;

        // 위 코드와 다른점?
        // -> 위 코드는 flatMap 내부에 onErrorResume 이 있는게 아니고 외부에 있다. 그래서 onErrorResume 을 수행하면 바로 전체 stream 이 Mono.empty 로 대체된다.
        // 반면, 아래 코드는 flatMap 내부에 onErrorResume 이 있으므로 file 한개에 대해 한개가 예외가 발생하면 한개만 완료되고 나머지는 정상 수행된다.
        // flatMap, onErrorResume 의 마블 다이어그램을 보면 쉽게 와닿을 수 있음
        Flux<String> content = getFilesContent()
                .flatMap(
                        file -> file.doOnError(e -> System.out.println("Error occur.. from = " + e))
                                .onErrorResume(e -> Mono.empty())
                );

        /**
         * - onErrorContinue 메서드의 문제점
         *      - 에러를 무시하고 계속 진행하기 때문에 디버깅이 어렵고, 예기치 않은 동작을 유발할 수 있다.
         * - onErrorContinue 대신.. onErrorResume 메서드를 사용하자
         *      - 에러가 발생하면 대체 스트림을 제공하여 안전하게 에러를 처리한다.
         * 적절한 예외 처리 예외를 명확하게 기록하고, 대체 데이터를 제공하여 시스템의 안정성을 유지하는 것이 중요하다.
         *
         * onErrorContinue 메서드의 사용을 피하고,
         * 더 안전하고 유지보수하기 쉬운 코드 작성을 위해 onErrorResume을 사용하여
         * 에러 처리 로직을 명확히 하고, 시스템의 안정성과 신뢰성을 높이도록 하자.
         */

        //don't change below this line
        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    /**
     * You are trying to read temperature from your recently installed DIY IoT temperature sensor. Unfortunately, sensor
     * is cheaply made and may not return value on each read. Keep retrying until you get a valid value.
     */
    @Test
    public void its_hot_in_here() {
        Mono<Integer> temperature = temperatureSensor()
                .retry()
                ; // 정상 item 이 도착하여 Mono 가 complete 될 때 까지 retry

        StepVerifier.create(temperature)
                    .expectNext(34)
                    .verifyComplete();
    }

    /**
     * In following example you are trying to establish connection to database, which is very expensive operation.
     * You may retry to establish connection maximum of three times, so do it wisely!
     * FIY: database is temporarily down, and it will be up in few seconds (5).
     */
    @Test
    public void back_off() {

        Mono<String> connection_result = establishConnection() // 최초에 연결 시도 시, 실패고 5초 후에 연결 시도 시 성공하도록 구현되어있는 publisher 인듯..
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
                ;

        StepVerifier.create(connection_result)
                    .expectNext("connection_established")
                    .verifyComplete();
    }

    /**
     * You are working with legacy system in which you need to read alerts by pooling SQL table. Implement polling
     * mechanism by invoking `nodeAlerts()` repeatedly until you get all (2) alerts. If you get empty result, delay next
     * polling invocation by 1 second.
     */
    @Test
    public void good_old_polling() {

        Flux<String> alerts = nodeAlerts()
                .repeatWhenEmpty(it -> it.delayElements(Duration.ofSeconds(1))) // nodeAlerts 에서 empty 로 오면 수행하는 연산자.. empty 로 오면 1초 후에 다시 수행하도록 한다.
                .repeat() // empty 가 아니더라도 nodeAlerts 를 반복 수행시킨다.
                ;

        //don't change below this line
        StepVerifier.create(alerts.take(2))
                    .expectNext("node1:low_disk_space", "node1:down")
                    .verifyComplete();
    }

    public static class SecurityException extends Exception {

        public SecurityException(Throwable cause) {
            super(cause);
        }
    }
}
