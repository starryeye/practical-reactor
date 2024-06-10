import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c6_CombiningPublishers extends CombiningPublishersBase {

    /**
     * Goal of this exercise is to retrieve e-mail of currently logged-in user.
     * `getCurrentUser()` method retrieves currently logged-in user
     * and `getUserEmail()` will return e-mail for given user.
     *
     * No blocking operators, no subscribe operator!
     * You may only use `flatMap()` operator.
     */
    @Test
    public void behold_flatmap() {
        Hooks.enableContextLossTracking(); //used for testing - detects if you are cheating!

        Mono<String> currentUserEmail = getCurrentUser()
                .flatMap(this::getUserEmail);

        //don't change below this line
        StepVerifier.create(currentUserEmail)
                    .expectNext("user123@gmail.com")
                    .verifyComplete();
    }

    /**
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute them.
     *
     * Answer:
     * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
     */
    @Test
    public void task_executor() {
        Flux<Void> tasks = taskExecutor() // Flux<Mono<Void>>
                .flatMap(Function.identity()); // Mono<Void> 를 수행하고 Flux 로 통합한다.
        // Mono<Void> 를 수행하면 아이템은 없고 완료 이벤트만 downstream 으로 전달된다..
        // 최종 결과 타입도 Mono<Void> 에서 아이템을 전달하지 않기 때문에 Flux<Void> 가 된다.
        // 참고 - identity 의 의미 : 전달된 아이템인 Mono<Void> 를(flatMap 첫번째 타입 파라미터) 그대로 두번째 타입 파라미터인 publisher 로 사용하겠다는 의미

        // 비동기 처리: 각 Mono<Void>는 별도의 스레드(여기서는 Schedulers.parallel() 를 사용)에서 실행된다.
        // 따라서 각 작업의 완료 시점은 서로 다를 수 있으며, flatMap 은 이러한 비동기 완료 이벤트들을 동기화하여 하나의 Flux 로 통합한다.

        // 동작 논리
        // StepVerifier 에 의해 Flux<Void> 가 수행되고
        // Flux<Void> 는 Mono<Void> 를 수행시킨 결과를 받는 stream 이다.
        // Mono<Void> 는 taskExecutor 내부에 subscribeOn 에 의해 Schedulers.parallel() 가 수행한다.

        /**
         * Mono 의 flatMap
         * 단일 값 Mono 에서 시작하여, 각 값에 대해 새로운 Mono 를 생성 (최종 요소 개수 1)
         *
         * Flux 의 flatMap
         * Flux(N 개의 요소) 에서 시작하여, 각 값에 대해 새로운 publisher(M 개의 요소) 를 생성 (최종 요소 개수 N * M)
         *
         * 공통점
         * flatMap 은 호출 스레드 응답 스레드가 다를 수 있어서 비동기로 동작한다고 표현가능
         */

        //don't change below this line
        StepVerifier.create(tasks)
                    .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * `streamingService()` opens a connection to the data provider.
     * Once connection is established you will be able to collect messages from stream.
     *
     * Establish connection and get all messages from data provider stream!
     */
    @Test
    public void streaming_service() {
        Flux<Message> messageFlux = streamingService() // Mono<Flux<Message>>
                .flatMapMany(Function.identity()); // Flux<Message> 를 수행하면서 나오는 아이템과 이벤트를 방출한다.

        //don't change below this line
        StepVerifier.create(messageFlux)
                    .expectNextCount(10)
                    .verifyComplete();
    }


    /**
     * Join results from services `numberService1()` and `numberService2()` end-to-end.
     * First `numberService1` emits elements and then `numberService2`. (no interleaving)
     *
     * Bonus: There are two ways to do this, check out both!
     */
    @Test
    public void i_am_rubber_you_are_glue() {

//        Flux<Integer> numbersFirstWay = Flux.concat(numberService1(), numberService2());
        Flux<Integer> numbersFirstWay = numberService1().concatWith(numberService2());

        Flux<Integer> numbersSecondWay = Flux.mergeSequential(numberService1(), numberService2());


        //don't change below this line
        StepVerifier.create(numbersFirstWay)
                    .expectNext(1, 2, 3, 4, 5, 6, 7)
                    .verifyComplete();
        StepVerifier.create(numbersSecondWay)
                .expectNext(1, 2, 3, 4, 5, 6, 7)
                .verifyComplete();
    }

    /**
     * Similar to previous task:
     *
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute each of them.
     *
     * Instead of flatMap() use concatMap() operator.
     *
     * Answer:
     * - What is difference between concatMap() and flatMap()?
     * - What is difference between concatMap() and flatMapSequential()?
     * - Why doesn't Mono have concatMap() operator?
     */
    @Test
    public void task_executor_again() {

        Flux<Void> tasks = taskExecutor()
                .log()
//                .flatMapSequential(Function.identity());
                .concatMap(Function.identity());

        /**
         * merge, concat, mergeSequential 3 개의 연산자 관계는
         * flatMap, concatMap, flatMapSequential 3 개의 연산자 관계와 굉장히 유사하다.
         * 순서없음, 순서있음, 순서있음
         * 병렬가능, 병렬안됨, 병렬가능
         *
         * 마블 다이어그램을 보면 이해가 잘 될 것이다.
         *
         *
         * 참고..
         * flatMapSequential 으로 하면 가끔 발행순서가 꼬이고..
         * concatMap 으로 하면 발행순서가 절대로 안꼬이는듯..
         * 마블 다이어그램에서 봤을땐.. 두개의 연산자에 대한 동작은 병렬이 되냐 안되냐지만 결과는 동일할 것으로 생각했는데 .. 아니네..? 이유는..? 아래 상세 동작 분석 참조
         *
         * 동작 분석 todo, 이거 다시 분석해야함..
         * flatMapSequential 으로 하면..
         * 1. StepVerifier 가 tasks 를 subscribe 한다.
         * 2. taskExecutor 의 Flux.range(1, 10) 에서 1 부터 10 까지의 숫자들이 순차적으로 발행된다. (main 스레드)
         * 3. taskExecutor 에서 map 으로 인해 Mono<Void> 라는 타입의 item 이.. 순차적으로 발행된다. (main 스레드) (그래서 타입이 Flux<Mono<Void>> 이다.)
         * 4. 병렬로 전달된 item 이 사실 publisher 였고 flatMapSequential 로 전달된 순서에 따라 item 자기 자신을 subscribe 하여 발행한다. (identity)
         * 5. 그런데.. item 이자 publisher 인 Mono<Void> 는 subscribeOn 으로 ParallelScheduler 에 의해 병렬적으로 수행된다.
         * 6. item (각 내부에 1 부터 10 까지 숫자가 담김) 숫자 순서대로 전달 되었지만, 거의 동시에 item 이 전달되었고 ParallelScheduler 로 멀티 스레드로 구독을 시작하기 때문에 살짝 순서가 꼬일 수 있다.
         * 7. Mono<Void> publisher 가 병렬적으로 수행되고 내부에서 또다른 item 을 발행하거나 하지 않으므로 외부로 onComplete 이벤트만 전달된다. (그래서 Mono<Void> 이다.)
         * 8. 그러한 병렬적으로 onComplete 들이 발행되는 publisher 가 Flux<Void> 인 것이다. (tasks)
         *
         * concatMap 으로 하면..
         * flatMapSequential 과 1 ~ 5 번이 동일하다. (4 번에서는 flatMapSequential 을 concatMap 으로 변경)
         * 6. item (각 내부에 1 부터 10 까지 숫자가 담김) 숫자 순서대로 전달 되었고, 1 번이 수행 ParallelScheduler 으로 수행되는데 이때 concatMap 이므로 한번에 하나의 item 만 subscribe 한다.
         * 즉, 1 번이 ParallelScheduler 로 수행되고 onComplete 발행이 되기 전까지는 절대로 2 번이 subscribe 될 수 없다.
         * 따라서, 1 부터 10 까지의 순서가 반드시 보장될 수 있는 것이다.
         */

        //don't change below this line
        StepVerifier.create(tasks)
                    .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    public void need_for_speed() {

        Flux<String> stocks = Flux.firstWithValue(getStocksGrpc(), getStocksRest());
        // 첫번째 아이템이 빨리 도착하는 스트림을 택한다.
        // 다른 스트림은 취소한다.

        //don't change below this line
        StepVerifier.create(stocks)
                    .expectNextCount(5)
                    .verifyComplete();
    }

    /**
     * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
     * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
     * `getStocksRest()`.
     */
    @Test
    public void plan_b() {

        Flux<String> stonks = getStocksLocalCache()
                .switchIfEmpty(getStocksRest());

        //don't change below this line
        StepVerifier.create(stonks)
                    .expectNextCount(6)
                    .verifyComplete();

        Assertions.assertTrue(localCacheCalled.get());
    }

    /**
     * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
     * switch to a second mailbox. Otherwise, read all messages from first mailbox.
     */
    @Test
    public void mail_box_switcher() {

        Flux<Message> myMail = mailBoxPrimary()
                .switchOnFirst(
                        (first, mailBoxPrimary)-> {
                            if (first.get().metaData.contains("spam")) {
                                return mailBoxSecondary();
                            } else {
                                return mailBoxPrimary;
                            }
                        }
                );

        //don't change below this line
        StepVerifier.create(myMail)
                    .expectNextMatches(m -> !m.metaData.equals("spam"))
                    .expectNextMatches(m -> !m.metaData.equals("spam"))
                    .verifyComplete();

        Assertions.assertEquals(1, consumedSpamCounter.get());
    }

    /**
     * You are implementing instant search for software company.
     * When user types in a text box results should appear in near real-time with each keystroke.
     *
     * Call `autoComplete()` function for each user input
     * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
     */
    @Test
    public void instant_search() {

        Flux<String> suggestions = userSearchInput()
                .switchMap(this::autoComplete)
                ; // Flux(userSearchInput) 가 아이템을 방출할 때마다 함수(autoComplete)를 통해 생성된 새로운 publisher 로 전환
        // flatMap 과 비슷한데 flatMap 은 새로운 publisher 로 전환하지 않고 publisher 를 계속 추가하는 방식이다.

        //don't change below this line
        StepVerifier.create(suggestions)
                    .expectNext("reactor project", "reactive project")
                    .verifyComplete();
    }


    /**
     * Code should work, but it should also be easy to read and understand.
     * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * If all operations have been executed successfully return boolean value `true`.
     */
    @Test
    public void prettify() {

        Mono<Boolean> successful = Mono.when(openFile())
                .then(writeToFile("0x3522285912341"))
                .then(closeFile())
                .thenReturn(true);

        //don't change below this line
        StepVerifier.create(successful)
                    .expectNext(true)
                    .verifyComplete();

        Assertions.assertTrue(fileOpened.get());
        Assertions.assertTrue(writtenToFile.get());
        Assertions.assertTrue(fileClosed.get());
    }

    /**
     * Before reading from a file we need to open file first.
     */
    @Test
    public void one_to_n() {

        Flux<String> fileLines = Mono.when(openFile())
                .thenMany(readFile());

        StepVerifier.create(fileLines)
                    .expectNext("0x1", "0x2", "0x3")
                    .verifyComplete();
    }

    /**
     * Execute all tasks sequentially and after each task have been executed, commit task changes.
     * Don't lose id's of committed tasks, they are needed to further processing!
     */
    @Test
    public void acid_durability() {

        Flux<String> committedTasksIds = tasksToExecute()
                .flatMapSequential( // solution 에서는 concatMap 을 사용함
                        task -> task.flatMap(
                                taskId -> commitTask(taskId)
                                        .thenReturn(taskId)
                        )
                );

        //don't change below this line
        StepVerifier.create(committedTasksIds)
                    .expectNext("task#1", "task#2", "task#3")
                    .verifyComplete();

        Assertions.assertEquals(3, committedTasksCounter.get());
    }


    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    public void major_merger() {

        Flux<String> microsoftBlizzardCorp = microsoftTitles()
                .mergeWith(blizzardTitles());
//        Flux<String> microsoftBlizzardCorp = Flux.merge(microsoftTitles(), blizzardTitles());

        //don't change below this line
        StepVerifier.create(microsoftBlizzardCorp)
                    .expectNext("windows12",
                                "wow2",
                                "bing2",
                                "overwatch3",
                                "office366",
                                "warcraft4")
                    .verifyComplete();
    }


    /**
     * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
     * manufacturer. You need both parts before you can produce a car.
     * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
     * slower part producer.
     * After both parts arrive connect them to a car.
     */
    @Test
    public void car_factory() {

        Flux<Car> producedCars = carChassisProducer()
                .zipWith(carEngineProducer(), Car::new);

//        Flux<Car> producedCars = Flux.zip(carChassisProducer(), carEngineProducer())
//                        .map(tuple -> new Car(tuple.getT1(), tuple.getT2()));

        //don't change below this line
        StepVerifier.create(producedCars)
                    .recordWith(ConcurrentLinkedDeque::new)
                    .expectNextCount(3)
                    .expectRecordedMatches(cars -> cars.stream()
                                                       .allMatch(car -> Objects.equals(car.chassis.getSeqNum(),
                                                                                       car.engine.getSeqNum())))
                    .verifyComplete();
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */

    //only read from sourceRef
    AtomicReference<String> sourceRef = new AtomicReference<>("X");

    public Mono<String> chooseSource() {
        return Mono.defer(
                () -> {
                    if(sourceRef.get() == "A") {
                        return sourceA(); //<- choose if sourceRef == "A"
                    } else if (sourceRef.get() == "B") {
                        return sourceB(); //<- choose if sourceRef == "B"
                    } else {
                        return Mono.empty(); //otherwise, return empty
                    }
                }
        );
    }

    @Test
    public void deterministic() {
        //don't change below this line
        Mono<String> source = chooseSource();

        sourceRef.set("A");
        StepVerifier.create(source)
                    .expectNext("A")
                    .verifyComplete();

        sourceRef.set("B");
        StepVerifier.create(source)
                    .expectNext("B")
                    .verifyComplete();
    }

    /**
     * Sometimes you need to clean up after your self.
     * Open a connection to a streaming service and after all elements have been consumed,
     * close connection (invoke closeConnection()), without blocking.
     *
     * This may look easy...
     */
    @Test
    public void cleanup() throws InterruptedException {
        BlockHound.install(); //don't change this line, blocking = cheating!
        // 실행을 위해서.. VM options 에.. "-XX:+AllowRedefinitionToAddDeleteMethods" 추가해줘야함

        Flux<String> stream = Flux.usingWhen(
                StreamingConnection.startStreaming(), //resource supplier -> supplies Flux from Mono
                Function.identity(),//resource closure  -> closure in this case is same as Flux completion
                tr -> StreamingConnection.closeConnection()//<-async complete, executes asynchronously after closure
        );
        // usingWhen
        // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#usingWhen-org.reactivestreams.Publisher-java.util.function.Function-java.util.function.Function-java.util.function.BiFunction-java.util.function.Function-


        // 이건 왜 안되지 (시간을 주면 되긴함, onFinally 가 onComplete 보다 늦어서 안될 때가 많은 것임..)
//        Flux<String> stream = StreamingConnection.startStreaming()
//                .flatMapMany(Function.identity())
////                .doFinally(signalType -> StreamingConnection.closeConnection().subscribe())
//                        .doOnComplete(() -> StreamingConnection.closeConnection().subscribe());



        //don't change below this line
        StepVerifier.create(stream)
                    .then(()-> Assertions.assertTrue(StreamingConnection.isOpen.get()))
                    .expectNextCount(20)
                    .verifyComplete();

//        Thread.sleep(1000);

        Assertions.assertTrue(StreamingConnection.cleanedUp.get());
    }
}
