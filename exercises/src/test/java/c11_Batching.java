import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Another way of controlling amount of data flowing is batching.
 * Reactor provides three batching strategies: grouping, windowing, and buffering.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#advanced-three-sorts-batching
 * https://projectreactor.io/docs/core/release/reference/#which.window
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c11_Batching extends BatchingBase {

    /**
     * To optimize disk writing, write data in batches of max 10 items, per batch.
     */
    @Test
    public void batch_writer() {

        Flux<Void> dataStream = dataStream()
                .buffer(10) // 10 개씩 모아서 방출
                .flatMap(this::writeToDisk);

        //do not change the code below
        StepVerifier.create(dataStream)
                    .verifyComplete();

        Assertions.assertEquals(10, diskCounter.get());
    }

    /**
     * You are implementing a command gateway in CQRS based system. Each command belongs to an aggregate and has `aggregateId`.
     * All commands that belong to the same aggregate needs to be sent sequentially, after previous command was sent, to
     * prevent aggregate concurrency issue.
     * But commands that belong to different aggregates can and should be sent in parallel.
     * Implement this behaviour by using `GroupedFlux`, and knowledge gained from the previous exercises.
     */
    @Test
    public void command_gateway() {
        //todo: implement your changes here
        Flux<Void> processCommands = inputCommandStream()
                .groupBy(Command::getAggregateId)
                .flatMap(groupedFlux -> groupedFlux.concatMap(command -> sendCommand(command)));

        /**
         * groupBy 파라미터로 전달된 함수를 바탕으로 동일한 결과값이면 같은 그룹으로 묶어서 flux 로 만든다.
         * 즉, 동일한 결과끼리 묶어서 여러개의 flux(GroupFlux) 를 방출한다. (각 flux 내부에는 분류된 Command 가 있겠지..)
         *
         * 해당 flux 를 flatMap 을 통해 병렬적으로 수행한다.
         * 각 command 는 concatMap 을 통해 sendCommand 를 수행하도록 해준다.
         */

        //do not change the code below
        Duration duration = StepVerifier.create(processCommands)
                .verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 3, "Expected to complete in less than 3 seconds");
    }


    /**
     * You are implementing time-series database. You need to implement `sum over time` operator. Calculate sum of all
     * metric readings that have been published during one second.
     */
    @Test
    public void sum_over_time() {
        Flux<Long> metrics = metrics()
                //todo: implement your changes here
                .take(10);

        StepVerifier.create(metrics)
                    .expectNext(45L, 165L, 255L, 396L, 465L, 627L, 675L, 858L, 885L, 1089L)
                    .verifyComplete();
    }
}
