import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * It's time to do some data manipulation!
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
public class c2_TransformingSequence extends TransformingSequenceBase {

    /***
     * Your task is simple:
     *  Increment each number emitted by the numerical service
     */
    @Test
    public void transforming_sequence() {
        Flux<Integer> numbersFlux = numerical_service()
                .map(i -> ++i)
                ;

        //StepVerifier is used for testing purposes
        //ignore it for now, or explore it independently
        StepVerifier.create(numbersFlux)
                    .expectNext(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                    .verifyComplete();
    }

    /***
     * Transform given number sequence to:
     *   - ">": if given number is greater than 0
     *   - "=": if number is equal to 0
     *   - "<": if given number is lesser then 0
     */
    @Test
    public void transforming_sequence_2() {
        Flux<Integer> numbersFlux = numerical_service_2();

        Flux<String> resultSequence = numbersFlux.map(
                integer -> {
                    if (integer > 0) {
                        return ">";
                    } else if (integer == 0) {
                        return "=";
                    } else {
                        return "<";
                    }
                }
        );

        //don't change code below
        StepVerifier.create(resultSequence)
                    .expectNext(">", "<", "=", ">", ">")
                    .verifyComplete();
    }

    /**
     * `object_service()` streams sequence of Objects, but if you peek into service implementation, you can see
     * that these items are in fact strings!
     * Casting using `map()` to cast is one way to do it, but there is more convenient way.
     * Remove `map` operator and use more appropriate operator to cast sequence to String.
     */
    @Test
    public void cast() {
        Flux<String> numbersFlux = object_service()
                .cast(String.class)
                ;


        StepVerifier.create(numbersFlux)
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    /**
     * `maybe_service()` may return some result.
     * In case it doesn't return any result, return value "no results".
     */
    @Test
    public void maybe() {
        Mono<String> result = maybe_service()
                .defaultIfEmpty("no results")
                ;

        StepVerifier.create(result)
                    .expectNext("no results")
                    .verifyComplete();
    }

    /**
     * Reduce the values from `numerical_service()` into a single number that is equal to sum of all numbers emitted by
     * this service.
     */
    @Test
    public void sequence_sum() {
        Mono<Integer> sum = numerical_service()
                .reduce(0, Integer::sum)
                ; // reduce 는 stream 에도 존재하는 연산자이다. 최종 결과만 방출한다.


        //don't change code below
        StepVerifier.create(sum)
                    .expectNext(55)
                    .verifyComplete();
    }

    /***
     *  Reduce the values from `numerical_service()` but emit each intermediary number
     *  Use first Flux value as initial value.
     */
    @Test
    public void sum_each_successive() {
        Flux<Integer> sumEach = numerical_service()
                .scan(Integer::sum)
                ;
        /**
         * scan 연산자는 스트림의 각 요소를 처리하여 누적값을 계산한다.
         * 각 단계에서 누적값과 현재 요소를 사용하여 새로운 누적값을 생성하고 내보낸다.
         * 즉, scan은 각 단계의 중간 누적 결과를 방출하는 것. 이는 최종 결과만 방출하는 reduce와 다른 느낌임
         */
        // public final Flux<T> scan(BiFunction<T, T, T> accumulator) // 초기값 없이 첫번째 요소를 초기값으로 사용함
        // public final <A> Flux<A> scan(A initial, BiFunction<A, ? super T, A> accumulator) // 초기값을 정해주고 초기값 부터 내보냄
        // -> 두번째 방식의 예시 .scan(0, (accumulator, current) -> accumulator + current) 이렇게 하면 0 부터 내보내게되긴하고, accumulator 가 누적 값이라 생각하면된다.

        StepVerifier.create(sumEach)
                    .expectNext(1, 3, 6, 10, 15, 21, 28, 36, 45, 55)
                    .verifyComplete();
    }

    /**
     * A developer who wrote `numerical_service()` forgot that sequence should start with zero, so you must prepend zero
     * to result sequence.
     *
     * Do not alter `numerical_service` implementation!
     * Use only one operator.
     */
    @Test
    public void sequence_starts_with_zero() {
        Flux<Integer> result = numerical_service()
                //todo: change this line only
                ;

        StepVerifier.create(result)
                    .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    .verifyComplete();
    }
}
