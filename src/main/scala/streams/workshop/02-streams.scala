package streams.workshop

import zio._
import zio.clock.Clock
import zio.stream._
import java.nio.file.Path
import java.io.IOException

import zio.console.Console

object StreamTypes {
  // 1. A stream that emits integers and cannot fail.
  val s1: ZStream[Any, Nothing, Int] = ???

  // 2. A stream that emits strings and can fail with throwables.
  val s2: ZStream[Any, Throwable, String] = ???

  // 3. A stream that emits no elements.
  val s3: ZStream[Any, Nothing, Nothing] = ???

  // 4. A stream that requires access to the console, can fail with
  // string errors and emits integers.
  val s4: ZStream[console.Console, String, Int] = ???
}

object ConstructingStreams {
  // One of the main design goals of ZStream is extremely good interoperability
  // for various scenarios. A consequence of that is a wide range of constructors
  // for streams. In this section, we will survey various ways to construct
  // streams.

  // 1. Construct a stream with a single integer, '42'.
  val single: ZStream[Any, Nothing, Int] = ZStream(42)

  // 2. Construct a stream with three characters, 'a', 'b', 'c'.
  val chars: ZStream[Any, Nothing, Char] = ZStream('a', 'b', 'c')

  // 3. Construct a stream from an effect that reads a line from the user.
  val readLine: ZStream[Console, IOException, String] = ZStream.fromEffect(console.getStrLn)

  // 4. Construct a stream that fails with the string "boom".
  val failed: ZStream[Any, String, Nothing] = ZStream.fail("boom")

  // 5. Create a stream that extracts the Clock from the environment.
  val clockStream: ZStream[Clock, Nothing, Clock] = ZStream.environment[Clock]

  // 6. Construct a stream from an existing list of numbers:
  val ns: List[Int]                       = List.fill(100)(1)
  val nStream: ZStream[Any, Nothing, Int] = ZStream.fromIterable(ns)

  // 7. Using repeatEffectOption, repeatedly read lines from the user
  // until the string "EOF" is entered.
  val allLines =
    // ZIO effect fails with None = Stream stop, fails with some = Stream error
    ZStream
      .repeatEffectOption(console.getStrLn.mapError(Option(_)).flatMap {
        case "EOF" => ZIO.fail[Option[IOException]](None)
        case o     => ZIO.succeed(o)
      })

  // 8. Drain an iterator using `repeatEffectOption`.
  def drainIterator[A](iterator: Iterator[A]): ZStream[Any, Throwable, A] =
    ZStream.repeatEffectOption(
      ZIO(iterator.hasNext).mapError(Option(_)).flatMap { hasData =>
        if (hasData) ZIO(iterator.next()).mapError(Option(_))
        else ZIO.fail[Option[Throwable]](None)
      }
    )

  // the ??? is an effect that should not throw exceptions, if it does, it will appear as a defect and not appear as a typed error
  //ZIO.effectSuspendTotal(???)

  // 9. Using ZStream.unwrap, unwrap the stream embedded in this effect.
  val wrapped: Task[ZStream[Any, Nothing, Int]] = ZIO(ZStream(1, 2, 3))
  val unwrapped: ZStream[Any, Throwable, Int]   = ZStream.unwrap(wrapped)

  // 10. Using ZStream.unfold, create a stream that emits the numbers 1 to 10.
  // NOTE: this is co-recursion (start from a seed and produce a bigger structure)
  val oneTo10: ZStream[Any, Nothing, Int] = ZStream.unfold(1)(next =>
    if (next > 10) None
    else Some((next, next + 1))
  )

  // 11. Do the same with unfoldM, but now sleep for `n` milliseconds after
  // emitting every number `n`.
  import zio.duration._
  val oneTo10WithSleeps: ZStream[Clock, Nothing, Int] = ZStream.unfoldM(1)(next =>
    if (next > 10) ZIO.succeed(None)
    else UIO(Some((next, next + 1))) <* ZIO.sleep(next.millis)
  )

  // 12. Read an array in chunks using unfoldChunkM.
  def readArray[A](array: Array[A], chunkSize: Int): ZStream[Any, Nothing, A] =
    ZStream.unfoldChunkM(0) { idx =>
      UIO {
        if (idx >= array.length) None
        else {
          val chunkToEmit = Chunk.fromArray(array.slice(idx, math.min(idx + chunkSize, array.length)))
          Some((chunkToEmit, idx + chunkSize))
        }
      }
    }

// This is less efficient
//    ZStream.unfoldChunkM(array) { array =>
//      if (array.isEmpty) UIO(None)
//      else UIO(Option((Chunk.fromArray(array.take(chunkSize)), array.drop(chunkSize))))
//    }

  // 13. Read an array in chunks using paginateM. You'll need to use
  // `flattenChunks` in this exercise.
  def readArray2[A](array: Array[A], chunkSize: Int): ZStream[Any, Nothing, A] =
    ZStream
      .paginateM(0) { idx =>
        UIO {
          Chunk.fromArray(array.slice(idx, math.min(idx + chunkSize, array.length))) -> (
            if ((idx + chunkSize) > array.length) None
            else Some(idx + chunkSize)
          )
        }
      }
      .flattenChunks
// This is less efficient
//    ZStream
//      .paginateM(array) { array =>
//        if (array.isEmpty) UIO((Chunk.empty, None))
//        else UIO((Chunk.fromArray(array.take(chunkSize)), Some(array.drop(chunkSize))))
//      }
//      .flattenChunks

  // 14. Implement `tail -f`-like functionality using ZStream. (come back to this later)
  def tail(path: Path, chunkSize: Int): ZStream[Clock, Throwable, Byte] =
    ???
}

object TransformingStreams {
  // In this section, we will cover operators for synchronous transformations
  // on streams. These are the bread and butter of stream operators, so we'll
  // use them quite a bit as we create stream processing programs.

  // 1. Transform a stream of ints to a stream of strings.
  val warmup: ZStream[Any, Nothing, String] = ZStream(1, 2, 3).map(_.toString)

  // 2. Multiply every integer of the stream using a coefficient
  // retrieved effectfully.
  val currentCoefficient: ZIO[random.Random, Nothing, Double] =
    random.nextDoubleBetween(0.5, 0.85)
  val multiplied: ZStream[random.Random, Nothing, Double] =
    ZStream.range(1, 10).mapM(i => currentCoefficient.map(_ * i))

  // 3. Split every string to separate lines in this stream.
  val lines: ZStream[Any, Nothing, String] =
    ZStream("line1\nline2", "line3\n\nline4\n").mapConcat(_.split("\n"))
//    ZStream("line1\nline2", "line3\n\nline4\n")
//      .flatMap(line => ZStream.fromIterable(line.split("\n")))

  // 4. Print out a JSON array from the following stream of strings
  // using intersperse and tap.
  val data: ZStream[console.Console, Nothing, String] = ZStream("ZIO", "ZStream", "ZSink")
    .map(e => s"'$e'")
    .intersperse("[", ",", "]")
    .tap(console.putStr(_))

  // 5. Read a 100 even numbers from the Random generator.
  val hundredEvens: ZStream[random.Random, Nothing, Int] =
    ZStream.repeatEffect(random.nextInt).filter(_ % 2 == 0).take(100)

  // 6. Read 10 lines from the user, but skip the first 3.
  val linesDropped: ZStream[console.Console, IOException, String] =
    ZStream.repeatEffect(console.getStrLn).drop(3)

  // 7. Read 10 lines from the user, but drop all lines until the user
  // writes the word "START". Don't include "START" in the stream.
  // dropUntil will drop the line that matches (so START wont appear here)
  val finalEx: ZStream[console.Console, IOException, String] =
    ZStream.repeatEffect(console.getStrLn).dropUntil(_ == "START").take(10)

  // 7. Using ZStream#++, print a message to the user, then read a line.
  val printAndRead: ZStream[console.Console, IOException, String] =
    ZStream.fromEffect(console.putStrLn("Enter text") *> console.getStrLn) ++ printAndRead

  // 8. Split the following stream of CSV data to individual tokens
  // that are emitted on a 2-second schedule
  // Note that ZStream.flatMap should not be on the hot path (it is really used for acquiring resources)
  import zio.duration._
  val scheduled: ZStream[clock.Clock, Nothing, String] =
    ZStream("DDOG,12.7,12.8", "NET,10.1,10.2").flatMap { rawInput =>
      val split     = rawInput.split(",")
      val stockName = split.head
      val stockData = split.tail

      ZStream
        .fromIterable(stockData.map(d => s"$stockName:$d"))
        .schedule(Schedule.fixed(2.seconds))
    }

  // 9. Terminate this infinite stream as soon as a `Left` is emitted.
  val terminateOnLeft: ZStream[random.Random, Nothing, Either[Unit, Unit]] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Left(()) else Right(()))).flatMap {
      case Left(_) => ZStream.empty
      case right   => ZStream(right)
    }
//    ZStream.repeatEffect(random.nextBoolean.map(if (_) Left(()) else Right(()))).takeWhile(_.isRight)

  // 10. Do the same but with `Option` and `None`:
  val terminateOnNone: ZStream[random.Random, Nothing, Unit] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Some(()) else None)).collectWhileSome
//    ZStream.repeatEffect(random.nextBoolean.map(if (_) Some(()) else None)).collect { case Some(x) => x }
}
