package streams.workshop

import java.io.FileReader
import java.nio.file.{ Path, Paths }

import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration.{ Duration, _ }
import zio.stream._
import zio.{ App => _, _ }

object ControlFlow extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  def readFile(path: Path, bytesToRead: Int): ZStream[Any, Throwable, Char] =
    ZStream
      .bracket(ZIO(new FileReader(path.toAbsolutePath.toFile)))(fr => UIO(fr.close()))
      .flatMap(fr =>
        ZStream.fromEffectOption(
          for {
            arr       <- UIO(Array.ofDim[Char](bytesToRead))
            charsRead <- Task(fr.read(arr)).mapError(Option(_))
            res       <- if (charsRead == -1) ZIO.fail(None) else ZIO.succeed(Chunk.fromArray(arr))
          } yield res
        )
      )
      .flattenChunks

  // 1. Write a stream that reads bytes from one file,
  // then prints "Done reading 1", then reads another bytes
  // from another file.
  def bytesFromHereAndThere(here: Path, there: Path, bytesAmount: Int): ZStream[console.Console, Throwable, Char] =
    readFile(here, bytesAmount).tap(c => console.putStr(c.toString)) ++ ZStream
      .fromEffect(
        console.putStrLn("\nDone reading 1")
      )
      .drain ++ readFile(there, bytesAmount).tap(c => console.putStr(c.toString))

  // 2. What would be the difference in output between these two streams?
  // Hello World Done printing!
  val output1 =
    ZStream("Hello", "World").tap(console.putStrLn(_)).drain ++
      ZStream.fromEffect(console.putStrLn("Done printing!")).drain

  // ++ is very different from *>
  // *> is essentially upstream.flatMap(_ => insideStream) for ZStream, meaning for each element emitted upstream,
  // you will run the insideStream so it will be Hello Done printing! then World Done printing!
  val output2 =
    ZStream("Hello", "World").tap(console.putStrLn(_)).drain *>
      ZStream.fromEffect(console.putStrLn("Done printing!")).drain

  // The only sane way to sequence with *> is to have an upstream with a single element otherwise you'll get the behavior
  // from output2

  // 3. Read 4 paths from the user. For every path input, read the file in its entirety
  // and print it out. Once done printing it out, log the file name in a `Ref` along with
  // its character count. Once the stream is completed, print out the all the files and
  // counts.
  val read4Paths: ZStream[???, ???, ???] = ???
}

object StreamErrorHandling extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Modify this stream, which is under our control, to survive transient exceptions.
  trait TransientException { self: Exception => }
  def query: RIO[random.Random, Int] = random.nextIntBetween(0, 11).flatMap { n =>
    if (n < 2) Task.fail(new RuntimeException("Unrecoverable"))
    else if (n < 6) Task.fail(new RuntimeException("recoverable") with TransientException)
    else Task.succeed(n)
  }

  val queryResults: ZStream[random.Random with clock.Clock, Throwable, Int] =
    ZStream.repeatEffect(query.retry(Schedule.fixed(50.milliseconds)))

  // 2. Apply retries to the transformation applied during this stream.
  type Lookup = Has[Lookup.Service]
  object Lookup {
    trait Service {
      def lookup(i: Int): Task[String]
    }

    def lookup(i: Int): RIO[Lookup, String] = ZIO.accessM[Lookup](_.get.lookup(i))

    def live: ZLayer[Any, Nothing, Lookup] =
      ZLayer.succeed { i =>
        if (i < 5) Task.fail(new RuntimeException("Lookup failure"))
        else Task.succeed("Lookup result")
      }
  }

//  val queryResultsTransformed: ZStream[random.Random with Lookup, Throwable, Int] =
//    ZStream
//      .repeatEffect(query)
//      .mapM(i => Lookup.lookup(i).map((i, _))) ?

  // 3. Switch to another stream once the source fails in this stream.
  val failover: ZStream[Any, Nothing, Int] =
    ZStream(ZIO.succeed(1), ZIO.fail("Boom")).mapM(identity).orElse(ZStream.succeed(2))

  // 4. Do the same, but when the source fails, print out the failure and switch
  // to the stream specified in the failure.
  case class UpstreamFailure[R, E, A](reason: String, backup: ZStream[R, E, A])
  val failover2: ZStream[console.Console, UpstreamFailure[Any, Nothing, Int], Int] =
    ZStream(ZIO.succeed(1), ZIO.fail(UpstreamFailure("Malfunction", ZStream(2))))
      .mapM(identity)
      .catchAll {
        case UpstreamFailure(reason, backup) =>
          ZStream.fromEffect(console.putStrLn(reason)).drain ++ backup
      }

  // 5. Implement a simple retry combinator that waits for the specified duration
  // between attempts.
  // this will leak due to the internals of ZStream (cannot have recursive streams)
  def retryStream[R, E, A](stream: ZStream[R, E, A], interval: Duration): ZStream[R with clock.Clock, E, A] =
//    stream.either.flatMap {
//      case Left(_)      => ZStream.fromEffect(ZIO.sleep(interval)).drain ++ retryStream(stream, interval)
//      case Right(value) => ZStream.succeed(value)
//    }
    stream.catchAll(_ => ZStream.fromEffect(ZIO.sleep(interval)).drain ++ retryStream(stream, interval))

  // 6. Measure the memory usage of this stream:
  val alwaysFailing = retryStream(ZStream.fail("Boom"), 1.millis)

  //retryStream(alwaysFailing, 1.second).runDrain.unsafeRun

  // 7. Surface typed errors as value-level errors in this stream using `either`:
  // we won't see 3 being printed because either uses catchAll which forces you to switch over to another stream
  // you cannot use either to retry/resume a stream, for now: all errors on a stream will end it
  val eithers = ZStream(ZIO.succeed(1), ZIO.fail("Boom"), ZIO.succeed(3)).mapM(identity).either

  // 8. Use catchAll to restart this stream, without re-acquiring the resource.
  val resourceStr = ZStream.bracket(console.putStrLn("Acquiring"))(_ => console.putStrLn("Releasing"))
  val subsection =
    resourceStr.flatMap { _ =>
      val s =
        ZStream(ZIO.succeed(1), ZIO.fail("Boom"), ZIO.succeed(2)).mapM(_.flatMap(i => console.putStrLn(i.toString)))
      s.catchAll(e => ZStream.fromEffect(console.putStrLn(s"Failed over because of $e!")))
    }

//  Acquiring
//  1
//  Failed over because of Boom!
//  Releasing
//  Note: You won't see 2 because catchAll will use a fallback stream
//  subsection.runDrain.unsafeRun
}

object Concurrency extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Create a stream that prints every element from a queue.
  val queuePrinter: ZStream[console.Console, Nothing, String] =
    ZStream.fromEffect(Queue.bounded[String](10)).flatMap { q =>
      ZStream.repeatEffect(q.offer("Hello")).take(10).drain ++
        ZStream.fromQueue(q).tap(console.putStrLn(_))
    }

  //queuePrinter.runDrain.unsafeRun

  // 2. Run the queuePrinter stream in one fiber, and feed it elements
  // from another fiber. The latter fiber should run a stream that reads
  // lines from the user.
  val queuePipeline: ZIO[Console with Clock, Nothing, Unit] = {
    def queuePrinter2(q: Queue[String]): ZStream[console.Console, Nothing, String] =
      ZStream.fromQueue(q).tap(console.putStrLn(_))

    for {
      q  <- Queue.bounded[String](10)
      f1 <- q.offer("Hello").repeat(Schedule.fixed(5.seconds)).fork
      f2 <- queuePrinter2(q).runDrain.fork
      _  <- Fiber.awaitAll(List(f1, f2))
    } yield ()
  }

  //queuePipeline.unsafeRun

  // 3. Introduce the ability to signal end-of-stream on the queue pipeline.
  val queuePipelineWithEOS: ZIO[console.Console with clock.Clock, Nothing, Unit] = {
    def queuePrinter2(q: Queue[Option[String]]): ZStream[console.Console, Nothing, String] =
      ZStream
        .fromQueue(q)
        .collectWhileSome // will shutdown the q when None
        .tap(console.putStrLn(_))

    for {
      q  <- Queue.bounded[Option[String]](10)
      f1 <- (q.offer(Option("Hello")).repeat(Schedule.recurs(10) && Schedule.fixed(1.second)) *> q.offer(None)).fork
      f2 <- queuePrinter2(q).runDrain.fork
      _  <- Fiber.awaitAll(List(f1, f2))
      _  <- console.putStrLn("All fibers have been joined")
    } yield ()
  }

  // 4. Introduce the ability to signal errors on the queue pipeline.
  val queuePipelineWithEOSAndErrors: ZIO[Console with Clock, String, Unit] = {
    def consumer(q: Queue[Either[Option[String], Int]]) =
      ZStream
        .fromQueue(q)
        .collectWhileM {
          case Left(Some(err)) => ZIO.fail(err)
          case Right(elem)     => ZIO.succeed(elem)
          // we purposely don't handle Left(None) as it indicates the end of the stream
        }
        .tap(i => console.putStrLn(i.toString))

    for {
      q <- Queue.bounded[Either[Option[String], Int]](10)
      f1 <- (q
             .offer(Right(1))
             .repeat(Schedule.recurs(10) && Schedule.fixed(1.second)) *> q.offer(Left(Some("boom"))) *> q.offer(
             Left(None)
           )).fork
      _ <- consumer(q).runDrain // consumer runs in the foreground
      _ <- f1.interrupt
      _ <- console.putStrLn("All fibers have been joined")
    } yield ()
  }

  // 5. Combine the line reading stream with the line printing stream using
  // ZStream#drainFork
  //   * Drains the provided stream in the background for as long as this stream is running.
  //   * If this stream ends before `other`, `other` will be interrupted. If `other` fails,
  //   * this stream will fail with that error.
  // Note: This will block on the foreground stream and only then propagate errors from the background stream
  // This is because getStrLn is not interruptible
  val backgroundDraining = {
    def consumer(q: Queue[Either[Option[String], String]]) =
      ZStream
        .fromQueue(q)
        .tap(i => console.putStrLn("From Queue: " + i))
        .collectWhileM {
          case Left(Some(err)) => ZIO.fail(err)
          case Right(elem)     => ZIO.succeed(elem)
          // we purposely don't handle Left(None) as it indicates the end of the stream
        }

    for {
      q <- Queue.bounded[Either[Option[String], String]](10)
      backgroundPrinter = (consumer(q) ++ ZStream.fromEffect(console.putStrLn("Ending the stream")))
        .mapError(new RuntimeException(_))
      _ <- ZStream
            .repeatEffect(console.getStrLn)
            .drainFork(backgroundPrinter)
            .map {
              case "EOF"     => Left(None)
              case "boom"    => Left(Some("boom"))
              case otherwise => Right(otherwise)
            }
            .tap(q.offer)
            .take(4)
            .runDrain
    } yield ()
  }

  // 6. Prove to yourself that drainFork terminates the background stream
  // when the foreground ends by:
  //   a. creating a stream that uses `ZStream.bracketExit` to print on interruption and
  //      delaying it with ZStream.never
  //   b. draining it in the background of another stream with drainFork
  //   c. making the foreground stream end after a 1 second delay
  //val drainForkTerminates: ??? = ???

  // 7. Simplify our queue pipeline with `ZStream#buffer`.
  //val buffered: ??? = ???

  // 8. Open a socket server, and wait and read from incoming connections for
  // 30 seconds.
  val timedSocketServer = ZStream
    .fromSocketServer(9001)
    .flatMapPar(32)(connection =>
      connection.read
        .transduce(ZTransducer.utf8Decode)
        .tap(console.putStrLn(_))
        .interruptAfter(30.seconds)
        .drain ++ ZStream.fromEffect(connection.close())
    )

  // 9. Create a stream that emits once after 30 seconds. Apply interruptAfter(10.seconds)
  // and haltAfter(10.seconds) to it and note the difference in behavior.
  // halt will allow the effect to continue and will not interrupt it
  // interruptAfter will interrupt the running computation and forcefully stop it
  // haltAfter is good for non-idempotent requests
  val interruptVsHalt: ZStream[Console with Clock, Nothing, Unit] =
    ZStream.fromEffect(ZIO.sleep(30.seconds) *> console.putStrLn("hello")).haltAfter(10.seconds)

  // 10. Use timeoutTo to avoid the delay embedded in this stream and replace the last
  // element with "<TIMEOUT>"
  // timeoutTo switches the stream if it does not produce a value after d duration.
  val csvData = (ZStream("symbol,price", "AAPL,500") ++
    ZStream.fromEffect(clock.sleep(30.seconds)).drain ++
    ZStream("AAPL,501")).timeoutTo(5.seconds)(ZStream("<TIMEOUT>"))

//  csvData.tap(console.putStrLn(_)).runDrain.unsafeRun

  // 11. Generate 3 random prices with a 5 second delay between each price
  // for every symbol in the following stream.
  val symbols = ZStream("AAPL", "DDOG", "NET")
    .scheduleElements(Schedule.recurs(3) && Schedule.fixed(5.seconds))
    .mapM(sym => random.nextDouble.map(price => sym -> price))
    .tap(kv => console.putStrLn(kv.toString()))

  // 12. Regulate the output of this infinite stream of records to emit records
  // on exponentially rising intervals, from 50 millis up to a maximum of 5 seconds.
  def pollSymbolQuote(symbol: String): URIO[random.Random, (String, Double)] =
    random.nextDoubleBetween(0.1, 0.5).map((symbol, _))
  val regulated = ZStream
    .repeatEffect(pollSymbolQuote("V"))
    // configures the intervals between elements
    .schedule(Schedule.exponential(50.millis) || Schedule.spaced(5.seconds)) // || picks the shorter schedule
    .tap(t => console.putStrLn(t.toString()))

  // 13. Introduce a parallel db writing operator in this stream. Handle up to
  // 5 records in parallel.
  case class Record(value: String)
  def writeRecord(record: Record): RIO[clock.Clock with console.Console, Unit] =
    console.putStrLn(s"Writing ${record}") *> clock.sleep(1.second)

  val dbWriter =
    ZStream
      .repeatEffect(random.nextString(5).map(Record))
      .mapMPar(5)(writeRecord)
//      .runDrain
//      .unsafeRun

  // 14. Test what happens when one of the parallel operations encounters an error.
  val whatHappens =
    ZStream(
      clock.sleep(5.seconds).onExit(ex => console.putStrLn(s"First element exit: ${ex.toString}")),
      clock.sleep(5.seconds).onExit(ex => console.putStrLn(s"Second element exit: ${ex.toString}")),
      clock.sleep(1.second) *> ZIO.fail("Boom")
    ).mapMPar(3)(identity)
      .runDrain
      .catchAllCause(cause => console.putStrLn(s"WHATS GOING ON\n${cause.prettyPrint}"))
      .unsafeRun
}

object StreamComposition extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Merge these two streams.
  val one    = ZStream.repeatWith("left", Schedule.fixed(1.second).jittered)
  val two    = ZStream.repeatWith("right", Schedule.fixed(500.millis).jittered)
  val merged = one.merge(two)

  // 2. Merge `one` and `two` above with the third stream here:
  val three          = ZStream.repeatWith("middle", Schedule.fixed(750.millis).jittered)
  val threeMerges    = ZStream.mergeAll(n = 3, outputBuffer = 16)(one, two, three)
  val altThreeMerges = ZStream.mergeAllUnbounded()(one, two, three, one.map(s => s"r$s"))
  altThreeMerges.tap(console.putStrLn(_))

  // 3. Emulate drainFork with mergeTerminateLeft.
  val emulation: ZStream[Clock with Console, Nothing, Long] =
    ZStream
      .bracket(Queue.bounded[Long](16))(_.shutdown)
      .flatMap { queue =>
        val writer = ZStream.repeatEffectWith(clock.nanoTime >>= queue.offer, Schedule.fixed(1.second))
        val reader = ZStream.fromQueue(queue).tap(l => console.putStrLn(l.toString))
        // reader is in foreground
        // writer is in background
        // if the reader terminates then the writer should terminate
        reader.mergeTerminateLeft(writer.drain)
      }

  // 4. Sum the numbers between the two streams, padding the right one with zeros.
  val left  = ZStream.range(1, 8)
  val right = ZStream.range(1, 5)

  // left and right define padding if one is longer than the other then it will use padding
  // if the left side is missing, the left function will be called with information from the right side and vice versa
  // if elements are there from both sides then the both function will be called
  val zipped =
    left.zipAllWith(right)(left = rightSide => rightSide, right = leftSide => leftSide)(both = (l, r) => l + r)

  // 5. Regulate the output of this stream by zipping it with another stream that ticks on a fixed schedule.
  val highVolume =
    ZStream
      .range(1, 100)
      .forever
      .zipLeft(ZStream.repeatEffectWith(ZIO.unit, Schedule.fixed(1.second)))
      .tap(i => console.putStrLn(i.toString))

  // 6. Truncate this stream by zipping it with a `Take` stream that is fed from a queue.
//  val toTruncate = ZStream.range(1, 100).forever ?

  // 7. Perform a deterministic merge of these two streams in a 1-2-2-1 pattern.
  val cats = ZStream("bengal", "shorthair", "chartreux").forever
  val dogs = ZStream("labrador", "poodle", "boxer").forever

  // 8. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter.
  val inner: ZStream[Blocking, Throwable, Byte] = ZStream.fromFile(Paths.get("DDOG.csv"))
  val echoingFiles: ZStream[Blocking with Console, Throwable, String] =
    ZStream
      .repeatEffect(console.getStrLn.mapError(identity[Throwable]))
      .flatMapPar(32)(_ =>
        // allow upto 32 of the inner streams in parallel
        inner.transduce(ZTransducer.utf8Decode).tap(console.putStrLn(_))
      )

  echoingFiles.runDrain.unsafeRun

  // 9. Run a variable number of background streams (according to the parameter) that
  // perform monitoring. They should interrupt the foreground fiber doing the processing.
  def doMonitor(id: Int) =
    ZStream.repeatEffectWith(
      random.nextIntBetween(1, 11).flatMap { i =>
        if (i < 8) console.putStrLn(s"Monitor OK from ${id}")
        else Task.fail(new RuntimeException(s"Monitor ${id} failed!"))
      },
      Schedule.fixed(2.seconds).jittered
    )

  val doProcessing =
    ZStream.repeatEffectWith(
      random.nextDoubleBetween(0.1, 0.5).map(f => s"Sample: ${f}"),
      Schedule.fixed(1.second).jittered
    )

  def runProcessing(nMonitors: Int) = {
    val _ = nMonitors
    doProcessing ?
  }

  // 10. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter, but only keeps the latest 5 files open.
  val echoingFilesLatest = ???
}
