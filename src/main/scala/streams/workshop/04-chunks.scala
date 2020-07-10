package streams.workshop

import zio.{ App => _, _ }
import zio.stream._

object Chunks extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Create a chunk of integers from an array.
  val intChunk: Chunk[Int] =
    Chunk.fromArray(Array.fill(10)(10))

  // 2. Fold a chunk of integers into its sum.
  val sum: Int = Chunk(1, 2, 3, 4, 5).sum

  // 3. Fold a chunk of integers into its sum and print the
  // partial sums as it is folded.
  val sum2: URIO[console.Console, Int] =
//    URIO
//      .foreach(Chunk(1, 2, 3).scan(0)(_ + _)) { eachIntermediate =>
//        console.putStrLn(eachIntermediate.toString).as(eachIntermediate)
//      }
//      .map(_.last)
    Chunk(1, 2, 3).foldM(0)((acc, next) => console.putStrLn(acc.toString).as(acc + next))

  sum2.unsafeRun

  // 4. Copy the contents of a chunk to an array.
  val arr: Array[Int] = Chunk(1, 2, 3).toArray

  // 5. Incrementally build a chunk using a ChunkBuilder.
  val buildChunk: Chunk[Int] =
// This works too
//    List
//      .fill(10)(10)
//      .foldLeft(ChunkBuilder.make[Int]())((acc, next) => acc += next) // this is mutable
//      // doing chunkBuilder += someValue returns a mutated chunkBuilder
//      .result()
    {
      val builder = ChunkBuilder.make[Int]()
      List.fill(10)(10).foreach(builder += _)
      builder.result()
    }

  println(buildChunk)
}

class PrimitiveBoxing {
  // Compare the bytecode resulting from each test here.
  // compile your code, look into target/scala-2.x/classes
  // find the class you are looking for and use java -p
  // cd /Users/calvinlfer/IdeaProjects/stream-processing-with-scala/target/scala-2.13/classes/streams/workshop
  //  javap -c PrimitiveBoxing.class | bat
  def test0(): Int = {
    val vec = Vector(1, 2, 3)

    val i1 = vec(1)

    i1
  }

  def test1(): Int = {
    // internal representation keeps integers unboxed
    val chunk = Chunk(1, 2, 3)

    // using a generic accessor boxes them... (you will see BoxesRunTime.unboxToInt)
    val i1 = chunk(1)

    i1
  }

  def test2(): Int = {
    val chunk = Chunk(1, 2, 3)

    // this keeps them unboxed :)
    // You will see InterfaceMethod zio/Chunk.int:(ILscala/$less$colon$less;)I
    // If you want to write performance sensitive code, you will need to be specific and use the right accessor
    // chunk.map(...) will box them (using generic methods) - no way to avoid boxing here
    // You will have to use specialization (Itamar provides a great explanation of this)
    val i1 = chunk.int(1)

    // for performance
    // 1. convert to Array (incurs penalty for copying)
    // 2. Use a while loop and Chunk.byte/int/float, you can use map for whatever is specialized
    // 3. Use Chunk.bytes/ints/floats: Array[Byte/Int/Float]

    i1
  }
}

object ChunkedStreams extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Create a stream from a chunk.
  val intStream: Stream[Nothing, Int] = ZStream.fromChunk(Chunk(1, 2, 3))

  // 2. Create a stream from multiple chunks.
  val intStream2: Stream[Nothing, Int] = ZStream.fromChunks(Chunk(1), Chunk(2, 3))

  // 3. Consume the chunks of a multiple chunk stream and print them out.
  val printChunks: URIO[console.Console, Unit] =
//    intStream2.runCollect.flatMap(_.mapM_(i => console.putStrLn(i.toString)))
    intStream2.foreachChunk(chunk => console.putStrLn(chunk.toString()))

  // 4. Transform each chunk of integers to its sum and print it.
  val summedChunks: ZStream[console.Console, Nothing, Int] =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4), Chunk(5, 6))
      .mapChunks(chunk => Chunk.single(chunk.sum))
      .tap(sum => console.putStrLn(sum.toString))

  // 5. Compare the chunking behavior of mapChunksM and mapM.
  // This destroys the chunking structure on our Stream
  // Results in singleton chunks
  val printedNumbers =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4))
      .mapM(i => console.putStrLn(i.toString).as(i))
      .foreachChunk(c => console.putStrLn(c.toString()))

  // This preserves our chunking structure, you want to do this for high performance sensitive code
  // ideal chunk size = 16K as recommended by Itamar
  val printedNumbersChunk =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4))
      .mapChunksM(_.mapM(i => console.putStrLn(i.toString).as(i)))
      .foreachChunk(c => console.putStrLn(c.toString()))

  // 6. Compare the behavior of the following streams under errors.
  // why is it correct for filterM/mapM to destroy chunking structure?
  def faultyPredicate(i: Int): Task[Boolean] =
    if (i < 10) Task.succeed(i % 2 == 0)
    else Task.fail(new RuntimeException("Boom"))

  // prints
  //  2
  //  Chunk(2)
  //  8
  //  Chunk(8)
  // Exception
  val filteredStream =
    ZStream
      .fromChunks(Chunk(1, 2, 3, 8, 9, 10, 11))
      .filterM(faultyPredicate)
      .tap(i => console.putStrLn(i.toString))
      .foreachChunk(c => console.putStrLn(c.toString()))
//      .unsafeRun

  // prints nothing since we filterM on a Chunk and it fails before proceeding to the tap
  // filterM crashes when processing the entire Chunk and will not emit elements downstream,
  // mapM will emit everything that passes downstream
  // i.e. it processes a chunk of elements before moving to the next step unlike mapM which processes
  // a single element
  val filteredChunkStream =
    ZStream
      .fromChunks(Chunk(1, 2, 3, 8, 9, 10, 11))
      .mapChunksM(_.filterM(faultyPredicate))
      .tap(i => console.putStrLn(i.toString))
      .foreachChunk(c => console.putStrLn(c.toString()))
//      .unsafeRun

  // 7. Re-chunk a singleton chunk stream into chunks of 4 elements.
  // rechunk using chunkN
  val rechunked =
    ZStream
      .fromChunks(List.fill(8)(Chunk(1)): _*)
      .chunkN(4)
      .foreachChunk(c => console.putStrLn(c.toString()))

  // 8. Build a stream of longs from 0 to Int.MaxValue + 3.
  val longs =
//    ZStream
//      .fromIterable(
//        Iterable.range(0L, Int.MaxValue.toLong + 3L)
//      ) //java.lang.IllegalArgumentException: More than Int.MaxValue elements.
    // lifts all the elements into Chunk (strict so it will be in memory)
    // don't lift very-large or infinite collections using fromIterable due to strictness
//      .mapChunksM(ch => console.putStrLn(ch.toString()).as(ch))
//      .runDrain
//      .unsafeRun
    Stream
      .iterate(0L)(_ + 1L) // creates singleton chunks...
      //.mapChunksM(ch => console.putStrLn(s"Before: ${ch.length.toString}").as(ch))
      .chunkN(16000) // rechunk
      .mapChunksM(ch => console.putStrLn(ch.length.toString).as(ch))
      .take(Int.MaxValue.toLong + 3L)
      .runDrain
      .unsafeRun

  // 9. Flatten this stream of chunks:
  val chunks: Stream[Nothing, Int] = ZStream(Chunk(1, 2, 3), Chunk(4, 5)).flattenChunks
}
