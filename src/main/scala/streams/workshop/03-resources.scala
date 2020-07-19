package streams.workshop

import java.io.{ FileInputStream, InputStreamReader }
import java.net.URL
import java.nio.file._
import java.util.zip.GZIPInputStream

import zio.blocking.Blocking
import zio.console.Console
import zio.random.Random
import zio.stream._
import zio.{ App => _, _ }

import scala.jdk.CollectionConverters._

object Resources {
  // Resource management is an important part of stream processing. Resources can be
  // opened and closed throughout the stream's lifecycle, and most importantly,
  // need to be kept open for precisely as long as they are required for processing
  // the stream's data.

  class DatabaseClient(clientId: String) {
    def readRow: URIO[random.Random, String] = random.nextString(5).map(str => s"${clientId}-${str}")
    def writeRow(str: String): UIO[Unit]     = UIO(println(s"Writing ${str}"))
    def close: UIO[Unit]                     = UIO(println(s"Closing $clientId"))
  }
  object DatabaseClient {
    def make(clientId: String): Task[DatabaseClient] =
      UIO(println(s"Opening $clientId")).as(new DatabaseClient(clientId))
  }

  // 1. Create a stream that allocates the database client, reads 5 rows, writes
  // them back to the client and ends.
  val fiveRows: ZStream[random.Random, Throwable, String] =
    // ZStream
    //    .bracket(DatabaseClient.make("example-client-id"))(_.close)
    //    .mapM(client => client.readRow.flatMap(client.writeRow))
    ZStream
      .managed(
        Managed.make(DatabaseClient.make("exampleclient-id"))(dbClient => dbClient.close)
      )
      .flatMap(client => ZStream.repeatEffect(client.readRow).take(5).tap(client.writeRow))

  // 2. Create a stream that reads 5 rows from 3 different database clients, and writes
  // them to a fourth (separate!) client, closing each reading client after finishing reading.
  def makeClient(clientId: String) =
    ZStream.managed(Managed.make(DatabaseClient.make(clientId))(dbClient => dbClient.close))

  val fifteenRows: ZStream[Random, Throwable, Unit] = {
    val readClients = ZStream.fromIterable(Range.inclusive(1, 3)).flatMap(id => makeClient(s"$id"))
    val writeClient = makeClient("write-client")

    writeClient.flatMap { writeClient =>
      readClients.mapMPar(3)(readClient => readClient.readRow.flatMap(writeClient.writeRow))
    }
  }

  def readDatabase(clientId: String, nRows: Int): ZStream[Random, Throwable, String] =
    ZStream
      .bracket(DatabaseClient.make(clientId))(_.close)
      .flatMap(client => ZStream.repeatEffect(client.readRow).take(nRows.toLong))

  def makeManageClient(clientId: String): ZManaged[Any, Throwable, DatabaseClient] =
    DatabaseClient.make(clientId).toManaged(_.close)

  // 3. Read 25 rows from 3 different database clients, and write the rows to 5 additional
  // database clients - 5 rows each. Hint: use ZManaged.scope.
  val scopes: ZStream[Random, Throwable, String] =
    ZStream.fromEffect(Ref.make(0)).flatMap { rowsRead =>     // keep track of how many rows are read
      ZStream.managed(ZManaged.scope).flatMap { openWriter => // allows us to have fine grain control on ZManaged
        ZStream.fromEffect(openWriter(makeManageClient("initial"))).flatMap {
          case (earlyFinalizer, dbClient) =>
            ZStream
              .fromEffect(Ref.make((earlyFinalizer, dbClient)))
              .flatMap { clientRef => // keep track of the client we are writing to
                ZStream
                  .concatAll(
                    Chunk(
                      readDatabase("one", 10),
                      readDatabase("two", 10),
                      readDatabase("three", 5)
                    )
                  )
                  .tap { row =>
                    val writeRow = clientRef.get.map(_._2).flatMap(_.writeRow(row))
                    val switchOut =
                      clientRef.get.flatMap { case (finalizer, _) => finalizer(Exit.unit) } *> // close the existing client
                        openWriter(makeManageClient("next")).flatMap {
                          case (newFinalizer, newClient) => // open a new client and record it in the ref
                            clientRef.set(newFinalizer -> newClient) *> newClient.writeRow(row)
                        }
                    rowsRead.modify { currRow =>
                      if (currRow == 5) (switchOut, 0)
                      else (writeRow, currRow + 1)
                    }.flatten
                  }
              }
        }
      }
    }
}

object FileIO {
  // 1. Implement the following combinator for reading bytes from a file using
  // java.io.FileInputStream.
  def readFileBytes(path: String): ZStream[blocking.Blocking, java.io.IOException, Byte] =
    ZStream.fromInputStreamEffect(ZIO(new FileInputStream(path)).refineToOrDie[java.io.IOException])

  // 2. Implement the following combinator for reading characters from a file using
  // java.io.FileReader.
  def readFileChars(path: String): ZStream[Blocking, Throwable, Char] =
    ZStream
      .bracket(blocking.effectBlocking(new java.io.FileReader(path)))(fileReader => UIO(fileReader.close()))
      .flatMap { fileReader =>
        ZStream.repeatEffectChunkOption(
          for {
            buffer    <- UIO(Array.ofDim[Char](1024))
            charsRead <- Task(fileReader.read(buffer)).mapError(Option(_))
            result <- if (charsRead == -1) ZIO.fail(None)
                     else UIO(Chunk.fromArray(buffer.slice(0, charsRead)))
          } yield result
        )
      }

  // 3. Recursively enumerate all files in a directory.
  def listFilesRecursive(path: String): ZStream[console.Console, Throwable, Path] =
    ZStream
      .bracket(ZIO(Files.walk(Paths.get(path))))(stream => UIO(stream.close()))
      .flatMap(ZStream.fromJavaStream(_))
      .tap(path => console.putStrLn(path.toString))

  // 4. Read data from all files in a directory tree.
  def readAllFiles(path: String): ZStream[Blocking with Console, Throwable, Char] =
    listFilesRecursive(path)
      .filter(path => !Files.isDirectory(path))
      .flatMap(path => readFileChars(path.toString))

  // 5. Monitor a directory for new files using Java's WatchService.
  // Imperative example:
  def monitor(path: Path): Unit = {
    val watcher = FileSystems.getDefault().newWatchService()
    path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    var cont = true

    while (cont) {
      val key = watcher.take()

      for (watchEvent <- key.pollEvents().asScala) {
        watchEvent.kind match {
          case StandardWatchEventKinds.ENTRY_CREATE =>
            val pathEv   = watchEvent.asInstanceOf[WatchEvent[Path]]
            val filename = pathEv.context()

            println(s"${filename} created")
        }
      }

      cont = key.reset()
    }
  }

  def monitorFileCreation(path: String): ZStream[Any, Throwable, WatchEvent[Path]] =
    ZStream
      .managed(ZManaged.fromAutoCloseable(ZIO(FileSystems.getDefault.newWatchService())))
      .flatMap(watcher =>
        // register path on watcher
        ZStream.fromEffect(ZIO(Paths.get(path).register(watcher, StandardWatchEventKinds.ENTRY_CREATE))).drain ++
          ZStream.repeatEffectChunkOption(
            for {
              key <- ZIO(watcher.take()).mapError(Option(_))
              events <- ZIO(
                         key
                           .pollEvents()
                           .asScala
                           .filter(_.kind() == StandardWatchEventKinds.ENTRY_CREATE)
                           .map(watchEvent => watchEvent.asInstanceOf[WatchEvent[Path]])
                       ).mapError(Option(_))
              continue <- ZIO(key.reset()).mapError(Option(_))
              _        <- ZIO.fail(None).when(!continue)
            } yield Chunk.fromIterable(events)
          )
      )

  // 6. Write a stream that synchronizes directories.
  def synchronize(source: String, dest: String): ZStream[Blocking, Throwable, Path] =
    monitorFileCreation(source)
      .map(_.context())
      .mapM(sourceFile =>
        blocking.effectBlocking(
          Files.copy(Path.of(source, sourceFile.toString), Path.of(dest, sourceFile.toString))
        )
      )
}

object SocketIO extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  def fromReaderEffect(reader: Task[java.io.Reader]): ZStream[Blocking, Throwable, Char] =
    ZStream
      .bracket(reader)(reader => UIO(reader.close()))
      .flatMap(reader =>
        ZStream.repeatEffectChunkOption(
          for {
            buf       <- UIO(Array.ofDim[Char](2048))
            charsRead <- blocking.effectBlocking(reader.read(buf)).mapError(Option(_))
            result    <- if (charsRead == -1) ZIO.fail(None) else UIO(Chunk.fromArray(buf.slice(0, charsRead)))
          } yield result
        )
      )

  // 1. Print the first 2048 characters of the URL.
  def readUrl(url: String): ZStream[Console with Blocking, Throwable, Char] =
    fromReaderEffect(ZIO(new InputStreamReader(new URL(url).openStream())))
      .take(2048)
      .tap(c => console.putStr(c.toString))

  // 2. Create an echo server with ZStream.fromSocketServer.
  // echo "lol\nhaha\nhow are you" | nc localhost 9001
  // this will only allow reading from one socket at a time (because of flatMap, use flatMapPar instead to
  // allow multiple connections to be open and read at the same time)
  val server = ZStream
    .fromSocketServer(9001, Option("0.0.0.0"))
    // i'm using ZTransducer to be fancy (He has not explained it yet)
    .flatMap(connection => connection.read.transduce(ZTransducer.utf8Decode).mapM(console.putStrLn(_)))
    .runDrain
  //.unsafeRun

  // 3. Use `ZStream#toInputStream` and `java.io.InputStreamReader` to decode a
  // stream of bytes from a file to a string.
  val data =
    // toInputStream only works for ZStream[_, Throwable, Byte]
    ZStream
      .managed(ZStream.fromFile(Paths.get("build.sbt")).toInputStream)
      .flatMap(i => fromReaderEffect(Task(new InputStreamReader(i))))
      .tap(c => console.putStr(c.toString))
      .runDrain
      .unsafeRun

  // 4. Integrate GZIP decoding using GZIPInputStream, ZStream#toInputStream
  // and ZStream.fromInputStream.
  // gzip build.sbt
  // cat build.sbt.gz | nc localhost 9001
  val gzipDecodingServer =
    ZStream
      .fromSocketServer(9001, Option("0.0.0.0"))
      .flatMap(conn => ZStream.managed(conn.read.toInputStream))
      .map(new GZIPInputStream(_))
      .mapM(gzis => Task(new InputStreamReader(gzis)))
      .flatMap(r => fromReaderEffect(Task(r)))
      .tap(c => console.putStr(c.toString))
      .runDrain
//      .unsafeRun
}
