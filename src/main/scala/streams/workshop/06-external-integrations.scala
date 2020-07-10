package streams.workshop

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.sql.{ Connection, DriverManager, ResultSet }
import java.{ util => ju }

import zio.{ App => _, _ }
import zio.duration._
import zio.stream._

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import software.amazon.awssdk.services.s3.model.{ ListObjectsV2Request, ListObjectsV2Response, S3Object }
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import java.net.URI

import software.amazon.awssdk.regions.Region

object ExternalSources extends App {
  implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]) {
    def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live))
  }

  // 1. Refactor this function, which drains a java.sql.ResultSet,
  // to use ZStream and ZManaged.
  // Type: Unbounded, stateless iteration
  def readRows(url: String, connProps: ju.Properties, sql: String): Chunk[String] = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, connProps)

      var resultSet: ResultSet = null
      try {
        val st = conn.createStatement()
        st.setFetchSize(5)
        resultSet = st.executeQuery(sql)

        val buf = mutable.ArrayBuilder.make[String]
        while (resultSet.next())
          buf += resultSet.getString(0)

        Chunk.fromArray(buf.result())
      } finally {
        if (resultSet ne null)
          resultSet.close()
      }

    } finally {
      if (conn ne null)
        conn.close()
    }
  }

  def streamRows(url: String, connProps: ju.Properties, sql: String) =
    // silly implementation that creates a large chunk
//    ZStream
//      .managed(
//        ZManaged.make(Task(DriverManager.getConnection(url, connProps)))(conn => UIO(conn.close()))
//      )
//      .mapM { conn =>
//        for {
//          st  <- Task(conn.createStatement())
//          _   <- Task(st.setFetchSize(5))
//          rs  <- Task(st.executeQuery(sql))
//          buf = ChunkBuilder.make[String]()
//          _ <- Task(rs.next()).doWhileM { res =>
//                if (!res) ZIO.succeed(res)
//                else UIO(buf += rs.getString(0)).as(rs.next())
//              }
//        } yield buf.result()
//      }
//      .flattenChunks
    ZStream
      .managed(
        ZManaged.make(Task(DriverManager.getConnection(url, connProps)))(conn => UIO(conn.close()))
      )
      .flatMap(conn =>
        ZStream
          .bracket(Task {
            val st = conn.createStatement()
            st.getFetchSize
            st.executeQuery(sql)
          })(rs => UIO(rs.close()))
          .flatMap { rs =>
            ZStream.repeatEffectOption(
              for {
                hasNext <- Task(rs.next()).mapError(Some(_))
                row     <- if (!hasNext) ZIO.fail(None) else Task(rs.getString(1)).mapError(Some(_))
              } yield row
            )
          }
      )

  streamRows("jdbc:postgresql://localhost/public", {
    val props = new ju.Properties()
    props.put("user", "root")
    props.put("password", "root")
    props
  }, "SELECT * FROM streams").tap(console.putStrLn(_)).runDrain.unsafeRun

  // 2. Convert this function, which polls a Kafka consumer, to use ZStream and
  // ZManaged.
  // Type: Unbounded, stateless iteration
  def pollConsumer(topic: String)(f: ConsumerRecord[String, String] => Unit): Unit = {
    val props = new ju.Properties
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "streams")
    props.put("auto.offset.reset", "earliest")
    var consumer: KafkaConsumer[String, String] = null

    try {
      consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
      consumer.subscribe(List(topic).asJava)

      while (true) consumer.poll(50.millis.asJava).forEach(f(_))
    } finally {
      if (consumer ne null)
        consumer.close()
    }
  }

  def pollConsumerStream(
    topic: String,
    connectionProps: ju.Properties
  ): ZStream[Any, Throwable, ConsumerRecord[String, String]] =
    ZStream
      .bracket(
        Task(new KafkaConsumer[String, String](connectionProps, new StringDeserializer, new StringDeserializer))
      )(consumer => UIO(consumer.close()))
      .tap(consumer => Task(consumer.subscribe(List(topic).asJava)))
      .flatMap(consumer =>
        ZStream
          .repeatEffectChunk(Task(Chunk.fromIterable(consumer.poll(50.millis.asJava).iterator().asScala.to(Iterable))))
      )

  pollConsumerStream(
    "example", {
      val props = new ju.Properties
      props.put("bootstrap.servers", "localhost:9092")
      props.put("group.id", "streams")
      props.put("auto.offset.reset", "earliest")
      props
    }
  ).tap(r => console.putStrLn((r.key(), r.value()).toString()))

  // 3. Convert this function, which enumerates keys in an S3 bucket, to use ZStream and
  // ZManaged. Bonus points for using S3AsyncClient instead.
  // Type: Unbounded, stateful iteration
  def listFiles(bucket: String, prefix: String): Chunk[S3Object] = {
    val client = S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123")))
      .endpointOverride(URI.create("http://localhost:9000"))
      .build()

    def listFilesToken(acc: Chunk[S3Object], token: Option[String]): Chunk[S3Object] = {
      val reqBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
      token.foreach(reqBuilder.continuationToken)
      val req  = reqBuilder.build()
      val resp = client.listObjectsV2(req)
      val data = Chunk.fromIterable(resp.contents().asScala)

      if (resp.isTruncated()) listFilesToken(acc ++ data, Some(resp.nextContinuationToken()))
      else acc ++ data
    }

    listFilesToken(Chunk.empty, None)
  }

  def listFilesStream(bucket: String, prefix: String): ZStream[Any, Throwable, S3Object] = {
    def request(client: S3Client, token: Option[String]): Task[ListObjectsV2Response] = {
      val req         = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
      val reqWithCont = token.fold(req)(req.continuationToken)
      Task(client.listObjectsV2(reqWithCont.build()))
    }

    ZStream
      .bracket(
        Task(
          S3Client
            .builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123")))
            .endpointOverride(URI.create("http://localhost:9000"))
            .build()
        )
      )(client => UIO(client.close()))
      .flatMap(client =>
        ZStream
          .fromEffect(request(client, None))
          .flatMap(firstResponse =>
            ZStream.paginateM(firstResponse) { response =>
              val continue =
                if (response.isTruncated) request(client, Some(response.nextContinuationToken())).map(Some(_))
                else UIO.succeed(None)

              continue.map(nextResponse => (Chunk.fromIterable(response.contents().asScala), nextResponse))
            }
          )
      )
      .flattenChunks
  }

  // 4. Convert this push-based mechanism into a stream. Hint: you'll need a queue.
  // Type: callback-based iteration
  case class Message(body: String)

  trait Subscriber {
    def onError(err: Throwable): Unit
    def onMessage(msg: Message): Unit
    def onShutdown(): Unit
  }

  trait RabbitMQ {
    def register(subscriber: Subscriber): Unit
    def shutdown(): Unit
  }

  // Simulate RabbitMQ in push-mode
  object RabbitMQ {
    def make: RabbitMQ = new RabbitMQ {
      val subs: AtomicReference[List[Subscriber]] = new AtomicReference(Nil)
      val shouldStop: AtomicBoolean               = new AtomicBoolean(false)
      val thread: Thread = new Thread {
        override def run(): Unit = {
          while (!shouldStop.get()) {
            if (scala.util.Random.nextInt(11) < 7)
              subs.get().foreach(_.onMessage(Message(s"Hello ${System.currentTimeMillis}")))
            else
              subs.get().foreach(_.onError(new RuntimeException("Boom!")))

            Thread.sleep(1000)
          }

          subs.get().foreach(_.onShutdown())
        }
      }

      thread.run()

      def register(sub: Subscriber): Unit = {
        subs.updateAndGet(sub :: _)
        ()
      }
      def shutdown(): Unit = shouldStop.set(true)
    }
  }

  def messageStream(rabbit: RabbitMQ): ZStream[Any, Throwable, Message] =
    ZStream.effectAsync[Any, Throwable, Message] { cb =>
      rabbit.register(new Subscriber {
        override def onError(err: Throwable): Unit =
          cb(ZIO.fail(Some(err)))

        override def onMessage(msg: Message): Unit =
          cb(ZIO.succeed(Chunk.single(msg)))

        override def onShutdown(): Unit =
          cb(ZIO.fail(None))
      })
    }

  // 5. Convert this Reactive Streams-based publisher to a ZStream.
  val publisher =
    ZStream
      .bracket(Task(RabbitMQ.make))(mq => UIO(mq.shutdown()))
      .flatMap(messageStream)

  //effectAsyncM
  //effectAsyncMaybe
  //effectAsyncInterrupt -> cancel subscription when Stream is interrupted
}
