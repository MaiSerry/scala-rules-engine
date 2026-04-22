package Project

import scala.io.{Codec, Source}
import scala.util.{Try, Using}
import java.time.{Instant,LocalDate,ZoneOffset}
import java.time.temporal.ChronoUnit
object project {
/// modeling
  case class Order(
                    timestamp: Instant,
                    productName: String,
                    expiryDate: LocalDate,
                    quantity: Int,
                    unitPrice: Double,
                    channel: String,
                    paymentMethod: String
                  )

  case class ProcessedOrder(
                             order: Order,
                             discount: Double,
                             finalPrice: Double
                           )
//readfile
  def readFile(fileName: String, codec: String = Codec.default.toString): Try[Iterator[String]] = {
    Try(Source.fromFile(fileName).getLines())
  }

  // parse one CSV line
  def parseLine(line: String): Option[Order] = {
    val columns = line.split(",").toList
    // only proceed if we have exactly 7 columns
    if (columns.length != 7)
      None
    else
      Try(Order(
        timestamp = Instant.parse(columns(0)),
        productName = columns(1),
        expiryDate = LocalDate.parse(columns(2)),
        quantity = columns(3).toInt,
        unitPrice = columns(4).toDouble,
        channel = columns(5),
        paymentMethod = columns(6)
      )).toOption // if any parse fails (bad date, bad number) > none
  }

  // list of lines get list of orders with parsed
  // doesn't load all in memory

  def parseOrders(lines: Iterator[String]): Iterator[Order] =
    lines
      .drop(1)
      .flatMap(parseLine)


  // Qualifying Rules
  def iswillExpire(o: Order): Boolean = {
    val remainingdays = ChronoUnit.DAYS.between(o.timestamp.atZone(ZoneOffset.UTC).toLocalDate, o.expiryDate)
    remainingdays < 30
  }

  def isWineOrCheese(o: Order): Boolean = o.productName.toLowerCase.contains("wine") || o.productName.toLowerCase.contains("cheese")
//23march sales
  def inMonthSale(o: Order): Boolean = {
    val date = o.timestamp.atZone(java.time.ZoneOffset.UTC).toLocalDate
    date.getMonthValue == 3 && date.getDayOfMonth == 23
  }
//quantity more than 5
  def QuanitiymoreThan5(o: Order): Boolean = o.quantity > 5
//sales on App
  def isApp(o: Order): Boolean = o.channel.toLowerCase == "app"
  //sales by visa
  def isVisa(o:Order):Boolean=o.paymentMethod.toLowerCase=="visa"

  // discount engine
  def VisaDiscount(o:Order):Double=0.05

  def AppDiscount(o: Order): Double = {
    Math.ceil(o.quantity / 5.0).toInt * 0.05
  }

  def expiryDiscount(o: Order): Double = {
    val days = ChronoUnit.DAYS.between(
      o.timestamp.atZone(ZoneOffset.UTC).toLocalDate, o.expiryDate )
    // 30 - days = discount percent  (29 days → 1%, 1 day → 29%)
    (30 - days) / 100.0
  }

  def cheeseOrWineDiscount(o: Order): Double =
    o.productName.toLowerCase match {
      case name if name.contains("cheese") => 0.10
      case name if name.contains("wine")   => 0.05
      case _                               => 0.0
    }

  def march23Discount(o: Order): Double = 0.50

  def morequantityDiscount(o: Order): Double =
    o.quantity match {
      case q if q >= 15 => 0.10
      case q if q >= 10 => 0.07
      case q if q >= 6  => 0.05
      case _            => 0.0
    }
  type DiscountRule = (Order => Boolean, Order => Double)
  // made list od tuple of (qulier,discount)
  def getDiscountRules(): Vector[DiscountRule] =
    Vector(
      (iswillExpire,  expiryDiscount),
      (isWineOrCheese, cheeseOrWineDiscount),
      (inMonthSale,  march23Discount),
      (QuanitiymoreThan5, morequantityDiscount),
      (isVisa,  VisaDiscount),
      (isApp,  AppDiscount)
    )

//apply rules to get discount of order
  def applyDiscount(rules: Vector[DiscountRule])(order: Order): ProcessedOrder = {
    val topTwo =
      rules
        .filter(rule => rule._1(order)) //qualifRule
        .map   (rule => rule._2(order)) // calcRule
        .sortBy(-_)           // highest first
        .take(2)

    val discount =
      if (topTwo.isEmpty) 0.0 // no rules matched → 0%
      else topTwo.sum / topTwo.size // average of top 2

    val finalPrice = order.unitPrice * order.quantity * (1 - discount)

    ProcessedOrder(order, discount ,finalPrice)
  }
  //  process all orders

  def processOrders(orders: Iterator[Order]): Iterator[ProcessedOrder] = orders.map(applyDiscount(getDiscountRules()))

}
import java.sql.{ DriverManager, PreparedStatement}
import project.ProcessedOrder

object Database {

  val url = "jdbc:sqlite:orders.db"

  // create table if not exists
  val createSql =
    """CREATE TABLE IF NOT EXISTS processed_orders (
      |  timestamp      TEXT,
      |  product_name   TEXT,
      |  discount       REAL,
      |  final_price    REAL
      |)""".stripMargin

  val insertSql =
    """INSERT OR REPLACE INTO processed_orders VALUES
      |(?,?,?,?)""".stripMargin

  def bindOrder(stmt: PreparedStatement, p: ProcessedOrder): Unit = {
    stmt.setString(1, p.order.timestamp.toString)
    stmt.setString(2, p.order.productName)
    stmt.setDouble(3, p.discount)
    stmt.setDouble(4, p.finalPrice)
  }

  // Side effect isolated: writes all processed orders to DB
//  def writeOrders(orders: Iterator[ProcessedOrder]): Try[Int] =
//    Try {
//      Using.resource(DriverManager.getConnection(url)) {
//        conn => conn.setAutoCommit(false)
//          conn.createStatement().execute(createSql)
//        val stmt = conn.prepareStatement(insertSql)
//          val count = orders
//            .map { p =>
//              bindOrder(stmt, p)
//              stmt.executeUpdate()   // returns 1 if inserted, 0 if not
//            }
//            .sum
//          conn.commit()
//          count
//      }
//    }

  def writeOrders(orders: Iterator[ProcessedOrder]): Try[Int] =
    Try {
      Using.resource(DriverManager.getConnection(url)) { conn =>
        conn.setAutoCommit(false)
        conn.createStatement().execute(createSql)
        val stmt = conn.prepareStatement(insertSql)

        val count = orders
          .grouped(50000) // chunk of 40000
          .map { chunk =>
            chunk.foreach { p =>
              bindOrder(stmt, p)
              stmt.addBatch() // add to batch — no disk hit
            }
            val inserted = stmt.executeBatch().sum // one disk hit per 40000
            stmt.clearBatch()
            inserted
          }
          .sum

        conn.commit()
        count
      }

    }
    }
import java.io.{BufferedWriter,FileWriter}

object Logger {

  val logFile = "rules_engine.log"

  sealed trait LogLevel
  case object INFO  extends LogLevel
  case object ERROR extends LogLevel

  // builds the log line
  def formatLog(level: LogLevel, message: String): String = s"${Instant.now()} $level $message"
  val writer = new BufferedWriter(new FileWriter(logFile, true))
  // Side effect: appends one line to log file
  def log(level: LogLevel, message: String): Try[Unit] =
    Try {
        writer.write(formatLog(level, message))
        writer.newLine()
      }
  def closeLog(): Try[Unit] =
    Try {
      writer.flush()
      writer.close()
    }
}


import project.{readFile,parseOrders,processOrders}
import scala.util.{Success,Failure}

object Main extends App {

  val orderFile = "src/main/resources/TRX10M.csv"

  def measureTime[A](label: String)(block: => A): A = {
    val start  = System.currentTimeMillis()
    val result = block
    val end    = System.currentTimeMillis()
    println(f"⏱  $label%-40s ${end - start} ms")
    result
  }

  val result = measureTime("Total engine time") {
    for {
      _         <- Logger.log(Logger.INFO,  "Engine started")
      lines     <- measureTime("Read file")       { readFile(orderFile) }
      _         <- Logger.log(Logger.INFO,  "File read successfully")
      orders     = measureTime("Parse orders")    { parseOrders(lines) }
      _         <- Logger.log(Logger.INFO,  "Orders parsing pipeline ready")
      processed  = measureTime("Apply discounts") { processOrders(orders) }
      _         <- Logger.log(Logger.INFO,  "Discount rules pipeline ready")
      _         <- Logger.log(Logger.INFO,  "Starting database write...")
      count     <- measureTime("Insert into DB")  { Database.writeOrders(processed) }
      _         <- Logger.log(Logger.INFO,  s"$count orders written to database successfully")
    } yield count
  }

  result match {
    case Success(count) =>
      Logger.log(Logger.INFO, s"Engine finished successfully — $count orders processed")
      Logger.closeLog()
      println(s"Done — $count orders processed successfully")

    case Failure(ex) =>
      Logger.log(Logger.ERROR, s"Engine failed: ${ex.getMessage}")
      Logger.closeLog()
      println(s"Error: ${ex.getMessage}")
  }

}

