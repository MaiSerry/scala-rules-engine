package Project

import scala.io.{Codec, Source}
import scala.util.{Try, Using}
import java.time.{Instant,LocalDate,ZoneOffset}
import java.time.temporal.ChronoUnit

object project {

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

  def readFile(fileName: String, codec: String = Codec.default.toString): Try[List[String]] = {
    Using(Source.fromFile(fileName, codec)) { source =>
      source.getLines().toList
    }
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
      )).toOption // if ANY parse fails (bad date, bad number) → None
  }

  // list of lines get list of orders with parsed
  def parseOrders(lines: List[String]): List[Order] =
    lines
      .drop(1)
      .flatMap(parseLine)

/*  val orderFile = "src/main/resources/orders.csv"
  val orders: Try[List[Order]] =
    readFile(orderFile).map(parseOrders)
*/
  // Qualifying Rules
  def iswillExpire(o: Order): Boolean = {
    val remainingdays = ChronoUnit.DAYS.between(o.timestamp.atZone(ZoneOffset.UTC).toLocalDate, o.expiryDate)
    remainingdays < 30
  }

  def isWineOrCheese(o: Order): Boolean = o.productName.toLowerCase.contains("wine") || o.productName.toLowerCase.contains("cheese")

  def inMonthSale(o: Order): Boolean = {
    val date = o.timestamp.atZone(java.time.ZoneOffset.UTC).toLocalDate
    date.getMonthValue == 3 && date.getDayOfMonth == 23
  }

def QuanitiymoreThan5(o:Order):Boolean= o.quantity>5

// discount engine
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
  def getDiscountRules(): List[DiscountRule] =
    List(
      (iswillExpire,  expiryDiscount),
      (isWineOrCheese,   cheeseOrWineDiscount),
      ( inMonthSale,      march23Discount),
      (QuanitiymoreThan5,      morequantityDiscount)
    )

//apply rules to get discount of order
  def applyDiscount(rules: List[DiscountRule])(order: Order): ProcessedOrder = {
    val topTwo =
      rules
        .filter(rule => rule._1(order)) //qualifRule
        .map   (rule => rule._2(order)) // calcRule
        .sortBy(-_)           // highest first
        .take(2)

    val discount =
      if (topTwo.isEmpty) 0.0                        // no rules matched → 0%
      else topTwo.sum / topTwo.size                  // average of top 2

    val finalPrice = order.unitPrice * order.quantity * (1 - discount)

    ProcessedOrder(order, discount ,finalPrice)
  }
  //  process all orders
  def processOrders(orders: List[Order]): List[ProcessedOrder] = orders.map(applyDiscount(getDiscountRules()))

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
      |  expiry_date    TEXT,
      |  quantity       INTEGER,
      |  unit_price     REAL,
      |  channel        TEXT,
      |  payment_method TEXT,
      |  discount       REAL,
      |  final_price    REAL
      |)""".stripMargin

  val insertSql =
    """INSERT INTO processed_orders VALUES
      |(?,?,?,?,?,?,?,?,?)""".stripMargin

  // binds one ProcessedOrder to a prepared statement
  def bindOrder(stmt: PreparedStatement, p: ProcessedOrder): Unit = {
    stmt.setString (1, p.order.timestamp.toString)
    stmt.setString (2, p.order.productName)
    stmt.setString (3, p.order.expiryDate.toString)
    stmt.setInt    (4, p.order.quantity)
    stmt.setDouble (5, p.order.unitPrice)
    stmt.setString (6, p.order.channel)
    stmt.setString (7, p.order.paymentMethod)
    stmt.setDouble (8, p.discount)
    stmt.setDouble (9, p.finalPrice)
  }

  // Side effect isolated: writes all processed orders to DB
  def writeOrders(orders: List[ProcessedOrder]): Try[Unit] =
    Try {
      Using.resource(DriverManager.getConnection(url)) {
        conn => conn.createStatement().execute(createSql)
        val stmt = conn.prepareStatement(insertSql)
        orders.foreach { p => bindOrder(stmt, p)
          stmt.addBatch() }
        stmt.executeBatch()
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

  val orderFile = "src/main/resources/orders.csv"

  val result = for {
    _         <- Logger.log(Logger.INFO,  "Engine started")
    lines     <- readFile(orderFile)
    _         <- Logger.log(Logger.INFO,  s"Read ${lines.size} lines from $orderFile")
    orders     = parseOrders(lines)
    _         <- Logger.log(Logger.INFO,  s"Parsed ${orders.size} orders")
    processed  = processOrders(orders)
    _         <- Logger.log(Logger.INFO,  s"Discounts calculated for ${processed.size} orders")
    _         <- Database.writeOrders(processed)
    _         <- Logger.log(Logger.INFO,  "Orders written to database successfully")
  } yield processed

  result match {
    case Success(orders) => Logger.log(Logger.INFO, "Engine finished successfully")
      Logger.closeLog()
      orders.foreach { p => println(f"${p.order.productName}%-35s | discount: ${p.discount * 100}%5.1f%% | final: ${p.finalPrice}%8.2f") }
    case Failure(ex) => Logger.log(Logger.ERROR, s"Engine failed: ${ex.getMessage}")
     Logger.closeLog()
      println(s"Error: ${ex.getMessage}")
  }



}

