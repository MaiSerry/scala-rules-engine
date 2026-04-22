package Project

object TestDB extends App {

  import java.sql.DriverManager

  val url = "jdbc:sqlite:orders.db"

  val query = "SELECT * FROM processed_orders LIMIT 10"

  val conn  = DriverManager.getConnection(url)
  val stmt  = conn.createStatement()
  val rs    = stmt.executeQuery(query)

  println("timestamp                  | product_name                        | discount | final_price")
  println("─" * 95)

  while (rs.next()) {
    val timestamp   = rs.getString("timestamp")
    val productName = rs.getString("product_name")
    val discount    = rs.getDouble("discount")
    val finalPrice  = rs.getDouble("final_price")

    println(f"$timestamp%-28s | $productName%-35s | ${discount * 100}%6.1f%% | $finalPrice%10.2f")
  }

  rs.close()
  stmt.close()
  conn.close()
}