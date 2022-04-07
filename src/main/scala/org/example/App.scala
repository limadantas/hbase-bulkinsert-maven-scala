package org.example

/**
 * Hello world!
 *
 */
object App {

  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    println(s"Time taken: ${(end - start) / 1000 / 1000} ms")
    ret
  }

  def main(args: Array[String]) {
    val importer = new ImportCsvToHBase()
    time {
      importer.process(
        "/home/douglas/Documents/projetos/java/hbase-bulkload/massa.csv",
        "127.0.0.1",
        "2181",
        "players_data",
        "codi_coro_clie_chav",
        "main"
      )
    }
  }
}
