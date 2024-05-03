
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {

    // Verifica se o caminho do arquivo foi fornecido como argumento
    if (args.length != 2) {
      println("Uso: DoubleMapReduceWordCount <caminho-do-arquivo>")
      System.exit(1)
    }

    // Carrega o arquivo de texto
    val fileName = args(0)
    val stopWordsFile = args(1)

    // Lê todas as linhas e armazena em uma lista
    val fileContents = scala.io.Source.fromFile(fileName).getLines().toList
    val stopWords = scala.io.Source.fromFile(stopWordsFile).getLines().flatMap(_.split(",")).toSet

    // Função para limpar as Strings
    def cleanWord(word: String): String = {
      word.replaceAll("[^a-zA-Z]", "")
    }

    // Função para dividir o conteúdo do arquivo em chunks de 200 linhas
    def splitIntoChunks(data: List[String], chunkSize: Int): List[List[String]] = {
      data.grouped(chunkSize).toList
    }

    // Processamento paralelo dos chunks
    val chunkSize = 200
    val chunks = splitIntoChunks(fileContents, chunkSize)

    // Usaremos Future.sequence para coletar os resultados paralelos
    val futureWordCounts = Future.sequence(chunks.map { chunk => Future {

      // Primeiro Map: Divide o conteúdo do arquivo em palavras
      val wordsMap = chunk.flatMap(_.split("\\s+")) // Utilizando paralelismo com .par
        .filter(_.nonEmpty) // Garante que nenhum espaço vazio será contado como palavra
        .map(word => (cleanWord(word.toLowerCase), 1))
        .filter { case (word, _) => !stopWords.contains(word) }

      // Segundo Map: Agrupa as palavras e conta as ocorrências de cada uma
      wordsMap.groupBy(_._1)
        .view.mapValues(_.map(_._2).sum)
    }})

    // Espera todos os Futures terminarem e extrai os resultados
    val wordCounts = Await.result(futureWordCounts, Duration.Inf)
      .flatten // Achata a lista de listas

      // Redução: Calcula o total de ocorrências de cada palavra
      .groupBy(_._1)
      .view.mapValues(_.map(_._2).sum)
      .toList
      .sortBy(-_._2)  // Organiza de forma decrescente
      .take(25)   // Seleciona as 25 palavras mais frequentes para printar

    // Imprime as contagens de palavra
    wordCounts.foreach { case (word, count) =>
      println(s"$word: $count")
    }
  }

  val lista = Array("caminho_do_arquivo.txt",
    "caminho_do_arquivo_stop_words.txt")

  main(lista)
}
