import scala.io.Source

object Main1 {
  def main(args: Array[String]): Unit = {

    // Verifica se o caminho do arquivo foi fornecido como argumento
    if (args.length != 2) {
      println("Uso: DoubleMapReduceWordCount <caminho-do-arquivo> <caminho-do-arquivo-stop-words>")
      System.exit(1)
    }

    // Carrega o arquivo de texto
    val fileName = args(0)
    val stopWordsFileName = args(1)

    // Lê todas as linhas e armazena em uma lista
    val fileContents = Source.fromFile(fileName).getLines().toList
    val stopWords = Source.fromFile(stopWordsFileName).getLines().flatMap(_.split(",")).toSet

    // Fecha os arquivos
    Source.fromFile(fileName).close()
    Source.fromFile(stopWordsFileName).close()

    // Função para limpar as Strings
    def cleanWord(word: String): String = {
      word.replaceAll("[^a-zA-Z ]", "")
    }

    // Filtra, limpa e mapeia as palavras do conteúdo do arquivo
    val wordsMap = fileContents.flatMap(_.split("\\s+"))
      .filter(_.nonEmpty) // Filtra palavras vazias
      .map(word => (cleanWord(word.toLowerCase), 1))
      .filter { case (word, _) => !stopWords.contains(word) && word.nonEmpty } // Filtra palavras vazias após a limpeza

    // Agrupa as palavras em categorias com base na primeira letra e conta a frequência de cada categoria
    val groupedCounts = wordsMap.groupBy { case (word, _) =>
        word.headOption match {
          case Some(letter) if "abcde".contains(letter) => "ae"
          case Some(letter) if "fghij".contains(letter) => "fj"
          case Some(letter) if "klmno".contains(letter) => "ko"
          case Some(letter) if "pqrst".contains(letter) => "pt"
          case Some(letter) if "uvwxyz".contains(letter) => "uz"
          case _ => "other"

        }
      }
      .view.mapValues(_.map(_._2).sum)

    //Printa a contagem de palavras em cada categoria
    val wordCounts = groupedCounts.toList
    wordCounts.foreach { case (group, count) =>

      println(s"$group: $count")
    }
  }

  val lista = Array("C:/Users/Sofia/Documents/Sofia/Faculdade/6_Semestre_2024_1/TP2/Trabalho_VideoAula/WordCount/WordCount/src/main/scala/pride-and-prejudice.txt",
    "C:/Users/Sofia/Documents/Sofia/Faculdade/6_Semestre_2024_1/TP2/Trabalho_VideoAula/WordCount/WordCount/src/main/scala/stop_words.txt")

  main(lista)
  
}
