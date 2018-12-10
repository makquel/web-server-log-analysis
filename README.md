# Análise de servidor web para a Semantix

Script de análise dos log de acesso de um servidor da NASA.

Disponível em: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

### Exemplo de uso

python semantix_test.py NASA_access_log_Jul95

python semantix_test.py NASA_access_log_Aug95


## Perguntas teoricas do teste


### Qual o objetivo do comando .cache() em Spark?

No contexto do Data Frames, o comando cache(), sinonimo do comando persist(), que guarda ou salva um determinado objeto DF na mémoria e disco, pudendo também alterar o nível de armazenamento.

(Sobre a performance deste comando: https://stackoverflow.com/questions/45558868/where-does-df-cache-is-stored)

### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

A diferença entre os Frameworks Spark e Hadoop MapReduce, está, basicamente, no processamento dos dados. Enquanto o Spark processa os dados sem precisar alocar em memória estática,o MapReduce precisa de uma unidade de armazenamento para a escrita e leiura dos dados que são as operações baseicas realizas com estes, o que torna Spark 100* vezes mais rápido

(*Fonte: https://www.scnsoft.com/blog/spark-vs-hadoop-mapreduce )

### Qual é a função do SparkContext?

É um objeto responsável pela abstração dos serviços que envolvem a manipulação de BigData. Sendo assim necessario para inicializar qualquer interação com o ambiente de execução. Este possui atributos como o nome da aplicação e o endereço do cluster.
(Fonte: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-SparkContext.html)

### Explique com suas palavras o que é Resilient Distributed Datasets(RDD)
RDD é uma das APIs disponíveis para o Spark que pode ser definido como um objeto abstrato para representar dados. São tolerantes a falhas, possuem tipo predefinido (data type) e estão baseados na API de Scala.
(Fonte: https://www.datacamp.com/community/tutorials/apache-spark-python)

### GroupByKey é menos eficiente que reduceByKey em grandes dataset Por quê?
O reduceByKey é mais eficiente sobre o uso de de largura de banda numa e mais rapida do que do que o reduceByKey. Isto baseia-se no fato que o reduceByKey realiza agrupa os dados antes de serem enviados para outros executores, já o reduceByKey envia dados desagrupados para os outros executores.

