# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

---

## Informe: Arquitectura de Coordinación del Sistema Distribuido

### Visión general de la arquitectura final

El sistema extiende el esqueleto provisto para soportar múltiples clientes concurrentes y escalar horizontalmente las instancias de Sum y Aggregation. El flujo de datos completo es:

```
Client --TCP--> Gateway --queue--> Sum[x] --queue--> Aggregation[y] --queue--> Join --queue--> Gateway --TCP--> Client
                                  |                                  ^
                                  +--- fanout exchange (broadcast) --+
                                  |                                  |
                                  +--- sum_eof_queue (notificación) -+
```

Cada mensaje interno lleva un `client_id` que permite separar los flujos de datos de los distintos clientes a lo largo de todo el pipeline, posibilitando el procesamiento concurrente de múltiples consultas.

### Cambios principales respecto al esqueleto original

#### 1. Identificación de clientes (`client_id`)

**Problema original:** El esqueleto no distingue entre clientes. Todos los datos (fruta, cantidad) se acumulan en un mismo estado global sin separación por cliente, lo que impide procesar consultas concurrentes.

**Solución:** Se introduce un contador atómico compartido (`multiprocessing.Value`) en el Gateway que asigna un identificador único y monótonamente creciente a cada cliente al conectarse. Este `client_id` se serializa en todo mensaje interno:

- Mensajes de datos: `[client_id, fruta, cantidad]`
- Mensajes de EOF: `[client_id]`
- Mensajes de resultado: `[client_id, top_parcial]`

El `MessageHandler` del Gateway fue modificado para incluir `client_id` en la serialización y deserialización, y para filtrar resultados por cliente en la etapa de respuesta.

**Justificación:** Sin `client_id` es imposible que los controles downstream (Sum, Aggregation, Join) separen los datos de distintos clientes. Cada control mantiene sus estructuras de datos indexadas por `client_id`, lo que permite que múltiples consultas fluyan simultáneamente por el pipeline sin interferencia.

#### 2. Distribución de mensajes a Sum mediante round-robin

**Problema original:** El Gateway envía todos los mensajes a una única cola `input_queue`. Cuando hay múltiples instancias de Sum, RabbitMQ distribuye los mensajes en _round-robin_ entre los consumidores. Esto implica que los datos de un mismo cliente se reparten entre distintas instancias de Sum, pero **solo una de ellas recibe el EOF** del cliente.

**Solución:** Se mantiene la distribución round-robin de RabbitMQ como mecanismo de balanceo de carga. Para resolver la asimetría del EOF, se implementa un mecanismo de _broadcast_ entre instancias de Sum (ver punto 4).

**Justificación:** Usar round-robin permite que el Gateway publique mensajes sin necesidad de conocer `SUM_AMOUNT` (la cantidad de instancias de Sum), lo cual es esencial dado que el Gateway no recibe esa variable de entorno en los escenarios provistos y no puede modificarse el docker-compose. La distribución automática también proporciona balanceo de carga natural.

#### 3. Sincronización entre instancias de Sum mediante fanout exchange

**Problema original:** Cuando un EOF de un cliente llega a una única instancia de Sum (por round-robin), las demás instancias de Sum que poseen datos parciales de ese cliente no tienen forma de saber que deben enviar sus resultados.

**Solución:** Se implementó un mecanismo de _broadcast_ entre Sumas utilizando un exchange de tipo `fanout` en RabbitMQ:

1. Cada instancia de Sum, al inicio, crea una cola exclusiva `sum_bcast_{ID}` y la vincula al exchange `sum_bcast`.
2. Cuando una Suma recibe un EOF por la cola de entrada, ejecuta `_flush_client()` para enviar sus propios datos agregados y luego publica un mensaje de broadcast al exchange fanout.
3. Todas las demás Sumas reciben este broadcast, descartan los mensajes propios (comparando `source_id == ID`) y ejecutan `_flush_client()` para enviar sus datos parciales del cliente.

La nueva clase `FanoutExchange` en el middleware permite publicar mensajes a un exchange fanout de RabbitMQ, donde cada consumidor tiene su propia cola vinculada, garantizando que el mensaje llegue a **todas** las instancias.

**Justificación:** Un exchange `fanout` es la primitiva natural de RabbitMQ para broadcast. A diferencia de enviar el aviso por la misma cola de entrada (lo cual causaría distribución round-robin, no broadcast), el fanout garantiza que cada mensaje llega a todas las colas vinculadas. Se descartan los mensajes propios para evitar que una Suma procese su cliente dos veces.

#### 4. Enrutamiento de Sum a Aggregation por hash de client_id

**Problema original:** El esqueleto realiza _broadcast_ desde cada Sum a todas las instancias de Aggregation. Si hay 3 instancias de Aggregation, cada fruta se contaría 3 veces, produciendo resultados incorrectos.

**Solución:** Cada Sum envía los datos de un cliente a una única instancia de Aggregation determinada por `client_id % AGGREGATION_AMOUNT`. Se utilizan colas directas `aggregation_{i}` en lugar de exchanges, ya que cada cola tiene exactamente un consumidor (la Aggregation correspondiente).

**Justificación:** El hash modulo asegura que todos los datos de un mismo cliente llegan a la misma Aggregation, eliminando la duplicación. El uso de colas directas en lugar de un exchange `direct` evita un problema sutil: cuando múltiples conexiones RabbitMQ declaran colas con el mismo nombre vinculado al mismo exchange, RabbitMQ alterna entre las vinculaciones (_last-binding-wins_), causando que algunos mensajes se entreguen a conexiones que no tienen consumidores activos, perdiéndose mensajes. Las colas directas no tienen este problema porque la cola es única y tiene un solo consumidor.

#### 5. Sincronización en Aggregation: conteo de EOFs

**Problema original:** En el esqueleto, la Aggregation procesa el primer EOF que recibe y envía su top parcial inmediatamente, sin esperar los datos de las demás instancias de Sum.

**Solución:** La Aggregation ahora cuenta los EOFs recibidos por cliente y solo envía el top parcial cuando recibe `SUM_AMOUNT` EOFs (uno de cada instancia de Sum). Cada EOF de Sum señala que esa instancia terminó de enviar datos para el cliente.

**Justificación:** Dado que los datos de un cliente están repartidos entre múltiples Sumas (por round-robin), la Aggregation debe esperar a que todas las Sumas hayan enviado sus datos parciales antes de consolidar. Sin este mecanismo, la Aggregation enviaría un top con datos incompletos, produciendo resultados erróneos. El envío de EOF desde cada Suma (incluso las que no tienen datos para ese cliente) garantiza que la Aggregation pueda contabilizar correctamente la finalización de todas las fuentes.

#### 6. Consumo multi-cola en Sum y Join

**Problema original:** Tanto Sum como Join necesitan consumir de múltiples colas simultáneamente, pero la clase `MessageMiddlewareQueueRabbitMQ` solo soporta una cola por conexión.

**Solución:** Se implementó la clase `MultiQueueConsumer` en el middleware, que utiliza una única conexión `pika.BlockingConnection` para registrar consumidores en múltiples colas. Esto permite:

- **Sum:** consume de `input_queue` (datos del Gateway) y `sum_bcast_{ID}` (broadcasts de otras Sumas).
- **Join:** consume de `join_queue` (tops parciales de Aggregation) y `sum_eof_queue` (notificaciones de EOF de Sum).

**Justificación:** `pika.BlockingConnection` no permite mezclar consumidores de múltiples conexiones en un mismo hilo. La alternativa de usar múltiples hilos con conexiones separadas fue descartada porque `BlockingConnection` no es _thread-safe_ y causa cierres inesperados de conexión. `MultiQueueConsumer` resuelve esto con una única conexión y un solo `start_consuming()`, que despacha los callbacks según la cola de origen.

#### 7. Condición de finalización en Join

**Problema original:** El Join del esqueleto simplemente reenvía el primer top parcial que recibe, sin esperar los de otras Aggregations ni señales de fin.

**Solución:** El Join ahora verifica dos condiciones antes de enviar el top final de un cliente:

1. Haber recibido al menos un top parcial (de la Aggregation correspondiente al cliente).
2. Haber recibido al menos una notificación de EOF desde `sum_eof_queue` (señal de que todas las Sumas terminaron de procesar ese cliente).

Las notificaciones de EOF se deduplican con un `set`, ya que cada Suma envía una notificación por cliente, pero el Join solo necesita saber que al menos una Señal llegó (lo cual indica que el broadcast se propagó y todas las Sumas enviaron sus datos).

**Justificación:** En la arquitectura con hash de client_id, cada cliente es atendido por exactamente una Aggregation, por lo que el Join solo necesita un top parcial por cliente. La señal de EOF (que se produce cuando la primera Suma recibe el EOF del Gateway y dispara el broadcast) es suficiente para saber que todas las Sumas ya enviaron sus datos y que la Aggregation correspondiente ya procesó o procesará el top parcial.

#### 8. Implementación del middleware RabbitMQ

Se refactorizaron las clases del middleware de RabbitMQ con las siguientes características:

- **Conexión con reintentos:** La función `_create_connection` realiza hasta 50 reintentos con espera de 1 segundo, tolerando el tiempo de inicio de RabbitMQ.
- **Thread-safety en envío:** El método `send` de Queue y Exchange utiliza un `threading.Lock` para evitar condiciones de carrera si se llama desde múltiples hilos.
- **Manejo de SIGTERM:** Los métodos `stop_consuming` y `close` permiten detener la consumisión de forma segura desde un manejador de señales, usando `add_callback_threadsafe` cuando es posible.
- **Nuevas clases:** `FanoutExchange` (para broadcast entre Sumas) y `MultiQueueConsumer` (para consumo multi-cola en Sum y Join).

### Escalabilidad del sistema

#### Respecto a los clientes

El sistema escala linealmente respecto a la cantidad de clientes concurrentes. Cada cliente recibe un `client_id` único y su flujo de datos se mantiene separado a lo largo de todo el pipeline. El Gateway utiliza un pool de procesos para atender múltiples clientes en paralelo, y las estructuras de datos internas de cada control están indexadas por `client_id`, permitiendo que múltiples consultas se procesen simultáneamente sin interferencia.

#### Respecto a las instancias de Sum

Al agregar instancias de Sum, el sistema distribuye la carga de trabajo mediante round-robin en la cola de entrada. La sincronización entre Sumas se realiza mediante el exchange fanout, cuyo costo es O(1) por EOF (un mensaje de broadcast por cliente finalizado, independientemente de la cantidad de Sumas). Cada Aggregation debe recibir `SUM_AMOUNT` EOFs por cliente, lo que escala linealmente con la cantidad de Sumas pero no implica procesamiento redundante de datos.

#### Respecto a las instancias de Aggregation

Al agregar instancias de Aggregation, los clientes se distribuyen entre ellas mediante `client_id % AGGREGATION_AMOUNT`. Cada Aggregation solo recibe los datos de los clientes que le corresponden, evitando procesamiento redundante. El Join recibe exactamente un top parcial por cliente (de la Aggregation que lo procesó), por lo que su trabajo no aumenta con la cantidad de Aggregations.

### Resumen del flujo de datos por cliente

1. El **Gateway** asigna un `client_id` y publica `(client_id, fruta, cantidad)` en `input_queue`.
2. Las instancias de **Sum** consumen de `input_queue` en round-robin, acumulando datos por `client_id`.
3. Cuando una **Sum** recibe el EOF de un cliente, envía sus datos agregados a la **Aggregation** correspondiente (`client_id % AGGREGATION_AMOUNT`), envía un EOF a esa Aggregation, envía una notificación a `sum_eof_queue` del Join, y publica un broadcast al exchange `sum_bcast`.
4. Las demás **Sumas** reciben el broadcast, envían sus datos parciales y EOF a la Aggregation correspondiente, y envían sus notificaciones a `sum_eof_queue`.
5. La **Aggregation** acumula datos y EOFs por cliente. Al recibir `SUM_AMOUNT` EOFs, computa el top parcial y lo envía a `join_queue`.
6. El **Join** espera al menos un top parcial y una señal de EOF por cliente. Al cumplirse ambas condiciones, envía el top final a `results_queue`.
7. El **Gateway** consume de `results_queue`, filtra por `client_id`, y envía el resultado al cliente por TCP.
